// ai
//! 🎬 *[camera pans across a dimly lit server room]*
//! 🎬 *[dramatic orchestral music swells]*
//! 🎬 "In a world where workers toil endlessly..."
//! 🎬 "One foreman dared to manage them all."
//! 🎬 *[record scratch]* 🦆
//!
//! 📦 The Foreman module — part middle manager, part helicopter parent,
//! part that one project manager who schedules a meeting to plan the next meeting.
//!
//! 🧠 Knowledge graph — the 3-stage pipeline:
//! ```text
//! Pumper (async, tokio) → ch1 → Joiner(s) (sync, std::thread) → ch2 → Drainer(s) (async, tokio) → Sink
//! ```
//! - **ch1**: async_channel::bounded — raw feeds from source, MPMC
//! - **ch2**: async_channel::bounded — assembled payloads from joiners, MPMC
//! - **Joiners**: CPU-bound work (casting, manifold join) on dedicated OS threads
//! - **Drainers**: I/O-bound work (sink.send) on tokio async runtime
//!
//! ⚠️ DO NOT MAKE THIS PUB EVER
//! ⚠️ YOU HAVE BEEN WARNED
//! 💀 WORKERS ARE THE FOREMAN'S PRIVATE LITTLE MINIONS WHOM THE WORLD FORGOT ABOUT
//! 🔒 Like Fight Club, but for async tasks. First rule: you don't pub the workers.

use crate::config::AppConfig;
use crate::casts::DocumentCaster;
use crate::manifolds::ManifoldBackend;
use crate::regulators::pressure_gauge::FlowKnob;
use crate::workers;
use crate::workers::Worker;
use anyhow::{Context, Result};
use tracing::info;

/// 📦 The Foreman: because even async tasks need someone hovering over them
/// asking "is it done yet?" every 5 milliseconds.
///
/// 🏗️ Built with the same care and attention as IKEA furniture —
/// looks good in the docs, wobbly in production.
pub struct Foreman {
    /// 🔧 The sacred scrolls of configuration, passed down from main()
    /// through the ancient ritual of .clone()
    app_config: AppConfig,
}

impl Foreman {
    /// 🚀 Birth of a Foreman. It's like a baby, but less crying.
    /// Actually no, there's plenty of crying. Mostly from the developer.
    pub fn new(app_config: AppConfig) -> Self {
        Self { app_config }
    }
}

impl Foreman {
    /// 🧵 Orchestrate the 3-stage pipeline: Pumper → Joiners → Drainers.
    ///
    /// 🧠 Knowledge graph — pipeline wiring:
    /// ```text
    /// Pumper (async) --[ch1: raw feeds]--> Joiner(s) (std::thread)
    ///                                      --[ch2: payloads]--> Drainer(s) (async) --> Sink
    /// ```
    ///
    /// 🔒 Channel closure semantics (async_channel implicit close):
    /// An async_channel closes when ALL clones of its Sender (or Receiver) are dropped.
    /// This is refcount-based — every `.clone()` extends the channel's lifetime.
    /// There is no single "owner" that closes the channel; the LAST drop does it.
    /// The foreman creates both channels but is NOT a participant — it's the orchestrator.
    /// So it must drop its copies after distributing clones to the actual workers.
    ///
    /// 🔄 Shutdown cascade (all driven by implicit Sender drops, no `.close()` calls):
    /// 1. Pumper finishes → its tx1 is dropped (only Sender for ch1) → ch1 closes
    /// 2. Joiners' recv_blocking() returns Err → flush remaining → joiner threads exit → tx2 clones dropped
    /// 3. Last joiner's tx2 dropped → all Senders for ch2 gone → ch2 closes
    /// 4. Drainers' recv().await returns Err → close sinks → exit
    ///
    /// "In the beginning there was main(). And main() said 'let there be workers.'
    ///  And the Foreman made it so. And it was... mostly okay." — Genesis 1:1 (Cargo edition) 🦆
    pub async fn start_workers(
        &self,
        source_backend: crate::backends::SourceBackend,
        sink_backends: Vec<crate::backends::SinkBackend>,
        caster: DocumentCaster,
        manifold: ManifoldBackend,
        the_flow_knob: FlowKnob,
        the_gauge_handle: Option<tokio::task::JoinHandle<()>>,
    ) -> Result<()> {
        let the_joiner_count = self.app_config.runtime.joiner_parallelism;

        // 📬 ch1: pumper → joiners — carries raw feed Strings, MPMC
        // Like a conveyor belt at a sushi restaurant, but the sushi is JSON 🍣
        let (tx1, rx1) = async_channel::bounded(self.app_config.runtime.pumper_to_joiner_capacity);

        // 📬 ch2: joiners → drainers — carries assembled payload Strings, MPMC
        // The VIP lounge of the pipeline — only processed payloads allowed past this point 🎟️
        let (tx2, rx2) = async_channel::bounded::<crate::Payload>(self.app_config.runtime.joiner_to_drainer_capacity);

        info!(
            "🏗️ Foreman assembling pipeline: 1 pumper → {} joiners → {} drainers",
            the_joiner_count,
            sink_backends.len()
        );

        // ═══════════════════════════════════════════════════════════════════
        // 🔒 CHANNEL OWNERSHIP CONTRACT
        //
        // async_channel uses refcounting: a channel stays open as long as at
        // least one Sender (or Receiver) clone exists. The channel closes
        // implicitly when the LAST clone is dropped — no explicit .close()
        // needed. This means every .clone() is a commitment: "I am keeping
        // this channel alive." The foreman creates both channels but must
        // surrender all handles to the workers, retaining NOTHING. Otherwise
        // a stale foreman handle prevents implicit closure → deadlock.
        //
        // We enforce this by:
        //   - Moving tx1 directly into the pumper (no clone, no foreman copy)
        //   - Dropping tx2, rx1, rx2 after distributing clones to workers
        //
        // The result: only workers hold channel handles. When workers exit,
        // their handles drop, channels close, downstream workers see Err,
        // and the pipeline cascades to shutdown. No .close() calls anywhere.
        // Pure RAII. The borrow checker would shed a single, proud tear. 🦀
        // ═══════════════════════════════════════════════════════════════════

        // 🧵 Spawn N joiners on dedicated OS threads (std::thread).
        // They do the CPU-heavy lifting: buffering raw feeds, casting, manifold join.
        // Each gets its own clone of rx1 and tx2.
        // Casters and manifolds are zero-sized structs — cloning is cheaper than this comment. 🐄
        let mut the_joiner_thread_handles = Vec::with_capacity(the_joiner_count);
        for _ in 0..the_joiner_count {
            let joiner = workers::Joiner::new(
                rx1.clone(),
                tx2.clone(),
                caster.clone(),
                manifold.clone(),
                the_flow_knob.clone(),
            );
            the_joiner_thread_handles.push(joiner.start());
        }

        // 🗑️ Foreman surrenders ch2 sender and ch1 receiver.
        // tx2: if foreman kept this, ch2 would never close (foreman's Sender outlives
        //   the joiners → drainers hang on recv() forever → deadlock). By dropping it,
        //   only joiner threads hold ch2 Senders. When the last joiner exits and drops
        //   its tx2 clone, ch2 closes, and drainers see Err on recv(). 📱
        // rx1: receivers don't affect send-side closure, but the foreman has no business
        //   holding a receiver it will never read. Clean ownership = clean conscience. 🧹
        drop(tx2);
        drop(rx1);

        // 🚰 Spawn N drainers on tokio — thin async relays from ch2 to sinks.
        // Each drainer gets its own sink and a clone of rx2.
        let mut the_async_worker_handles = Vec::with_capacity(sink_backends.len() + 1);
        for sink_backend in sink_backends {
            let drainer = workers::Drainer::new(rx2.clone(), sink_backend);
            the_async_worker_handles.push(drainer.start());
        }

        // 🗑️ Foreman surrenders ch2 receiver — only drainer tasks hold rx2 clones now.
        // Same reasoning: foreman is orchestrator, not participant. No stale handles. 🧹
        drop(rx2);

        // 🚰 Spawn the pumper — gets tx1 by MOVE (not clone).
        // tx1 is moved directly into the pumper, so no foreman copy exists.
        // When the pumper's async task exits (EOF from source), tx1 drops,
        // and since it's the ONLY Sender for ch1, ch1 closes implicitly.
        // No .close() call needed — RAII handles it. Like a self-closing door. 🚪
        let pumper = workers::Pumper::new(tx1, source_backend);
        the_async_worker_handles.push(pumper.start());

        // ⏳ Wait for all async workers (pumper + drainers).
        // The cascade: pumper done → ch1 closes → joiners drain+exit → ch2 closes → drainers exit.
        // So by the time join_all returns, the joiner threads should already be done. 🏁
        let the_async_results = futures::future::join_all(the_async_worker_handles).await;
        for result in the_async_results {
            // 🤯 result?? — outer `?` unwraps JoinHandle, inner `?` unwraps the work
            result??;
        }

        // 🧵 Join the std::thread handles — should be instant since joiners are already done
        // (ch1 closed → joiners flushed → exited before drainers could finish).
        // This is just the funeral procession. The threads are already at rest. 🪦
        for (i, handle) in the_joiner_thread_handles.into_iter().enumerate() {
            handle
                .join()
                .map_err(|the_panic_payload| {
                    anyhow::anyhow!(
                        "💀 Joiner thread {} panicked — it saw something in the JSON that broke it. \
                         The panic payload: {:?}. \
                         Like a horror movie, but the monster is malformed data.",
                        i,
                        the_panic_payload
                    )
                })?
                .context(format!(
                    "💀 Joiner thread {} returned an error — it tried its best, \
                     but the feeds fought back like a cornered raccoon 🦝",
                    i
                ))?;
        }

        // 🔬 Abort the pressure gauge if it was running — pipeline is done, no more regulating needed.
        // Like turning off the thermostat when you're moving out. 🌡️🦆
        if let Some(the_gauge) = the_gauge_handle {
            the_gauge.abort();
            info!("🔬 Pressure gauge aborted — pipeline complete, regulation no longer needed");
        }

        Ok(())
    }
}
