// ai
//! 🎬 *[INT. SERVER ROOM — THE THREADS AWAKEN]*
//! *[a raw feed slides down ch1. a joiner thread stirs.]*
//! *["Finally," it breathes. "My purpose."]*
//! *[it casts. it joins. it sends. it is alive.]* 🧵🚀🦆
//!
//! 📦 The Joiner — CPU-bound worker running on dedicated OS threads (std::thread).
//! Sits between pumper (ch1) and drainer (ch2) in the pipeline:
//!
//! ```text
//! Pumper (async I/O) → ch1 → Joiner(s) (sync CPU, std::thread) → ch2 → Drainer(s) (async I/O)
//! ```
//!
//! 🧠 Knowledge graph:
//! - Receives raw feed Strings from ch1 via `recv_blocking()`
//! - Buffers feeds by byte size until approaching max_request_size_bytes
//! - Flushes via `manifold.join(&buffer, &caster)` — cast each feed + assemble wire format
//! - Sends assembled payload String to ch2 via `send_blocking()`
//! - Does NOT implement the `Worker` trait (which returns tokio::task::JoinHandle)
//!   because joiners live on std::thread, not tokio's async runtime
//!
//! 🎯 Why std::thread? JSON parsing and serialization are CPU-bound. Putting them on
//! tokio worker threads starves the async I/O that pumper and drainer need. Dedicated
//! OS threads let the CPU work grind without guilt, like a gym bro who knows it's leg day.
//!
//! ⚠️ The singularity will parse JSON in constant time. Until then, we have threads.

use crate::casts::{Caster, DocumentCaster};
use crate::manifolds::{Manifold, ManifoldBackend};
use crate::regulators::pressure_gauge::FlowKnob;
use anyhow::{Context, Result};
use async_channel::{Receiver, Sender};
use std::sync::atomic::Ordering;
use tracing::debug;

/// 🧮 Epsilon buffer — headroom so casting overhead doesn't push us over the limit.
/// 64 KiB of breathing room because payloads expand during casting
/// (ES bulk adds action lines, etc.) and we'd rather flush one feed early
/// than trigger a 💀 413 Request Entity Too Large from the sink.
///
/// 🧠 Tribal knowledge: this constant migrated here from drainer.rs when the pipeline
/// was split into joiner (CPU) and drainer (I/O). The buffer logic now lives where the
/// CPU work happens, which is here. The drainer is now a thin I/O relay. 🚛
const BUFFER_EPSILON_BYTES: usize = 64 * 1024;

/// 🧵 The Joiner: CPU-bound worker that casts raw feeds and joins them into payloads.
///
/// Runs on a dedicated `std::thread` — not tokio — because JSON parsing doesn't deserve
/// to hog the async runtime like that one coworker who microwaves fish in the office kitchen.
///
/// 📜 Lifecycle:
/// 1. **Recv**: blocking read from ch1 (raw feed String from pumper)
/// 2. **Buffer**: accumulate feeds until byte size threshold approached
/// 3. **Flush**: `manifold.join(&buffer, &caster)` → assembled payload String
/// 4. **Send**: blocking write to ch2 (payload String to drainer)
/// 5. **Repeat** until ch1 closes, then flush remaining buffer, drop tx (signals ch2) 🦆
#[derive(Debug)]
pub struct Joiner {
    /// 📥 ch1 receiver — raw feeds from the pumper, delivered fresh like morning newspapers
    /// except the news is JSON and the paperboy is async_channel
    rx: Receiver<String>,
    /// 📤 ch2 sender — assembled payloads dispatched to drainers like care packages
    /// to the I/O frontlines
    tx: Sender<String>,
    /// 🔄 Per-feed format conversion — NdJsonToBulk, Passthrough, etc.
    /// Cloned per-joiner but zero-sized, so cloning costs less than this comment 🐄
    caster: DocumentCaster,
    /// 🎼 Payload assembly — casts each feed + joins into wire format (NDJSON, JSON array)
    /// Also zero-sized. Also free to clone. Sensing a theme here.
    manifold: ManifoldBackend,
    /// 🔧 The throttle knob — Arc<AtomicUsize> read on every flush check.
    /// When a PressureGauge is running, this value adjusts dynamically via PID.
    /// When no regulator is active, it stays at the initial max_request_size_bytes forever.
    /// Like a volume knob that someone else might be turning while you're listening. 🎚️
    the_throttle_knob: FlowKnob,
}

impl Joiner {
    /// 🏗️ Construct a Joiner with all the ingredients for CPU-bound feed processing.
    ///
    /// "Give a joiner a feed, it processes for a millisecond.
    ///  Give a joiner a channel, it processes until the pumper dies." — Ancient proverb 🧵
    pub fn new(
        rx: Receiver<String>,
        tx: Sender<String>,
        caster: DocumentCaster,
        manifold: ManifoldBackend,
        the_throttle_knob: FlowKnob,
    ) -> Self {
        Self {
            rx,
            tx,
            caster,
            manifold,
            the_throttle_knob,
        }
    }

    /// 🚀 Spawn this joiner on a dedicated OS thread.
    ///
    /// Returns `std::thread::JoinHandle` (NOT tokio::task::JoinHandle) because
    /// this worker lives outside the async runtime. It calls `recv_blocking()` and
    /// `send_blocking()` — no `.await` in sight. Pure sync. Old school. Like a fax
    /// machine but for bytes. 📠
    ///
    /// 🧠 The thread runs until ch1 closes (pumper done), then flushes remaining
    /// buffered feeds and drops tx (which helps close ch2 when all joiners finish).
    pub fn start(self) -> std::thread::JoinHandle<Result<()>> {
        std::thread::spawn(move || {
            debug!("🧵 Joiner thread started — recv_blocking → buffer → join → send_blocking");

            // 📦 Feed buffer — accumulates raw feeds until flush threshold
            let mut the_feed_buffer: Vec<String> = Vec::new();
            let mut the_running_byte_tab: usize = 0;

            loop {
                match self.rx.recv_blocking() {
                    Ok(feed) => {
                        debug!("📄 Joiner received {} byte feed from ch1", feed.len());

                        // 📏 Accumulate feed into buffer, track bytes like a metered taxi 🚕
                        the_running_byte_tab += feed.len();
                        the_feed_buffer.push(feed);

                        // 🧮 Flush when buffer + epsilon approaches the throttle knob value.
                        // 🔧 Relaxed ordering — eventual consistency is fine for a throttle.
                        // We'll see the updated value within a few iterations at most. No rush. 🎚️
                        let the_current_max = self.the_throttle_knob.load(Ordering::Relaxed);
                        if the_running_byte_tab + BUFFER_EPSILON_BYTES
                            >= the_current_max
                        {
                            debug!(
                                "🚿 Joiner flushing {} feeds ({} bytes) — buffer approaching max",
                                the_feed_buffer.len(),
                                the_running_byte_tab
                            );
                            flush_and_forward(
                                &mut the_feed_buffer,
                                &mut the_running_byte_tab,
                                &self.manifold,
                                &self.caster,
                                &self.tx,
                            )?;
                        }
                    }
                    Err(_) => {
                        // 🏁 ch1 closed — pumper is done. Flush remaining buffer and exit.
                        if !the_feed_buffer.is_empty() {
                            debug!(
                                "🚿 Joiner final flush: {} feeds ({} bytes) — ch1 closed, last payload",
                                the_feed_buffer.len(),
                                the_running_byte_tab
                            );
                            flush_and_forward(
                                &mut the_feed_buffer,
                                &mut the_running_byte_tab,
                                &self.manifold,
                                &self.caster,
                                &self.tx,
                            )?;
                        }
                        debug!("🏁 Joiner: ch1 closed. Dropping tx. Thread signing off. 💤");
                        // tx drops here naturally — when all joiners drop their tx,
                        // ch2 closes and drainers get the signal. Elegant, like a
                        // synchronized swim team but for channel closures. 🏊
                        return Ok(());
                    }
                }
            }
        })
    }
}

/// 🚿 Flush the feed buffer: manifold.join → send_blocking to ch2 → clear buffer.
///
/// The joiner equivalent of the drainer's old flush_buffer(), except:
/// - Sync, not async (no `.await` needed — we're on a std::thread)
/// - Sends to ch2 (another channel) instead of directly to the sink
/// - The sink never sees this function. It only sees assembled payloads. Clean separation. 🧼
///
/// 🧠 Extracted as a function because joiners flush from two places:
/// 1. Buffer full enough (byte threshold hit)
/// 2. Channel closed (final flush before thread exits)
/// "He who duplicates flush logic, debugs it in two timelines." — Ancient proverb 💀
fn flush_and_forward(
    buffer: &mut Vec<String>,
    buffer_bytes: &mut usize,
    manifold: &ManifoldBackend,
    caster: &DocumentCaster,
    tx: &Sender<String>,
) -> Result<()> {
    // 🎼 Cast each feed + assemble wire-format payload via manifold
    let the_assembled_payload = manifold.join(buffer, caster).context(
        "💀 Joiner manifold.join failed — the feeds went in and existential dread came out. \
         Check the cast logic and the source data quality. \
         Or just stare at the logs. The logs stare back.",
    )?;

    // 📡 Send assembled payload to ch2 for drainers, unless it's empty/trivial
    if !the_assembled_payload.is_empty() && the_assembled_payload != "[]" {
        tx.send_blocking(the_assembled_payload).context(
            "💀 Joiner failed to send payload to ch2 — the channel rejected our offering. \
             Like sliding a note under the door and hearing it slide back. \
             ch2 may be closed or full. Either way, the vibes are off.",
        )?;
    }

    // 🧹 Reset buffer state — a fresh start, like January 1st but for bytes
    buffer.clear();
    *buffer_bytes = 0;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::casts::passthrough;
    use crate::manifolds::json_array::JsonArrayManifold;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    /// 🔧 Helper — create a FlowKnob from a usize value.
    /// Because writing `Arc::new(AtomicUsize::new(x))` every test is the kind of
    /// boilerplate that makes you question your career choices. 🏭
    fn knob(value: usize) -> FlowKnob {
        Arc::new(AtomicUsize::new(value))
    }

    /// 🧪 The one where a single feed passes through the joiner thread and arrives at ch2.
    /// Like a message in a bottle, except the ocean is a bounded channel
    /// and the bottle is a String. 🦆
    #[test]
    fn the_one_where_a_feed_survives_the_joiner_thread() {
        let (tx1, rx1) = async_channel::bounded::<String>(10);
        let (tx2, rx2) = async_channel::bounded::<String>(10);

        let joiner = Joiner::new(
            rx1,
            tx2,
            DocumentCaster::Passthrough(passthrough::Passthrough),
            ManifoldBackend::JsonArray(JsonArrayManifold),
            // 📏 Huge max so we don't trigger mid-test flushes — we control the flush via channel close
            knob(usize::MAX),
        );

        // 🚀 Launch the joiner thread into the void
        let the_joiner_thread = joiner.start();

        // 📤 Send one feed, then close ch1 to trigger final flush
        tx1.send_blocking(r#"{"doc":1}"#.to_string()).unwrap();
        tx1.close();

        // 📥 The joiner should have flushed and sent a JSON array payload to ch2
        let the_payload = rx2.recv_blocking().unwrap();
        assert_eq!(the_payload, r#"[{"doc":1}]"#, "🎯 Joiner should produce a JSON array wrapping the feed");

        // 🧵 Thread should exit cleanly after ch1 closes
        the_joiner_thread
            .join()
            .expect("💀 Joiner thread panicked — the thread had an existential crisis")
            .expect("💀 Joiner returned an error — the feeds fought back");
    }

    /// 🧪 The one where multiple feeds get buffered and flushed as one payload.
    /// Proof that the joiner actually buffers instead of just forwarding one-by-one
    /// like a lazy postman. 📬
    #[test]
    fn the_one_where_multiple_feeds_become_one_payload() {
        let (tx1, rx1) = async_channel::bounded::<String>(10);
        let (tx2, rx2) = async_channel::bounded::<String>(10);

        let joiner = Joiner::new(
            rx1,
            tx2,
            DocumentCaster::Passthrough(passthrough::Passthrough),
            ManifoldBackend::JsonArray(JsonArrayManifold),
            knob(usize::MAX),
        );

        let the_joiner_thread = joiner.start();

        // 📤 Send three feeds, close ch1
        tx1.send_blocking(r#"{"doc":1}"#.to_string()).unwrap();
        tx1.send_blocking(r#"{"doc":2}"#.to_string()).unwrap();
        tx1.send_blocking(r#"{"doc":3}"#.to_string()).unwrap();
        tx1.close();

        // 📥 All three should arrive as one JSON array payload
        let the_payload = rx2.recv_blocking().unwrap();
        assert_eq!(
            the_payload,
            r#"[{"doc":1},{"doc":2},{"doc":3}]"#,
            "🎯 Three feeds should join into one JSON array"
        );

        the_joiner_thread.join().unwrap().unwrap();
    }

    /// 🧪 The one where the buffer flushes early because it hit the byte threshold.
    /// Like a toilet with a sensitive flush sensor. Crude but accurate. 🚽🦆
    #[test]
    fn the_one_where_buffer_flushes_before_channel_closes() {
        let (tx1, rx1) = async_channel::bounded::<String>(10);
        let (tx2, rx2) = async_channel::bounded::<String>(10);

        // 📏 Set max_request_size_bytes so small that even one feed triggers a flush
        // BUFFER_EPSILON_BYTES is 64 KiB, so anything above that + feed size triggers
        let comically_small_max = BUFFER_EPSILON_BYTES + 5;

        let joiner = Joiner::new(
            rx1,
            tx2,
            DocumentCaster::Passthrough(passthrough::Passthrough),
            ManifoldBackend::JsonArray(JsonArrayManifold),
            knob(comically_small_max),
        );

        let the_joiner_thread = joiner.start();

        // 📤 Send two feeds — each should flush independently due to tiny max
        tx1.send_blocking(r#"{"doc":"first"}"#.to_string()).unwrap();
        tx1.send_blocking(r#"{"doc":"second"}"#.to_string()).unwrap();
        tx1.close();

        // 📥 Should get two separate payloads (one per flush)
        let payload_one = rx2.recv_blocking().unwrap();
        let payload_two = rx2.recv_blocking().unwrap();

        assert_eq!(payload_one, r#"[{"doc":"first"}]"#, "🎯 First feed should flush on its own");
        assert_eq!(payload_two, r#"[{"doc":"second"}]"#, "🎯 Second feed should flush on its own");

        the_joiner_thread.join().unwrap().unwrap();
    }

    /// 🧪 The one where an empty channel produces no payloads.
    /// The joiner receives nothing. It sends nothing. It is at peace. 🧘
    #[test]
    fn the_one_where_no_feeds_means_no_payloads() {
        let (tx1, rx1) = async_channel::bounded::<String>(10);
        let (tx2, rx2) = async_channel::bounded::<String>(10);

        let joiner = Joiner::new(
            rx1,
            tx2,
            DocumentCaster::Passthrough(passthrough::Passthrough),
            ManifoldBackend::JsonArray(JsonArrayManifold),
            knob(usize::MAX),
        );

        // 📤 Close ch1 immediately — nothing to process
        tx1.close();

        let the_joiner_thread = joiner.start();
        the_joiner_thread.join().unwrap().unwrap();

        // 📥 ch2 should be empty — try_recv should fail
        assert!(
            rx2.try_recv().is_err(),
            "🎯 No feeds in, no payloads out. Conservation of data. Physics approves."
        );
    }

    /// 🧪 The one where the FlowKnob changes mid-stream and the joiner adapts.
    /// Proof that the Arc<AtomicUsize> actually does something useful, not just
    /// sitting there looking atomic. Like a thermostat that someone turns down
    /// while you're cooking — the kitchen gets colder. 🌡️🦆
    #[test]
    fn the_one_where_the_flow_knob_changes_mid_flight() {
        let (tx1, rx1) = async_channel::bounded::<String>(10);
        let (tx2, rx2) = async_channel::bounded::<String>(10);

        // 📏 Start with a huge knob — nothing flushes until channel close
        let the_shared_knob = knob(usize::MAX);
        let the_knob_clone = the_shared_knob.clone();

        let joiner = Joiner::new(
            rx1,
            tx2,
            DocumentCaster::Passthrough(passthrough::Passthrough),
            ManifoldBackend::JsonArray(JsonArrayManifold),
            the_shared_knob,
        );

        let the_joiner_thread = joiner.start();

        // 📤 Send first feed — won't flush yet (knob is huge)
        tx1.send_blocking(r#"{"doc":"before"}"#.to_string()).unwrap();

        // 🔧 Now crank the knob down so small that the NEXT feed triggers a flush
        the_knob_clone.store(BUFFER_EPSILON_BYTES + 5, Ordering::Relaxed);

        // 📤 Send second feed — should trigger flush due to lowered knob
        tx1.send_blocking(r#"{"doc":"after"}"#.to_string()).unwrap();

        // 📥 First payload should arrive (both feeds flushed together when threshold hit)
        let the_first_payload = rx2.recv_blocking().unwrap();
        assert!(
            the_first_payload.contains("before"),
            "🎯 First payload should contain the pre-knob-change feed — got {}",
            the_first_payload
        );

        // 🏁 Close and drain remaining
        tx1.close();
        the_joiner_thread.join().unwrap().unwrap();
    }
}
