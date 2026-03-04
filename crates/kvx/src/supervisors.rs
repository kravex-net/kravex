// ai
//! 🎬 *[camera pans across a dimly lit server room]*
//! 🎬 *[dramatic orchestral music swells]*
//! 🎬 "In a world where workers toil endlessly..."
//! 🎬 "One supervisor dared to manage them all."
//! 🎬 *[record scratch]* 🦆
//!
//! 📦 The Supervisor module — part middle manager, part helicopter parent,
//! part that one project manager who schedules a meeting to plan the next meeting.
//!
//! ⚠️ DO NOT MAKE THIS PUB EVER
//! ⚠️ YOU HAVE BEEN WARNED
//! 💀 WORKERS ARE SUPERVISORS PRIVATE LITTLE MINIONS WHOM THE WORLD FORGOT ABOUT
//! 🔒 Like Fight Club, but for async tasks. First rule: you don't pub the workers.

use crate::app_config::AppConfig;
use crate::composers::ComposerBackend;
use crate::throttlers::{ControllerBackend, ThrottleControllerBackend};
use crate::transforms::DocumentTransformer;
use crate::workers::Worker;
use anyhow::Result;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// 📦 The Supervisor: because even async tasks need someone hovering over them
/// asking "is it done yet?" every 5 milliseconds.
///
/// 🏗️ Built with the same care and attention as IKEA furniture —
/// looks good in the docs, wobbly in production.
pub(crate) struct Supervisor {
    /// 🔧 The sacred scrolls of configuration, passed down from main()
    /// through the ancient ritual of .clone()
    app_config: AppConfig,
}

impl Supervisor {
    /// 🚀 Birth of a Supervisor. It's like a baby, but less crying.
    /// Actually no, there's plenty of crying. Mostly from the developer.
    pub(crate) fn new(app_config: AppConfig) -> Self {
        // -- 🐛 "My therapist says I should let go of control"
        // -- — said no supervisor ever
        Self { app_config }
    }
}

impl Supervisor {
    /// 🧵 Unleash the workers! Now with Composer powers and page buffering.
    ///
    /// 🧠 Knowledge graph: the pipeline flow is now:
    /// ```text
    /// Source.pump() → channel(PoolBuffer) → SinkWorker(buffer pages → composer.compose → sink.drain) → Sink(I/O)
    /// ```
    /// Each SinkWorker gets its own clone of the `DocumentTransformer` and `ComposerBackend`.
    /// Since transforms and composers are zero-sized structs, cloning is free.
    /// The Composer handles both transformation AND assembly — the Cow lives there. 🐄
    // 🐛 TODO: bundle transformer/composer/controller/throttle_controllers into a PipelineConfig struct
    // to reduce arg count. For now, clippy gets a hall pass.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn start_workers(
        &self,
        source_backend: crate::backends::SourceBackend,
        sink_backends: Vec<crate::backends::SinkBackend>,
        transformer: DocumentTransformer,
        composer: ComposerBackend,
        // 🐛 TODO: wire this into SinkWorker to enforce max payload size limits.
        // It's passed from AppConfig but the SinkWorker doesn't use it yet.
        _max_request_size_bytes: usize,
        controller: ControllerBackend,
        throttle_controllers: Vec<ThrottleControllerBackend>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        // 📬 TWO channels — the assembly line has a new station in the middle.
        // Channel1: Source → TransformWorkers (raw PoolBuffers, fresh from the well)
        // Channel2: TransformWorkers → SinkWorkers (composed PoolBuffers, wire-format ready)
        // 🧠 Backpressure propagates: if sinks are slow, Channel2 fills → TransformWorkers block →
        //    Channel1 fills → Source blocks. No data lost, no memory explosion. Just waiting. Like DMV. 🦆
        let (tx_raw, rx_raw) =
            async_channel::bounded::<crate::buffer_pool::PoolBuffer>(self.app_config.runtime.queue_capacity);
        let (tx_composed, rx_composed) =
            async_channel::bounded::<crate::buffer_pool::PoolBuffer>(self.app_config.runtime.queue_capacity);

        let the_transform_count = self.app_config.runtime.transform_parallelism;
        let the_sink_count = sink_backends.len();
        let mut worker_handles = Vec::with_capacity(the_transform_count + the_sink_count + 1);

        // 🧠 Spawn N transform workers — CPU-bound byte crunchers between source and sink.
        // Each owns a clone of the transformer + composer (zero-sized, Clone+Copy = free).
        // They share Channel1 receiver (MPMC fan-out) and Channel2 sender (MPMC fan-in).
        // "He who transforms in parallel, ships in production." — Ancient proverb 🧵
        for _ in 0..the_transform_count {
            let transform_worker = crate::workers::TransformWorker::new(
                rx_raw.clone(),
                tx_composed.clone(),
                transformer.clone(),
                composer.clone(),
                cancellation_token.clone(),
            );
            worker_handles.push(transform_worker.start());
        }
        // 🗑️ Drop the supervisor's clone of tx_composed so Channel2 closes
        // when ALL TransformWorkers finish (they each hold a clone).
        // Without this drop, Channel2 stays open forever. Like a gas station bathroom. 🚽
        drop(tx_composed);

        // 🗑️ Spawn N sink workers — pure I/O, no more transform responsibility.
        // Each gets rx_composed (Channel2 receiver), its own sink + throttle controller.
        // 🧠 SinkWorker is now simplified: buffer composed payloads → concatenate → drain.
        // The compose step moved to TransformWorker. Separation of church and state. ⛪🏛️
        for (sink_backend, throttle_controller) in
            sink_backends.into_iter().zip(throttle_controllers)
        {
            let sink_worker = crate::workers::SinkWorker::new(
                rx_composed.clone(),
                sink_backend,
                throttle_controller,
                cancellation_token.clone(),
            );
            worker_handles.push(sink_worker.start());
        }

        // 🔬 Spawn the diagnostic reporter if bench mode — prints one-line summary every 5s.
        // 🧠 Monitors Channel1 fill (source→transform) — the leading indicator of pipeline saturation.
        // "The unexamined pipeline is not worth running." — Socrates, probably 🦆
        if crate::is_bench_mode() {
            let the_diag_rx_raw = rx_raw.clone();
            let the_diag_rx_composed = rx_composed.clone();
            let the_diag_cancel = cancellation_token.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let snap = crate::diag_snapshot_and_reset();
                            let ch1_len = the_diag_rx_raw.len();
                            let ch1_cap = the_diag_rx_raw.capacity().unwrap_or(0);
                            let ch1_fill = if ch1_cap > 0 { (ch1_len as f64 / ch1_cap as f64) * 100.0 } else { 0.0 };
                            let ch2_len = the_diag_rx_composed.len();
                            let ch2_cap = the_diag_rx_composed.capacity().unwrap_or(0);
                            let ch2_fill = if ch2_cap > 0 { (ch2_len as f64 / ch2_cap as f64) * 100.0 } else { 0.0 };

                            let compose_avg_ms = if snap.compose_calls > 0 {
                                (snap.compose_nanos as f64 / snap.compose_calls as f64) / 1_000_000.0
                            } else { 0.0 };
                            let drain_avg_ms = if snap.drain_calls > 0 {
                                (snap.drain_nanos as f64 / snap.drain_calls as f64) / 1_000_000.0
                            } else { 0.0 };

                            let src_pages_per_sec = snap.source_pages as f64 / 5.0;
                            let src_mib_per_sec = (snap.source_bytes as f64 / (1024.0 * 1024.0)) / 5.0;
                            let docs_per_sec = snap.docs_drained as f64 / 5.0;
                            let docs_per_min = docs_per_sec * 60.0;

                            eprintln!(
                                "🔬 DIAG | ch1={}/{} {:.0}% | ch2={}/{} {:.0}% | compose={:.1}ms ({}) | drain={:.1}ms ({}) | src={:.0}pg/s {:.1}MiB/s | docs/s={:.0} docs/min={:.0}",
                                ch1_len, ch1_cap, ch1_fill,
                                ch2_len, ch2_cap, ch2_fill,
                                compose_avg_ms, snap.compose_calls,
                                drain_avg_ms, snap.drain_calls,
                                src_pages_per_sec, src_mib_per_sec,
                                docs_per_sec, docs_per_min,
                            );
                        }
                        _ = the_diag_cancel.cancelled() => {
                            break;
                        }
                    }
                }
            });
        }

        // 🚰 Spawn the source worker — it pumps raw pages into Channel1.
        // Now with a controller for adaptive batch sizing AND graceful cancellation! 🎛️🛑
        let source_worker = crate::workers::SourceWorker::new(
            tx_raw.clone(),
            source_backend,
            controller,
            cancellation_token,
        );
        worker_handles.push(source_worker.start());
        // 🗑️ Drop supervisor's clone of tx_raw so Channel1 closes when SourceWorker finishes
        drop(tx_raw);

        let results = futures::future::join_all(worker_handles).await;
        for result in results {
            // 🤯 result?? — the outer `?` unwraps the JoinHandle, the inner `?` unwraps the work.
            result??;
        }

        Ok(())
    }
}
