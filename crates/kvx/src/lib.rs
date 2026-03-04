// ai
//! 🚀 kvx — the core library crate, the beating heart, the engine room
//! where dreams of zero-config search migration become mildly-configured reality.
//!
//! 📦 This crate contains the supervisor, the workers, and all the existential
//! dread that comes with building a data migration tool for fun. 🦆
//!
//! ⚠️ "The singularity will happen before this crate reaches 1.0"

// -- 🗑️ The blanket allow has been lifted. The compiler sees all now. 👁️
pub mod app_config;
pub mod backends;
pub mod buffer_pool;
pub mod cache_aligned;
pub(crate) mod composers;
pub(crate) mod progress;
mod supervisors;
pub(crate) mod throttlers;
pub mod transforms;
pub(crate) mod workers;
use crate::app_config::AppConfig;
use crate::composers::ComposerBackend;
use crate::supervisors::Supervisor;
use crate::throttlers::{ControllerBackend, ThrottleControllerBackend};
use crate::transforms::DocumentTransformer;
use anyhow::{Context, Result};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;
use tracing::info;

// 🛑 The global escape hatch — a CancellationToken that lives for the lifetime of a run().
// OnceLock ensures thread-safe, one-time initialization. Calling stop() cancels the token,
// which propagates to all workers via their clones. Like pulling the fire alarm, but for data. 🔥
// 🧠 Knowledge graph: run() creates → Supervisor receives → workers clone → stop() cancels.
static THE_ESCAPE_HATCH: OnceLock<CancellationToken> = OnceLock::new();

// 📊 Bench mode counters — atomic because N sink workers hammer these concurrently.
// 🧠 These are only meaningful when KVX_BENCH_MODE=1 (set by --bench-seconds).
// Relaxed ordering is fine — we read these once at the end, not mid-flight for correctness. 🦆
static BENCH_DOCS_DRAINED: AtomicU64 = AtomicU64::new(0);
static BENCH_BYTES_DRAINED: AtomicU64 = AtomicU64::new(0);
static BENCH_START_NANOS: AtomicU64 = AtomicU64::new(0);

// 🔬 Diagnostic counters — interval metrics, reset every 5s by the reporter task.
// 🧠 Compose = CPU time in Composer.compose(). Drain = wall time in Sink.drain().
// Source = pages pumped + bytes read. All atomic, all Relaxed, all vibes. 🦆
static DIAG_COMPOSE_NANOS: AtomicU64 = AtomicU64::new(0);
static DIAG_COMPOSE_CALLS: AtomicU64 = AtomicU64::new(0);
static DIAG_DRAIN_NANOS: AtomicU64 = AtomicU64::new(0);
static DIAG_DRAIN_CALLS: AtomicU64 = AtomicU64::new(0);
static DIAG_SOURCE_PAGES: AtomicU64 = AtomicU64::new(0);
static DIAG_SOURCE_BYTES: AtomicU64 = AtomicU64::new(0);

// 📊 Cumulative counters — never reset, accumulate across reporter snapshots.
// bench_summary() reads these + any remaining un-snapshotted BENCH_* values.
// 🧠 Without these, bench_summary would read 0 because the reporter resets BENCH_* every 5s.
static DIAG_CUMULATIVE_DOCS: AtomicU64 = AtomicU64::new(0);
static DIAG_CUMULATIVE_BYTES: AtomicU64 = AtomicU64::new(0);

/// 🚀 The grand entry point. The big kahuna. The main event.
pub async fn run(app_config: AppConfig) -> Result<()> {
    let start_time = SystemTime::now();
    info!("🚀 KRAVEX IS BLASTING OFF — hold onto your indices, we are MIGRATING, baby!");

    // 🛑 Create the cancellation token for this run. Workers get clones.
    // OnceLock::set returns Err if already set (e.g., second run in same process) — we just use a fresh one.
    let the_cancellation_token = CancellationToken::new();
    let _ = THE_ESCAPE_HATCH.set(the_cancellation_token.clone());

    // -- 🏗️ Build the backends from config — five flavors of source, four flavors of sink.
    // -- Like a search engine buffet, except you can't come back for seconds. Or can you? 🔄
    let source_backend = app_config
        .source
        .build_backend(&app_config.throttle.source)
        .await
        .context("Failed to create source backend")?;

    let sink_parallelism = app_config.runtime.sink_parallelism;
    let mut sink_backends = Vec::with_capacity(sink_parallelism);
    for _ in 0..sink_parallelism {
        sink_backends.push(
            app_config
                .sink
                .build_backend()
                .await
                .context("Failed to create sink backend")?,
        );
    }

    // 🔄 Resolve the transform from source/sink config pair.
    // 🧠 Knowledge graph: DocumentTransformer::from_configs() matches (source, sink) → transform.
    // File→ES = RallyS3ToEs, File→File = Passthrough, InMemory→InMemory = Passthrough, etc.
    let transformer = DocumentTransformer::from_configs(&app_config.source, &app_config.sink);

    // 🎼 Resolve the composer from sink config.
    // 🧠 ES/File → NdjsonComposer, InMemory → JsonArrayComposer.
    // The Composer transforms raw pages AND assembles them into wire format. Two birds, one Cow. 🐄
    let composer = ComposerBackend::from_sink_config(&app_config.sink);

    // 🧠 Build throttle controllers for each sink worker via from_config() + clone().
    // One factory call, N clones — no shared mutable state, no Mutex, no drama.
    // 🧠 Knowledge graph: SinkThrottleConfig → ThrottleControllerBackend::from_config()
    //   Static → fixed bytes (the OG). Pid → PidControllerBytesToMs (the secret sauce, LICENSE-EE) 🔒
    let max_request_size_bytes = app_config.throttle.sink.max_request_size_bytes;
    let the_prototype_controller =
        ThrottleControllerBackend::from_config(&app_config.throttle.sink);
    let throttle_controllers: Vec<_> = (0..sink_parallelism)
        .map(|_| the_prototype_controller.clone())
        .collect();

    // 🎛️ Resolve the controller from throttle config.
    // Static = fixed batch size (default, preserves existing behavior).
    // PidBytesToDocCount = adaptive feedback-driven batch sizing (the fancy one).
    // 🧠 Knowledge graph: controller lives in SourceWorker, feeds output to source.pump(hint).
    let the_default_page_size = app_config.throttle.source.max_batch_size_docs;
    let the_controller = ControllerBackend::from_config(
        &app_config.throttle.source.controller,
        the_default_page_size,
    );

    let supervisor = Supervisor::new(app_config.clone());
    supervisor
        .start_workers(
            source_backend,
            sink_backends,
            transformer,
            composer,
            max_request_size_bytes,
            the_controller,
            throttle_controllers,
            the_cancellation_token,
        )
        .await?;

    info!(
        "🎉 MIGRATION COMPLETE! Took: {:#?} — not bad for a Rust crate that was \"almost done\" six sprints ago 🦆",
        start_time.elapsed()?
    );
    Ok(())
}

// 🧠 from_source_config() and from_sink_config() have been promoted to methods:
// SourceConfig::build_backend() in app_config/source_config.rs
// SinkConfig::build_backend() in app_config/sink_config.rs
// Config knows best how to instantiate its own backends. The waiter delivers, the menu decides. 🍽️🦆

/// 🛑 Stops the migration — gracefully.
///
/// Triggers the CancellationToken, which propagates to all workers:
/// - SourceWorker: closes the channel, stops pumping
/// - SinkWorkers: flush remaining buffer, close their sinks, exit
///
/// "Today IS that day. The function does something. Character development arc complete." 🎬
///
/// 🧠 Knowledge graph: THE_ESCAPE_HATCH (OnceLock<CancellationToken>) is set by run(),
/// read by stop(). Workers hold clones. cancel() is idempotent — calling it twice is fine.
/// Like double-tapping the elevator button. It doesn't go faster, but it feels right. 🦆
pub async fn stop() -> Result<()> {
    if let Some(token) = THE_ESCAPE_HATCH.get() {
        info!("🛑 Cancellation requested — workers will drain and exit gracefully. Hold tight.");
        token.cancel();
    }
    Ok(())
}

/// 📊 Report docs+bytes drained by a sink worker flush.
/// Called from flush_and_measure() on the hot path — atomic add, no lock, no drama.
/// "He who counts documents atomically, counts them accurately." — Ancient concurrency proverb 🦆
pub fn report_docs_drained(doc_count: u64, byte_count: u64) {
    BENCH_DOCS_DRAINED.fetch_add(doc_count, Ordering::Relaxed);
    BENCH_BYTES_DRAINED.fetch_add(byte_count, Ordering::Relaxed);
}

/// 📊 Arm the bench timer — called once at pipeline start when bench mode is active.
/// Stores the current timestamp as nanos-since-UNIX-epoch. We use this instead of
/// Instant because Instant can't be stored in an AtomicU64 portably. 🔬
pub fn bench_arm() {
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    BENCH_START_NANOS.store(now_nanos, Ordering::Relaxed);
}

/// 🔬 Cached bench mode check — O(1) after first call. Reads KVX_BENCH_MODE env var once,
/// caches result in OnceLock. Every subsequent call is a pointer deref + bool load.
/// "He who checks env vars in a hot loop, deserves the latency he gets." — Ancient proverb 🦆
pub fn is_bench_mode() -> bool {
    static THE_CACHED_ANSWER: OnceLock<bool> = OnceLock::new();
    *THE_CACHED_ANSWER.get_or_init(|| std::env::var("KVX_BENCH_MODE").is_ok())
}

/// 🔬 A frozen moment in diagnostic time — the counters between two reporter ticks.
/// All values represent a 5-second interval (or whatever the reporter period is).
/// 🧠 swap(0) atomically reads and resets — no lost increments, no torn reads, no drama.
pub struct DiagSnapshot {
    pub compose_nanos: u64,
    pub compose_calls: u64,
    pub drain_nanos: u64,
    pub drain_calls: u64,
    pub source_pages: u64,
    pub source_bytes: u64,
    pub docs_drained: u64,
    pub bytes_drained: u64,
}

/// 🔬 Report compose timing from a sink worker flush.
/// Called after composer.compose() — measures CPU time in transform+assemble. 🎼
pub fn diag_report_compose(nanos: u64) {
    DIAG_COMPOSE_NANOS.fetch_add(nanos, Ordering::Relaxed);
    DIAG_COMPOSE_CALLS.fetch_add(1, Ordering::Relaxed);
}

/// 🔬 Report drain timing from a sink worker flush.
/// Called after sink.drain() — measures wall time waiting on ES/OS bulk response. 📡
pub fn diag_report_drain(nanos: u64) {
    DIAG_DRAIN_NANOS.fetch_add(nanos, Ordering::Relaxed);
    DIAG_DRAIN_CALLS.fetch_add(1, Ordering::Relaxed);
}

/// 🔬 Report a page pumped from the source.
/// Called after source.pump() returns Some(page). Counts pages + bytes for src throughput. 🚰
pub fn diag_report_source_page(bytes: u64) {
    DIAG_SOURCE_PAGES.fetch_add(1, Ordering::Relaxed);
    DIAG_SOURCE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

/// 🔬 Atomically snapshot all interval counters and reset them to zero.
/// swap(0) is the secret — reads the current value and sets to 0 in one atomic op.
/// No lost increments between read and reset. No mutex. No tears.
/// Also accumulates docs/bytes into DIAG_CUMULATIVE_* so bench_summary() stays correct. 🧠
pub fn diag_snapshot_and_reset() -> DiagSnapshot {
    // -- 🔄 Swap the bench counters first — these feed the cumulative totals
    let docs = BENCH_DOCS_DRAINED.swap(0, Ordering::Relaxed);
    let bytes = BENCH_BYTES_DRAINED.swap(0, Ordering::Relaxed);
    // -- 📊 Accumulate into cumulative so bench_summary() can read the grand total
    DIAG_CUMULATIVE_DOCS.fetch_add(docs, Ordering::Relaxed);
    DIAG_CUMULATIVE_BYTES.fetch_add(bytes, Ordering::Relaxed);

    DiagSnapshot {
        compose_nanos: DIAG_COMPOSE_NANOS.swap(0, Ordering::Relaxed),
        compose_calls: DIAG_COMPOSE_CALLS.swap(0, Ordering::Relaxed),
        drain_nanos: DIAG_DRAIN_NANOS.swap(0, Ordering::Relaxed),
        drain_calls: DIAG_DRAIN_CALLS.swap(0, Ordering::Relaxed),
        source_pages: DIAG_SOURCE_PAGES.swap(0, Ordering::Relaxed),
        source_bytes: DIAG_SOURCE_BYTES.swap(0, Ordering::Relaxed),
        docs_drained: docs,
        bytes_drained: bytes,
    }
}

/// 📊 Print the bench summary — docs/min, bytes/sec, total docs, elapsed seconds.
/// Called once after the pipeline completes (or after --bench-seconds timeout).
/// "The numbers, Mason. What do they mean?" — Black Ops, but for throughput metrics 🎮🦆
pub fn bench_summary() {
    let start_nanos = BENCH_START_NANOS.load(Ordering::Relaxed);
    if start_nanos == 0 {
        return; // -- 🤷 bench_arm() was never called. nothing to report. ghost metrics.
    }
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let elapsed_secs = (now_nanos.saturating_sub(start_nanos)) as f64 / 1_000_000_000.0;
    // 🧠 Cumulative (from reporter resets) + remaining (not yet snapshotted by reporter).
    // Without this, bench_summary would read near-zero because the reporter swap(0)s every 5s.
    let total_docs = DIAG_CUMULATIVE_DOCS.load(Ordering::Relaxed)
        + BENCH_DOCS_DRAINED.load(Ordering::Relaxed);
    let total_bytes = DIAG_CUMULATIVE_BYTES.load(Ordering::Relaxed)
        + BENCH_BYTES_DRAINED.load(Ordering::Relaxed);

    let docs_per_sec = if elapsed_secs > 0.0 {
        total_docs as f64 / elapsed_secs
    } else {
        0.0
    };
    let docs_per_min = docs_per_sec * 60.0;
    let mib_per_sec = if elapsed_secs > 0.0 {
        (total_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs
    } else {
        0.0
    };

    // 🎯 Print in a machine-parseable AND human-readable format
    println!("\n📊 === BENCH SUMMARY ===");
    println!("⏱️  elapsed:    {:.2}s", elapsed_secs);
    println!("📄 total_docs: {}", total_docs);
    println!("📦 total_bytes: {} ({:.2} MiB)", total_bytes, total_bytes as f64 / (1024.0 * 1024.0));
    println!("🚀 docs/sec:   {:.0}", docs_per_sec);
    println!("🚀 docs/min:   {:.0}", docs_per_min);
    println!("📡 MiB/sec:    {:.2}", mib_per_sec);
    println!("📊 === END SUMMARY ===\n");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::{RuntimeConfig, SinkConfig, SourceConfig};
    use crate::backends::in_mem::{InMemorySink, InMemorySource};
    use crate::backends::{SinkBackend, SourceBackend};
    use crate::throttlers::ControllerConfig;
    use tokio_util::sync::CancellationToken;

    /// 🧪 Full pipeline integration: InMemory→Passthrough→InMemory.
    /// Four raw docs in (as one newline-delimited page), one JSON array payload out.
    ///
    /// 🧠 InMemory source returns one page: "{"doc":1}\n{"doc":2}\n{"doc":3}\n{"doc":4}".
    /// Passthrough returns the entire page as one Cow::Borrowed item.
    /// JsonArrayComposer wraps it as [page_content].
    ///
    /// 🐄 Zero-copy verification: passthrough borrows from the buffered page, no per-doc alloc.
    #[tokio::test]
    async fn the_one_where_four_docs_made_it_home_safely() -> Result<()> {
        let app_config = AppConfig {
            runtime: RuntimeConfig {
                queue_capacity: 10,
                sink_parallelism: 1,
                transform_parallelism: 1,
            },
            source: SourceConfig::InMemory(()),
            sink: SinkConfig::InMemory(()),
            throttle: Default::default(),
        };

        let source = SourceBackend::InMemory(InMemorySource::new().await?);
        let sink_inner = InMemorySink::new().await?;
        let sink = SinkBackend::InMemory(sink_inner.clone());

        // 🔄 InMemory→InMemory resolves to Passthrough transform
        let transformer = DocumentTransformer::from_configs(&app_config.source, &app_config.sink);

        // 🎼 InMemory sink → JsonArrayComposer: [item,item,...]
        let composer = ComposerBackend::from_sink_config(&app_config.sink);

        // 🧠 Build a static throttle controller for the test — InMemory doesn't need PID
        let max_request_size_bytes = app_config.throttle.sink.max_request_size_bytes;
        let throttle_controllers = vec![ThrottleControllerBackend::new_static(
            max_request_size_bytes,
        )];

        // 🎛️ Static controller for testing — no PID, just the configured batch size
        let the_controller = ControllerBackend::from_config(
            &app_config.throttle.source.controller,
            app_config.throttle.source.max_batch_size_docs,
        );

        let the_cancellation_token = CancellationToken::new();
        let supervisor = Supervisor::new(app_config);
        supervisor
            .start_workers(
                source,
                vec![sink],
                transformer,
                composer,
                max_request_size_bytes,
                the_controller,
                throttle_controllers,
                the_cancellation_token,
            )
            .await?;

        // 📦 SinkWorker received 1 page (4 docs newline-delimited), passthrough composed into JSON array.
        // 🧠 Passthrough treats entire page as one item → payload = '[{"doc":1}\n{"doc":2}\n{"doc":3}\n{"doc":4}]'
        // The page content includes newlines because passthrough doesn't split — that's by design!
        let received = sink_inner.received.lock().await;
        assert_eq!(received.len(), 1, "Should have received exactly 1 payload");

        let the_payload = &received[0];
        // 📄 Passthrough returns the whole page as one item, so JSON array wraps the entire page
        let expected = format!(
            "[{}]",
            [
                r#"{"doc":1}"#,
                r#"{"doc":2}"#,
                r#"{"doc":3}"#,
                r#"{"doc":4}"#
            ]
            .join("\n")
        );
        assert_eq!(
            the_payload, &expected,
            "InMemory sink should receive a JSON array wrapping the passthrough page"
        );

        Ok(())
    }

    /// 🧪 THE DEFINITIVE PID DUPLICATE TEST: Full pipeline with PID controllers,
    /// FileSource, RallyS3ToEs transform, NdjsonComposer, and InMemorySink.
    ///
    /// Creates a temp file with exactly N geonames-like JSON docs, runs the full
    /// supervisor pipeline with PID controllers on both source and sink side,
    /// then counts ES bulk action lines across all payloads received by InMemorySink.
    ///
    /// 🧠 TRIBAL KNOWLEDGE: This test replicates the EXACT benchmark scenario that
    /// produced 19.3M docs from 11.4M input. If PID causes duplicates in the pipeline,
    /// this test catches it — at scale sufficient to trigger any accumulating off-by-one.
    ///
    /// "I am become test, the destroyer of bugs." — J. Robert Oppenheimer,
    /// if he had pivoted to search migration software 🦆
    #[tokio::test]
    async fn the_one_where_pid_pipeline_never_creates_phantom_documents() -> Result<()> {
        use crate::backends::file::{FileSource, FileSourceConfig};
        use crate::composers::ndjson::NdjsonComposer;
        use crate::transforms::rally_s3_to_es::RallyS3ToEs;
        use std::io::Write;

        // 📝 Create a temp file with exactly N geonames-like JSON docs
        let the_sacred_doc_count: usize = 5_000;
        let mut temp_file = tempfile::NamedTempFile::new()
            .expect("💀 Temp file creation failed — the filesystem has opinions");

        for i in 0..the_sacred_doc_count {
            writeln!(
                temp_file,
                r#"{{"geonameid":{},"name":"Place {}","latitude":42.65,"longitude":1.53,"country_code":"AD","population":{}}}"#,
                i, i, i * 100
            )
            .expect("💀 Write failed");
        }
        temp_file.flush().expect("💀 Flush failed");

        // 🏗️ Build the source — FileSource with PID source controller settings
        let source_config = FileSourceConfig {
            file_name: temp_file.path().to_string_lossy().to_string(),
        };
        let source =
            SourceBackend::File(FileSource::new(source_config, 50 * 1024 * 1024, 1000).await?);

        // 🗑️ Build the sink — InMemorySink to capture payloads for counting
        let sink_inner = InMemorySink::new().await?;
        let sink = SinkBackend::InMemory(sink_inner.clone());

        // 🔄 Use RallyS3ToEs transform + NdjsonComposer — matches the benchmark exactly
        let transformer = DocumentTransformer::RallyS3ToEs(RallyS3ToEs);
        let composer = ComposerBackend::Ndjson(NdjsonComposer);

        // 🎛️ PID source controller — adaptive batch sizing, the suspected culprit
        let the_pid_config = ControllerConfig::PidBytesToDocCount {
            desired_response_size_bytes: 1_000_000.0, // 🎯 Target 1MB pages
            initial_doc_count: 100,
            min_doc_count: 10,
            max_doc_count: 5000,
        };
        let the_controller = ControllerBackend::from_config(&the_pid_config, 1000);

        // 🎛️ PID sink controller — adaptive byte budget, also a suspect
        let max_request_size_bytes: usize = 10 * 1024 * 1024; // 🧮 10MB max
        let throttle_controllers = vec![ThrottleControllerBackend::new_pid(
            5000.0,                 // 🎯 set_point_ms = 5s target latency
            2 * 1024 * 1024,        // 🚀 initial = 2MB
            512 * 1024,             // 📉 min = 512KB
            max_request_size_bytes, // 📈 max = 10MB
        )];

        // 🏗️ Wire up the supervisor — 1 source, 1 sink, PID on both
        let app_config = AppConfig {
            runtime: RuntimeConfig {
                queue_capacity: 8,
                sink_parallelism: 1,
                transform_parallelism: 1,
            },
            source: SourceConfig::InMemory(()), // -- dummy, not used by supervisor
            sink: SinkConfig::InMemory(()),
            throttle: Default::default(),
        };

        let the_cancellation_token = CancellationToken::new();
        let supervisor = Supervisor::new(app_config);
        supervisor
            .start_workers(
                source,
                vec![sink],
                transformer,
                composer,
                max_request_size_bytes,
                the_controller,
                throttle_controllers,
                the_cancellation_token,
            )
            .await?;

        // 📊 Count total ES bulk action lines across ALL received payloads
        let received = sink_inner.received.lock().await;
        let the_total_action_lines: usize = received
            .iter()
            .map(|payload| {
                payload
                    .lines()
                    .filter(|l| l.starts_with(r#"{"index":"#))
                    .count()
            })
            .sum();

        // 🎯 THE ASSERTION: exactly N documents, no more, no less
        assert_eq!(
            the_total_action_lines,
            the_sacred_doc_count,
            "🐛 PID PIPELINE DUPLICATE BUG: Expected {} docs, got {} across {} payloads. \
             Ratio: {:.4}x. If > 1.0, the PID pipeline is creating phantom documents!",
            the_sacred_doc_count,
            the_total_action_lines,
            received.len(),
            the_total_action_lines as f64 / the_sacred_doc_count as f64
        );

        Ok(())
    }

    /// 🧪 PID pipeline with MULTIPLE sink workers — tests the MPMC channel delivery.
    /// If a page is delivered to multiple consumers, this test catches it.
    ///
    /// 🧠 TRIBAL KNOWLEDGE: The benchmark uses sink_parallelism=4. If the async_channel
    /// MPMC semantics somehow deliver the same page to multiple workers, the total doc
    /// count would be inflated. This test uses 4 parallel sinks to match production config.
    ///
    /// "Four sinks walk into a bar. Only one should get each page." — Ancient MPMC proverb 🦆
    #[tokio::test]
    async fn the_one_where_four_parallel_pid_sinks_never_double_dip() -> Result<()> {
        use crate::backends::file::{FileSource, FileSourceConfig};
        use crate::composers::ndjson::NdjsonComposer;
        use crate::transforms::rally_s3_to_es::RallyS3ToEs;
        use std::io::Write;

        let the_sacred_doc_count: usize = 10_000;
        let mut temp_file = tempfile::NamedTempFile::new().expect("💀 Temp file creation failed");

        for i in 0..the_sacred_doc_count {
            writeln!(temp_file, r#"{{"geonameid":{},"name":"P{}"}}"#, i, i)
                .expect("💀 Write failed");
        }
        temp_file.flush().expect("💀 Flush failed");

        let source_config = FileSourceConfig {
            file_name: temp_file.path().to_string_lossy().to_string(),
        };
        let source =
            SourceBackend::File(FileSource::new(source_config, 50 * 1024 * 1024, 1000).await?);

        // 🗑️ 4 parallel InMemorySinks — each gets its own received Vec
        let the_sink_parallelism = 4;
        let mut sink_inners: Vec<InMemorySink> = Vec::new();
        let mut sinks: Vec<SinkBackend> = Vec::new();
        for _ in 0..the_sink_parallelism {
            let inner = InMemorySink::new().await?;
            sinks.push(SinkBackend::InMemory(inner.clone()));
            sink_inners.push(inner);
        }

        let transformer = DocumentTransformer::RallyS3ToEs(RallyS3ToEs);
        let composer = ComposerBackend::Ndjson(NdjsonComposer);

        // 🎛️ PID source controller
        let the_pid_config = ControllerConfig::PidBytesToDocCount {
            desired_response_size_bytes: 500_000.0,
            initial_doc_count: 200,
            min_doc_count: 10,
            max_doc_count: 2000,
        };
        let the_controller = ControllerBackend::from_config(&the_pid_config, 500);

        // 🎛️ PID sink controllers — one per worker
        let max_request_size_bytes: usize = 5 * 1024 * 1024;
        let throttle_controllers: Vec<_> = (0..the_sink_parallelism)
            .map(|_| {
                ThrottleControllerBackend::new_pid(
                    2000.0,
                    1024 * 1024,
                    256 * 1024,
                    max_request_size_bytes,
                )
            })
            .collect();

        let app_config = AppConfig {
            runtime: RuntimeConfig {
                queue_capacity: 16,
                sink_parallelism: the_sink_parallelism,
                transform_parallelism: 1,
            },
            source: SourceConfig::InMemory(()),
            sink: SinkConfig::InMemory(()),
            throttle: Default::default(),
        };

        let the_cancellation_token = CancellationToken::new();
        let supervisor = Supervisor::new(app_config);
        supervisor
            .start_workers(
                source,
                sinks,
                transformer,
                composer,
                max_request_size_bytes,
                the_controller,
                throttle_controllers,
                the_cancellation_token,
            )
            .await?;

        // 📊 Sum action lines across ALL 4 sinks' received payloads
        let mut the_total_action_lines: usize = 0;
        for (idx, inner) in sink_inners.iter().enumerate() {
            let received = inner.received.lock().await;
            let sink_action_count: usize = received
                .iter()
                .map(|payload| {
                    payload
                        .lines()
                        .filter(|l| l.starts_with(r#"{"index":"#))
                        .count()
                })
                .sum();
            tracing::debug!(
                "🗑️ Sink {} received {} payloads with {} action lines",
                idx,
                received.len(),
                sink_action_count
            );
            the_total_action_lines += sink_action_count;
        }

        assert_eq!(
            the_total_action_lines,
            the_sacred_doc_count,
            "🐛 PARALLEL PID DUPLICATE BUG: Expected {} docs across 4 sinks, got {}. \
             Ratio: {:.4}x. MPMC channel delivering pages to multiple consumers?!",
            the_sacred_doc_count,
            the_total_action_lines,
            the_total_action_lines as f64 / the_sacred_doc_count as f64
        );

        Ok(())
    }
}
