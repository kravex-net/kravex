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
use std::time::SystemTime;
use tokio_util::sync::CancellationToken;
use tracing::info;

// 🛑 The global escape hatch — a CancellationToken that lives for the lifetime of a run().
// OnceLock ensures thread-safe, one-time initialization. Calling stop() cancels the token,
// which propagates to all workers via their clones. Like pulling the fire alarm, but for data. 🔥
// 🧠 Knowledge graph: run() creates → Supervisor receives → workers clone → stop() cancels.
static THE_ESCAPE_HATCH: OnceLock<CancellationToken> = OnceLock::new();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::{RuntimeConfig, SinkConfig, SourceConfig};
    use crate::backends::in_mem::{InMemorySink, InMemorySource};
    use crate::backends::{SinkBackend, SourceBackend};
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
}
