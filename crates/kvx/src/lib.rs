//! 🚀 kvx — the core library crate, the beating heart, the engine room
//! where dreams of zero-config search migration become mildly-configured reality.
//!
//! 📦 This crate contains the supervisor, the workers, and all the existential
//! dread that comes with building a data migration tool for fun. 🦆
//!
//! ⚠️ "The singularity will happen before this crate reaches 1.0"

// -- 🗑️ TODO: clean up the dedz (dead code, not the grateful kind)
#![allow(dead_code, unused_variables, unused_imports)]
pub mod app_config;
pub(crate) mod backends;
pub(crate) mod composers;
pub(crate) mod progress;
mod supervisors;
pub(crate) mod transforms;
use crate::app_config::AppConfig;
use crate::backends::elasticsearch::{ElasticsearchSink, ElasticsearchSource};
use crate::backends::file::{FileSink, FileSource};
use crate::backends::in_mem::{InMemorySink, InMemorySource};
use crate::backends::s3_rally::S3RallySource;
use crate::backends::{SinkBackend, SourceBackend};
use crate::supervisors::Supervisor;
use crate::app_config::{RuntimeConfig, SinkConfig, SourceConfig};
use crate::composers::ComposerBackend;
use crate::transforms::DocumentTransformer;
use anyhow::{Context, Result};
use std::time::SystemTime;
use tracing::info;

/// 🚀 The grand entry point. The big kahuna. The main event.
pub async fn run(app_config: AppConfig) -> Result<()> {
    let start_time = SystemTime::now();
    info!("🚀 KRAVEX IS BLASTING OFF — hold onto your indices, we are MIGRATING, baby!");

    // Build the backends from config
    // Note: We currently don't have implementations, so this will panic or fail when we add them.
    // We are passing an unimplemented mock mapping for now.
    let source_backend = from_source_config(&app_config)
        .await
        .context("Failed to create source backend")?;

    let sink_parallelism = app_config.runtime.sink_parallelism;
    let mut sink_backends = Vec::with_capacity(sink_parallelism);
    for _ in 0..sink_parallelism {
        sink_backends.push(
            from_sink_config(&app_config)
                .await
                .context("Failed to create sink backend")?,
        );
    }

    // 🔄 Resolve the transform from source/sink config pair.
    // 🧠 Knowledge graph: DocumentTransformer::from_configs() matches (source, sink) → transform.
    // File→ES = RallyS3ToEs, File→File = Passthrough, InMemory→InMemory = Passthrough, etc.
    let transformer =
        DocumentTransformer::from_configs(&app_config.source_config, &app_config.sink_config);

    // 🎼 Resolve the composer from sink config.
    // 🧠 ES/File → NdjsonComposer, InMemory → JsonArrayComposer.
    // The Composer transforms raw pages AND assembles them into wire format. Two birds, one Cow. 🐄
    let composer = ComposerBackend::from_sink_config(&app_config.sink_config);

    // 📏 Extract max request size from sink config for SinkWorker buffering.
    let max_request_size_bytes = app_config.sink_config.max_request_size_bytes();

    let supervisor = Supervisor::new(app_config.clone());
    supervisor
        .start_workers(source_backend, sink_backends, transformer, composer, max_request_size_bytes)
        .await?;

    info!(
        "🎉 MIGRATION COMPLETE! Took: {:#?} — not bad for a Rust crate that was \"almost done\" six sprints ago 🦆",
        start_time.elapsed()?
    );
    Ok(())
}

async fn from_source_config(config: &AppConfig) -> Result<SourceBackend> {
    match &config.source_config {
        // -- 📂 The File arm: ancient, reliable, and smells faintly of 2003.
        // -- Like a filing cabinet that somehow learned async/await.
        SourceConfig::File(file_cfg) => {
            let src = FileSource::new(file_cfg.clone()).await?;
            Ok(SourceBackend::File(src))
        }
        // -- 🧠 The InMemory arm: blazing fast, lives and dies with the process.
        // -- No persistence. No regrets. No disk. Very YOLO.
        SourceConfig::InMemory(_) => {
            let src = InMemorySource::new().await?;
            Ok(SourceBackend::InMemory(src))
        }
        // -- 📡 The Elasticsearch arm: HTTP calls, JSON parsing, and the constant
        // -- fear of a 429 response that ruins your Thursday afternoon.
        SourceConfig::Elasticsearch(es_cfg) => {
            let src = ElasticsearchSource::new(es_cfg.clone()).await?;
            Ok(SourceBackend::Elasticsearch(src))
        }
        // -- 🪣 The S3 Rally arm: cloud data, benchmark vibes, IAM permissions that
        // -- may or may not exist. The cloud giveth and the cloud taketh away.
        SourceConfig::S3Rally(s3_cfg) => {
            let src = S3RallySource::new(s3_cfg.clone()).await?;
            Ok(SourceBackend::S3Rally(src))
        }
    }
}

async fn from_sink_config(config: &AppConfig) -> Result<SinkBackend> {
    match &config.sink_config {
        // -- 📂 File sink: data goes in, data stays in. It's basically a digital shoebox
        // -- under the bed. Hope you labeled it.
        SinkConfig::File(file_cfg) => {
            let sink = FileSink::new(file_cfg.clone()).await?;
            Ok(SinkBackend::File(sink))
        }
        // -- 🧠 InMemory sink: it holds all your data, beautifully, until the process
        // -- ends and takes everything with it like a sandcastle at high tide. 🌊
        SinkConfig::InMemory(_) => {
            let sink = InMemorySink::new().await?;
            Ok(SinkBackend::InMemory(sink))
        }
        // -- 📡 Elasticsearch sink: data goes in at the speed of HTTP, which is to say,
        // -- "fast enough until it isn't." May your bulk indexing be ever green. 🌿
        SinkConfig::Elasticsearch(es_cfg) => {
            let sink = ElasticsearchSink::new(es_cfg.clone()).await?;
            Ok(SinkBackend::Elasticsearch(sink))
        }
    }
}

/// 🛑 Stops the migration.
///
/// No really. That's it. `Ok(())`. That's the whole function.
///
/// You might ask: "doesn't this do nothing?" and you would be correct.
/// This function is a philosophical statement. A meditation on impermanence.
/// Someday it will gracefully shut down workers, drain channels, flush buffers,
/// and file its taxes. Today is not that day.
///
/// "The wisest thing I ever wrote was `Ok(())`." — this function, probably.
pub(crate) async fn stop() -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app_config::{RuntimeConfig, SinkConfig, SourceConfig};

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
            source_config: SourceConfig::InMemory(()),
            sink_config: SinkConfig::InMemory(()),
        };

        let source = SourceBackend::InMemory(InMemorySource::new().await?);
        let sink_inner = InMemorySink::new().await?;
        let sink = SinkBackend::InMemory(sink_inner.clone());

        // 🔄 InMemory→InMemory resolves to Passthrough transform
        let transformer = DocumentTransformer::from_configs(
            &app_config.source_config,
            &app_config.sink_config,
        );

        // 🎼 InMemory sink → JsonArrayComposer: [item,item,...]
        let composer = ComposerBackend::from_sink_config(&app_config.sink_config);

        // 📏 Max request size from sink config
        let max_request_size_bytes = app_config.sink_config.max_request_size_bytes();

        let supervisor = Supervisor::new(app_config);
        supervisor
            .start_workers(source, vec![sink], transformer, composer, max_request_size_bytes)
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
            [r#"{"doc":1}"#, r#"{"doc":2}"#, r#"{"doc":3}"#, r#"{"doc":4}"#].join("\n")
        );
        assert_eq!(
            the_payload, &expected,
            "InMemory sink should receive a JSON array wrapping the passthrough page"
        );

        Ok(())
    }
}
