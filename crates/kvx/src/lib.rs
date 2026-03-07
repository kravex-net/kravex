//! 🚀 kvx — the core library crate, the beating heart, the engine room
//! where dreams of zero-config search migration become mildly-configured reality.
//!
//! 📦 This crate contains the supervisor, the workers, and all the existential
//! dread that comes with building a data migration tool for fun. 🦆
//!
//! ⚠️ "The singularity will happen before this crate reaches 1.0"

// -- 🗑️ TODO: clean up the dedz (dead code, not the grateful kind)
#![allow(dead_code, unused_variables, unused_imports)]
pub mod config;
pub mod backends;
pub mod manifolds;
pub mod progress;
pub mod foreman;
pub mod casts;
pub mod regulators;
pub mod workers;

use crate::config::AppConfig;
use crate::backends::elasticsearch::{ElasticsearchSink, ElasticsearchSource};
use crate::backends::file::{FileSink, FileSource};
use crate::backends::in_mem::{InMemorySink, InMemorySource};
use crate::backends::{SinkBackend, SourceBackend};
use crate::foreman::Foreman;
use crate::config::{RuntimeConfig, SinkConfig, SourceConfig};
use crate::manifolds::ManifoldBackend;
use crate::casts::PageToEntriesCaster;
use crate::regulators::pressure_gauge::{FlowKnob, SinkAuth, spawn_pressure_gauge};
use anyhow::{Context, Result};
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
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

    // 🔄 Resolve the caster from source/sink config pair.
    // 🧠 Knowledge graph: DocumentCaster::from_configs() matches (source, sink) → caster.
    // File→ES = NdJsonToBulk, File→File = Passthrough, InMemory→InMemory = Passthrough, etc.
    let caster =
        PageToEntriesCaster::from_configs(&app_config.source_config, &app_config.sink_config);

    // 🎼 Resolve the manifold from sink config.
    // 🧠 ES/File → NdjsonManifold, InMemory → JsonArrayManifold.
    // The Manifold casts raw feeds AND joins them into wire format. Two birds, one Cow. 🐄
    let manifold = ManifoldBackend::from_sink_config(&app_config.sink_config);

    // 📏 Extract max request size from sink config — the hard ceiling for payload size.
    let max_request_size_bytes = app_config.sink_config.max_request_size_bytes();

    // 🔧 Create the FlowKnob — shared atomic valve between pressure gauge and joiners.
    // 🧠 When regulator is present, init to regulator's initial_output_bytes (PID starting point).
    // When absent, init to sink's max_request_size_bytes (static, never changes). 🎚️
    let the_initial_flow = match &app_config.regulator {
        Some(reg) => reg.initial_output_bytes,
        None => max_request_size_bytes,
    };
    let the_flow_knob: FlowKnob = Arc::new(AtomicUsize::new(the_initial_flow));

    // 🔬 Optionally spawn the pressure gauge — but only for cluster sinks (ES).
    // File/InMemory sinks have no cluster to monitor. If someone puts [regulator] in a
    // File→File config, we warn and skip rather than spam HTTP errors to localhost. 📡
    let the_gauge_handle = match (&app_config.regulator, &app_config.sink_config) {
        (Some(regulator_config), SinkConfig::Elasticsearch(_)) => {
            let (the_sink_url, the_sink_auth) = extract_sink_url_and_auth(&app_config.sink_config);
            info!(
                "🔬 Regulator configured — spawning pressure gauge (target CPU: {}%, poll: {}s)",
                regulator_config.target_cpu, regulator_config.poll_interval_secs
            );
            Some(spawn_pressure_gauge(
                regulator_config.clone(),
                the_sink_url,
                the_sink_auth,
                the_flow_knob.clone(),
                max_request_size_bytes,
            ))
        }
        (Some(_), _) => {
            info!(
                "⚠️ Regulator configured but sink is not a cluster backend — \
                 pressure gauge not spawned. FlowKnob stays at initial value. \
                 Like buying a thermostat for a tent. 🏕️"
            );
            None
        }
        (None, _) => None,
    };

    let foreman = Foreman::new(app_config.clone());
    foreman
        .start_workers(
            source_backend,
            sink_backends,
            caster,
            manifold,
            the_flow_knob,
            the_gauge_handle,
        )
        .await?;

    info!(
        "🎉 MIGRATION COMPLETE! Took: {:#?} — not bad for a Rust crate that was \"almost done\" six sprints ago 🦆",
        start_time.elapsed()?
    );
    Ok(())
}

/// 🔧 Extract the sink URL and auth from sink config for the pressure gauge.
///
/// The gauge needs to hit `_nodes/stats/os` on the same cluster the drainers write to.
/// We reuse the sink's connection details rather than asking for separate config. DRY, baby. 🏜️
///
/// 🧠 Returns ("http://localhost:9200", SinkAuth::None) for File/InMemory sinks since
/// they have no cluster to monitor. The gauge won't be spawned for those anyway. 🦆
fn extract_sink_url_and_auth(sink_config: &SinkConfig) -> (String, SinkAuth) {
    match sink_config {
        SinkConfig::Elasticsearch(es_cfg) => {
            let the_auth = match (&es_cfg.username, &es_cfg.password) {
                (Some(u), Some(p)) => SinkAuth::Basic {
                    username: u.clone(),
                    password: p.clone(),
                },
                _ => SinkAuth::None,
            };
            (es_cfg.url.clone(), the_auth)
        }
        SinkConfig::File(_) | SinkConfig::InMemory(_) => {
            // ⚠️ No cluster to monitor — return dummy values.
            // The caller shouldn't spawn a gauge for non-cluster sinks. 🤷
            ("http://localhost:9200".to_string(), SinkAuth::None)
        }
    }
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
pub async fn stop() -> Result<()> {
    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct Page(pub String);

impl Deref for Page {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Page {
    fn from(s: String) -> Self {
        Page(s)
    }
}

// 📦 A fully assembled, wire-ready payload — the final form before I/O.
#[derive(Debug, Clone, PartialEq)]
pub struct Payload(pub String);

impl Deref for Payload {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Payload {
    fn from(s: String) -> Self {
        Payload(s)
    }
}

impl PartialEq<&str> for Payload {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

#[derive(Debug, PartialEq)]
pub struct Entry(pub String);
impl Deref for Entry {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for Entry {
    fn from(s: String) -> Self {
        Entry(s)
    }
}

impl PartialEq<&str> for Entry {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

pub enum GaugeReading {
    CpuValue(usize),
    LatencyMs(usize),
    Error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{RuntimeConfig, SinkConfig, SourceConfig};

    /// 🧪 Full pipeline integration: InMemory→Passthrough→InMemory.
    /// Four raw docs in (as one newline-delimited feed), one JSON array payload out.
    ///
    /// 🧠 InMemory source returns one feed: "{"doc":1}\n{"doc":2}\n{"doc":3}\n{"doc":4}".
    /// Passthrough returns the entire feed as-is.
    /// JsonArrayManifold wraps it as [feed_content].
    ///
    /// 🐄 Zero-copy verification: passthrough borrows from the buffered feed, no per-doc alloc.
    #[tokio::test]
    async fn the_one_where_four_docs_made_it_home_safely() -> Result<()> {
        let app_config = AppConfig {
            runtime: RuntimeConfig {
                pumper_to_joiner_capacity: 10,
                joiner_to_drainer_capacity: 10,
                sink_parallelism: 1,
                joiner_parallelism: 1,
            },
            source_config: SourceConfig::InMemory(()),
            sink_config: SinkConfig::InMemory(()),
            regulator: None,
            drainer: Default::default(),
        };

        let source = SourceBackend::InMemory(InMemorySource::new().await?);
        let sink_inner = InMemorySink::new().await?;
        let sink = SinkBackend::InMemory(sink_inner.clone());

        // 🔄 InMemory→InMemory resolves to Passthrough caster
        let caster = PageToEntriesCaster::from_configs(
            &app_config.source_config,
            &app_config.sink_config,
        );

        // 🎼 InMemory sink → JsonArrayManifold: [item,item,...]
        let manifold = ManifoldBackend::from_sink_config(&app_config.sink_config);

        // 📏 Max request size from sink config
        let max_request_size_bytes = app_config.sink_config.max_request_size_bytes();

        // 🔧 No regulator for tests — static flow knob at max 🎚️
        let the_test_flow_knob: FlowKnob = Arc::new(AtomicUsize::new(max_request_size_bytes));

        let foreman = Foreman::new(app_config);
        foreman
            .start_workers(source, vec![sink], caster, manifold, the_test_flow_knob, None)
            .await?;

        // 📦 Joiner received 1 feed (4 docs newline-delimited), passthrough-cast and joined into JSON array.
        // Joiner buffers raw feeds → manifold.join(buffer, caster) → payload on ch2 → Drainer relays to sink.
        // 🧠 Passthrough treats entire feed as one item → payload = '[{"doc":1}\n{"doc":2}\n{"doc":3}\n{"doc":4}]'
        // The feed content includes newlines because passthrough doesn't split — that's by design!
        let received = sink_inner.received.lock().await;
        assert_eq!(received.len(), 1, "Should have received exactly 1 payload");

        let the_payload = &received[0];
        // 📄 Passthrough returns the whole feed as one item, so JSON array wraps the entire feed
        let expected = format!(
            "[{}]",
            [r#"{"doc":1}"#, r#"{"doc":2}"#, r#"{"doc":3}"#, r#"{"doc":4}"#].join("\n")
        );
        assert_eq!(
            the_payload, &expected,
            "InMemory sink should receive a JSON array wrapping the passthrough feed"
        );

        Ok(())
    }
}
