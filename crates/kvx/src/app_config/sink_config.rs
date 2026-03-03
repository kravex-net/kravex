// ai
//! 🚰 SinkConfig — where the data GOES. No returns. No refunds.
//!
//! 🎬 *[INT. DATA CENTER — NIGHT. A byte walks into a sink and never comes back.]*
//! *"Where did it go?" asks the junior dev. "It was indexed," whispers the senior dev,*
//! *"indexed into the abyss."*
//!
//! 🧠 Knowledge graph: SinkConfig declares WHICH sink backend to use and its
//! connection details. `build_backend()` turns config into a live backend.
//! Request sizing lives in ThrottleAppConfig, not here. Clean separation. 🔌🦆

use anyhow::Result;
use serde::Deserialize;

use crate::backends::SinkBackend;
use crate::backends::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use crate::backends::file::{FileSink, FileSinkConfig};
use crate::backends::in_mem::InMemorySink;
use crate::backends::opensearch::{OpenSearchSink, OpenSearchSinkConfig};

/// 🗑️ SinkConfig: same vibe as SourceConfig but for the *receiving* end.
/// Data goes IN. Data does not come back out. It is not a revolving door.
/// It is a black hole of bytes, and we are at peace with that.
/// The InMemory(()) variant holds `()` which is the Rust way of saying "we have nothing to say here."
///
/// 🧠 Knowledge graph: resolved at startup into a `SinkBackend` by `build_backend()`.
/// The SinkWorker uses `max_request_size_bytes` from ThrottleAppConfig to know when to flush. 🚰
#[derive(Debug, Deserialize, Clone)]
pub enum SinkConfig {
    /// 📡 Write to an Elasticsearch index via bulk API
    Elasticsearch(ElasticsearchSinkConfig),
    /// 📂 Write to a local file (NDJSON)
    File(FileSinkConfig),
    /// 🔍 Write to an OpenSearch index via bulk API — the ES fork's favorite endpoint
    OpenSearch(OpenSearchSinkConfig),
    /// 🧪 In-memory test sink — captures payloads for assertion, no I/O
    InMemory(()),
}

// 🧠 SinkConfig no longer has max_request_size_bytes() or throttle_config() —
// those values now live in ThrottleAppConfig.sink. Backends are pure connection config. 🔌🦆

impl SinkConfig {
    /// 🏗️ Build a live SinkBackend from this config.
    ///
    /// 🧠 Knowledge graph: this was formerly `from_sink_config()` in lib.rs.
    /// Moved here because the config knows best how to instantiate its own backend.
    /// Sink backends are pure I/O — they don't need throttle config at construction time.
    /// "He who builds his own backend, owns his own destiny." — Ancient Rust proverb 🦆
    pub(crate) async fn build_backend(&self) -> Result<SinkBackend> {
        match self {
            SinkConfig::File(file_cfg) => {
                let sink = FileSink::new(file_cfg.clone()).await?;
                Ok(SinkBackend::File(sink))
            }
            SinkConfig::InMemory(_) => {
                let sink = InMemorySink::new().await?;
                Ok(SinkBackend::InMemory(sink))
            }
            SinkConfig::Elasticsearch(es_cfg) => {
                let sink = ElasticsearchSink::new(es_cfg.clone()).await?;
                Ok(SinkBackend::Elasticsearch(sink))
            }
            SinkConfig::OpenSearch(os_cfg) => {
                let sink = OpenSearchSink::new(os_cfg.clone()).await?;
                Ok(SinkBackend::OpenSearch(sink))
            }
        }
    }
}
