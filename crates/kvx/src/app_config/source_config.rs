// ai
//! 🔌 SourceConfig — the velvet rope at the source backend club.
//!
//! 🎬 *[INT. CONFIGURATION ROOM — The source config sits at a desk, reviewing applicants]*
//! *"Are you a File? An Elasticsearch? An S3 bucket? Show me your credentials."*
//! *[The InMemory source walks in wearing a fake mustache. Nobody is fooled.]*
//!
//! 🧠 Knowledge graph: SourceConfig is the enum that declares WHICH source backend
//! to use and its connection details. `build_backend()` turns config into a live backend.
//! Throttle config (batch sizing) is separate — lives in ThrottleAppConfig. 🔌🦆

use anyhow::Result;
use serde::Deserialize;

use crate::backends::SourceBackend;
use crate::backends::elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use crate::backends::file::{FileSource, FileSourceConfig};
use crate::backends::in_mem::InMemorySource;
use crate::backends::opensearch::{OpenSearchSource, OpenSearchSourceConfig};
use crate::backends::s3_rally::{S3RallySource, S3RallySourceConfig};
use crate::throttlers::SourceThrottleConfig;

/// 🎭 SourceConfig: the velvet rope at the backend club.
/// You are either a File, an Elasticsearch, or an InMemory.
/// There is no Other. There is no Unsupported. There is only the enum.
/// (Until someone files a feature request. There is always a feature request.)
///
/// 🧠 Knowledge graph: application-level config that the supervisor *uses* but doesn't *own*.
/// `build_backend()` resolves this into a `SourceBackend` for the worker pipeline. 🚰
#[derive(Debug, Deserialize, Clone)]
pub enum SourceConfig {
    /// 📡 Read from an Elasticsearch index via scroll API
    Elasticsearch(ElasticsearchSourceConfig),
    /// 📂 Read from a local file (NDJSON or Rally JSON array)
    File(FileSourceConfig),
    /// 🔍 Read from an OpenSearch index via PIT + search_after — the fork that reads
    OpenSearch(OpenSearchSourceConfig),
    /// 🪣 Stream Rally benchmark data from an S3 bucket — geonames, pmc, nyc_taxis, etc.
    S3Rally(S3RallySourceConfig),
    /// 🧪 In-memory test source — 4 hardcoded docs, no I/O, no regrets
    InMemory(()),
}

// 🧠 SourceConfig no longer has default_page_size() — that value now lives in
// ThrottleAppConfig.source.max_batch_size_docs. Backends are pure connection config. 🔌🦆

impl SourceConfig {
    /// 🏗️ Build a live SourceBackend from this config + throttle settings.
    ///
    /// 🧠 Knowledge graph: this was formerly `from_source_config()` in lib.rs.
    /// Moved here because config-to-backend resolution is the config's own business,
    /// not the orchestrator's. Like how a restaurant menu builds the dish, not the waiter. 🍽️
    ///
    /// The `throttle` arg provides batch sizing — max_batch_size_bytes and max_batch_size_docs.
    /// Backends that need them (File, S3Rally) receive them as constructor args.
    /// ES/OpenSearch use doc count via pump() hint. InMemory ignores everything. 🦆
    pub(crate) async fn build_backend(
        &self,
        throttle: &SourceThrottleConfig,
    ) -> Result<SourceBackend> {
        let max_batch_size_bytes = throttle.max_batch_size_bytes;
        let max_batch_size_docs = throttle.max_batch_size_docs;

        match self {
            SourceConfig::File(file_cfg) => {
                let src =
                    FileSource::new(file_cfg.clone(), max_batch_size_bytes, max_batch_size_docs)
                        .await?;
                Ok(SourceBackend::File(src))
            }
            SourceConfig::InMemory(_) => {
                let src = InMemorySource::new().await?;
                Ok(SourceBackend::InMemory(src))
            }
            SourceConfig::Elasticsearch(es_cfg) => {
                let src = ElasticsearchSource::new(es_cfg.clone()).await?;
                Ok(SourceBackend::Elasticsearch(src))
            }
            SourceConfig::OpenSearch(os_cfg) => {
                let src = OpenSearchSource::new(os_cfg.clone()).await?;
                Ok(SourceBackend::OpenSearch(src))
            }
            SourceConfig::S3Rally(s3_cfg) => {
                let src =
                    S3RallySource::new(s3_cfg.clone(), max_batch_size_bytes, max_batch_size_docs)
                        .await?;
                Ok(SourceBackend::S3Rally(src))
            }
        }
    }
}
