// ai
//! 🎬 *[the transforms have been applied. the sink awaits. but someone... must assemble the payload.]*
//! *[dramatic zoom on a trait nobody knew they needed until the newlines got political]*
//!
//! 📦 The Collectors module — payload assembly, extracted and dignified.
//!
//! Takes a slice of transformed document strings and assembles them into
//! a single payload string in the wire format the sink expects. This is the
//! formatting step between "I transformed each doc" and "I sent the payload."
//!
//! 🧠 Knowledge graph:
//! - **NDJSON** (`NdjsonCollector`): `\n`-delimited. Used by ES `/_bulk` and file sinks.
//! - **JSON Array** (`JsonArrayCollector`): `[doc,doc,doc]`. Used by in-memory sinks for testing.
//! - **Resolution**: from `SinkConfig`, same pattern as backends and transforms.
//! - **Submodules**: `ndjson` owns `NdjsonCollector`, `json_array` owns `JsonArrayCollector`
//! - Zero serde for JSON array — just brackets and commas, artisan-grade string assembly.
//!
//! ```text
//! SinkWorker pipeline:
//!   channel → transform each → collector.collect(&[String]) → sink.send(payload)
//! ```
//!
//! The collector is the bouncer between transforms and sinks. It decides whether
//! your documents get newlines, commas, or brackets. It does not negotiate.
//!
//! 🦆 (the duck collects... ducks? rubber ones? unclear. the duck has no comment.)

mod json_array;
mod ndjson;

pub use json_array::JsonArrayCollector;
pub use ndjson::NdjsonCollector;

use crate::app_config::SinkConfig;

// ===== Trait =====

/// 📦 Assembles transformed doc strings into a final payload format.
///
/// Each sink type has its own wire format. NDJSON for Elasticsearch and files.
/// JSON array for in-memory testing. The collector handles this concern
/// so the SinkWorker, Sink, and Transform don't have to know about delimiters.
///
/// 🧠 Knowledge graph: this trait mirrors the `Transform` and `Source`/`Sink` pattern —
/// trait → concrete impls → enum dispatcher → from_config resolver.
///
/// Ancient proverb: "He who hardcodes '\n' in the worker, reformats in production."
pub trait PayloadCollector: std::fmt::Debug {
    /// 📦 Assemble a slice of transformed strings into a single payload.
    /// The input strings are already in their final per-document format.
    /// The collector only adds inter-document delimiters and framing.
    fn collect(&self, items: &[String]) -> String;
}

// ===== Dispatcher Enum =====

/// 🎭 The polymorphic collector — wraps concrete collectors, dispatches via match.
///
/// Same pattern as `DocumentTransformer`, `SourceBackend`, `SinkBackend`.
/// The compiler monomorphizes each arm. Branch prediction eliminates the match
/// after a couple iterations. The enum is a formality. The dispatch is basically free.
///
/// 🧠 Knowledge graph: resolved from `SinkConfig` because the payload format
/// is determined by where the data is going, not where it came from.
/// ES needs NDJSON. Files need NDJSON. InMemory wants JSON arrays. Simple.
#[derive(Debug, Clone)]
pub enum CollectorBackend {
    /// 📡 Newline-delimited JSON — one `\n` per transformed string
    Ndjson(NdjsonCollector),
    /// 📦 JSON array — `[`, commas, `]`, zero serde
    JsonArray(JsonArrayCollector),
}

impl CollectorBackend {
    /// 🔧 Resolve the collector from the sink config.
    ///
    /// | SinkConfig | Collector | Format |
    /// |---|---|---|
    /// | Elasticsearch | NdjsonCollector | `doc\ndoc\n` |
    /// | File | NdjsonCollector | `doc\ndoc\n` |
    /// | InMemory | JsonArrayCollector | `[doc,doc]` |
    pub fn from_sink_config(sink: &SinkConfig) -> Self {
        match sink {
            SinkConfig::Elasticsearch(_) => Self::Ndjson(NdjsonCollector),
            SinkConfig::File(_) => Self::Ndjson(NdjsonCollector),
            SinkConfig::InMemory(_) => Self::JsonArray(JsonArrayCollector),
        }
    }
}

impl PayloadCollector for CollectorBackend {
    #[inline]
    fn collect(&self, items: &[String]) -> String {
        match self {
            Self::Ndjson(c) => c.collect(items),
            Self::JsonArray(c) => c.collect(items),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 🧪 Dispatcher tests: the sorting hat of collectors

    #[test]
    fn backend_the_one_where_es_config_resolves_to_ndjson() {
        // 🧪 Elasticsearch → NDJSON. No surprises. The sorting hat spoke clearly.
        use crate::backends::elasticsearch::ElasticsearchSinkConfig;
        let config = SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
            url: "http://localhost:9200".into(),
            username: None,
            password: None,
            api_key: None,
            index: None,
            common_config: Default::default(),
        });
        let collector = CollectorBackend::from_sink_config(&config);
        assert!(matches!(collector, CollectorBackend::Ndjson(_)));
    }

    #[test]
    fn backend_the_one_where_inmemory_resolves_to_json_array() {
        // 🧪 InMemory → JsonArray. Tests want valid JSON. Tests get valid JSON. 🎯
        let config = SinkConfig::InMemory(());
        let collector = CollectorBackend::from_sink_config(&config);
        assert!(matches!(collector, CollectorBackend::JsonArray(_)));
    }

    #[test]
    fn backend_the_one_where_file_resolves_to_ndjson() {
        // 🧪 File → NDJSON. Files and ES are kindred spirits. Both love newlines.
        use crate::backends::file::FileSinkConfig;
        let config = SinkConfig::File(FileSinkConfig {
            file_name: "output.json".into(),
            common_config: Default::default(),
        });
        let collector = CollectorBackend::from_sink_config(&config);
        assert!(matches!(collector, CollectorBackend::Ndjson(_)));
    }
}
