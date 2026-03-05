// ai
//! 🎬 *[two manifolds walk into a bar. the dispatcher buys both a drink.]*
//! *[one wants newlines. one wants brackets. the enum holds them both.]*
//! *["In a world where payloads needed joining... one enum dared to dispatch."]*
//!
//! 🎭 **ManifoldBackend** — polymorphic dispatcher resolved from `SinkConfig`.
//!
//! 🧠 Knowledge graph:
//! - Same pattern as `DocumentCaster`, `SourceBackend`, `SinkBackend`
//! - Resolution: SinkConfig → ManifoldBackend::from_sink_config() → concrete manifold
//! - ES/File → NdjsonManifold | InMemory → JsonArrayManifold
//! - The compiler monomorphizes each arm; branch prediction eliminates the match
//!   after a couple iterations. The enum is a formality. The dispatch is basically free.
//! - Cloning ManifoldBackend is free — NdjsonManifold and JsonArrayManifold are zero-sized.
//!
//! 🦆 The duck asked why we need a backend enum when we have trait objects.
//!    We said "monomorphization." The duck left. It didn't want a lecture.

use super::{Manifold, JsonArrayManifold, NdjsonManifold};
use crate::app_config::SinkConfig;
use crate::casts::DocumentCaster;
use anyhow::Result;

// -- ┌─────────────────────────────────────────────────────────┐
// -- │  ManifoldBackend                                         │
// -- │  Enum → impl ManifoldBackend → impl Manifold → tests    │
// -- └─────────────────────────────────────────────────────────┘

/// 🎭 The polymorphic manifold — wraps concrete manifolds, dispatches via match.
///
/// Same pattern as `DocumentCaster`, `SourceBackend`, `SinkBackend`.
/// The compiler monomorphizes each arm. Branch prediction eliminates the match
/// after a couple iterations. The enum is a formality. The dispatch is basically free.
///
/// 🧠 Knowledge graph: resolved from `SinkConfig` because the payload format
/// is determined by where the data is going, not where it came from.
/// ES needs NDJSON. Files need NDJSON. InMemory wants JSON arrays. Simple.
#[derive(Debug, Clone)]
pub enum ManifoldBackend {
    /// 📡 Newline-delimited JSON — cast + join with `\n`
    Ndjson(NdjsonManifold),
    /// 📦 JSON array — cast + wrap in `[`, commas, `]`
    JsonArray(JsonArrayManifold),
}

impl ManifoldBackend {
    /// 🔄 Every manifold variant in one neat Vec — for benchmarks that auto-discover new backends.
    ///
    /// 🧠 Add a variant to the enum? It shows up in benchmarks automatically.
    /// Forget to add it here? The compiler won't catch it, but your conscience will. ⚖️
    /// (Actually, the `match` in this fn IS exhaustive, so the compiler catches it too. Phew.)
    pub fn all_variants() -> Vec<ManifoldBackend> {
        vec![
            // 📡 NDJSON — the lingua franca of bulk APIs everywhere
            Self::Ndjson(NdjsonManifold),
            // 📦 JSON array — for when your sink likes brackets more than newlines
            Self::JsonArray(JsonArrayManifold),
        ]
    }

    /// 🔧 Resolve the manifold from the sink config.
    ///
    /// | SinkConfig      | Manifold          | Format          |
    /// |-----------------|-------------------|-----------------|
    /// | Elasticsearch   | NdjsonManifold    | `item\nitem\n`  |
    /// | File            | NdjsonManifold    | `item\nitem\n`  |
    /// | InMemory        | JsonArrayManifold | `[item,item]`   |
    ///
    /// 🧠 Format follows the sink, not the source. The sink decides the wire format.
    /// This is the one true law. Do not question it. The borrow checker already has enough opinions.
    pub fn from_sink_config(sink: &SinkConfig) -> Self {
        match sink {
            // -- 📡 ES bulk requires NDJSON — action+source pairs, trailing \n
            SinkConfig::Elasticsearch(_) => Self::Ndjson(NdjsonManifold),
            // -- 📡 File sinks: NDJSON — one doc per line, trailing \n, everyone's happy
            SinkConfig::File(_) => Self::Ndjson(NdjsonManifold),
            // -- 📦 InMemory: JSON array — test assertions want `[doc1,doc2]` not `doc1\ndoc2\n`
            SinkConfig::InMemory(_) => Self::JsonArray(JsonArrayManifold),
        }
    }
}

impl Manifold for ManifoldBackend {
    #[inline]
    fn join(&self, feeds: &[String], caster: &DocumentCaster) -> Result<String> {
        // -- 🎭 Dispatch to the concrete manifold — the match arm that wins is the one that deserves to
        // -- TODO: win the lottery, retire, replace this with a lookup table. Just kidding. This is fine.
        match self {
            Self::Ndjson(m) => m.join(feeds, caster),
            Self::JsonArray(m) => m.join(feeds, caster),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::casts::passthrough::Passthrough;

    // -- 🔧 Passthrough caster helper — the identity function of the cast world
    fn passthrough_caster() -> DocumentCaster {
        DocumentCaster::Passthrough(Passthrough)
    }

    #[test]
    fn backend_the_one_where_es_config_resolves_to_ndjson() {
        use crate::backends::elasticsearch::ElasticsearchSinkConfig;
        let config = SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
            url: "http://localhost:9200".into(),
            username: None,
            password: None,
            api_key: None,
            index: None,
            common_config: Default::default(),
        });
        let manifold = ManifoldBackend::from_sink_config(&config);
        assert!(matches!(manifold, ManifoldBackend::Ndjson(_)));
    }

    #[test]
    fn backend_the_one_where_inmemory_resolves_to_json_array() {
        let config = SinkConfig::InMemory(());
        let manifold = ManifoldBackend::from_sink_config(&config);
        assert!(matches!(manifold, ManifoldBackend::JsonArray(_)));
    }

    #[test]
    fn backend_the_one_where_file_resolves_to_ndjson() {
        use crate::backends::file::FileSinkConfig;
        let config = SinkConfig::File(FileSinkConfig {
            file_name: "output.json".into(),
            common_config: Default::default(),
        });
        let manifold = ManifoldBackend::from_sink_config(&config);
        assert!(matches!(manifold, ManifoldBackend::Ndjson(_)));
    }

    #[test]
    fn backend_the_one_where_join_dispatches_correctly() -> Result<()> {
        // 🧪 ManifoldBackend dispatches to the right concrete manifold
        let manifold = ManifoldBackend::from_sink_config(&SinkConfig::InMemory(()));
        let feeds = vec![
            String::from(r#"{"a":1}"#),
            String::from(r#"{"b":2}"#),
        ];
        let result = manifold.join(&feeds, &passthrough_caster())?;
        assert_eq!(result, r#"[{"a":1},{"b":2}]"#);
        Ok(())
    }
}
