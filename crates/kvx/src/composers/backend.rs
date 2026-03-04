// ai
//! 🎬 *[two composers walk into a bar. the dispatcher buys both a drink.]*
//! *[one wants newlines. one wants brackets. the enum holds them both.]*
//! *["In a world where payloads needed composing... one enum dared to dispatch."]*
//!
//! 🎭 **ComposerBackend** — polymorphic dispatcher resolved from `SinkConfig`.
//!
//! 🧠 Knowledge graph:
//! - Same pattern as `DocumentTransformer`, `SourceBackend`, `SinkBackend`
//! - Resolution: SinkConfig → ComposerBackend::from_sink_config() → concrete composer
//! - ES/File → NdjsonComposer | InMemory → JsonArrayComposer
//! - The compiler monomorphizes each arm; branch prediction eliminates the match
//!   after a couple iterations. The enum is a formality. The dispatch is basically free.
//! - Cloning ComposerBackend is free — NdjsonComposer and JsonArrayComposer are zero-sized.
//! - `compose_page` (test-only) offloads CPU-bound composition to `spawn_blocking` — keeps the
//!   tokio runtime free for I/O while transforms crunch on dedicated threads.
//!
//! 🦆 The duck asked why we need a backend enum when we have trait objects.
//!    We said "monomorphization." The duck left. It didn't want a lecture.

use super::{Composer, JsonArrayComposer, NdjsonComposer};
use crate::app_config::SinkConfig;
use crate::buffer_pool::PoolBuffer;
use crate::transforms::DocumentTransformer;
use anyhow::Result;

// -- ┌─────────────────────────────────────────────────────────┐
// -- │  ComposerBackend                                        │
// -- │  Enum → impl ComposerBackend → impl Composer → tests   │
// -- └─────────────────────────────────────────────────────────┘

/// 🎭 The polymorphic composer — wraps concrete composers, dispatches via match.
///
/// Same pattern as `DocumentTransformer`, `SourceBackend`, `SinkBackend`.
/// The compiler monomorphizes each arm. Branch prediction eliminates the match
/// after a couple iterations. The enum is a formality. The dispatch is basically free.
///
/// 🧠 Knowledge graph: resolved from `SinkConfig` because the payload format
/// is determined by where the data is going, not where it came from.
/// ES needs NDJSON. Files need NDJSON. InMemory wants JSON arrays. Simple.
#[derive(Debug, Clone)]
pub(crate) enum ComposerBackend {
    /// 📡 Newline-delimited JSON — transform + join with `\n`
    Ndjson(NdjsonComposer),
    /// 📦 JSON array — transform + wrap in `[`, commas, `]`
    JsonArray(JsonArrayComposer),
}

impl ComposerBackend {
    /// 🔧 Resolve the composer from the sink config.
    ///
    /// | SinkConfig      | Composer          | Format          |
    /// |-----------------|-------------------|-----------------|
    /// | Elasticsearch   | NdjsonComposer    | `item\nitem\n`  |
    /// | File            | NdjsonComposer    | `item\nitem\n`  |
    /// | InMemory        | JsonArrayComposer | `[item,item]`   |
    ///
    /// 🧠 Format follows the sink, not the source. The sink decides the wire format.
    /// This is the one true law. Do not question it. The borrow checker already has enough opinions.
    pub(crate) fn from_sink_config(sink: &SinkConfig) -> Self {
        match sink {
            // -- 📡 ES bulk requires NDJSON — action+source pairs, trailing \n
            SinkConfig::Elasticsearch(_) => Self::Ndjson(NdjsonComposer),
            // -- 🔍 OpenSearch bulk: same NDJSON format as ES. The fork preserved the wire format.
            SinkConfig::OpenSearch(_) => Self::Ndjson(NdjsonComposer),
            // -- 📡 File sinks: NDJSON — one doc per line, trailing \n, everyone's happy
            SinkConfig::File(_) => Self::Ndjson(NdjsonComposer),
            // -- 📦 InMemory: JSON array — test assertions want `[doc1,doc2]` not `doc1\ndoc2\n`
            SinkConfig::InMemory(_) => Self::JsonArray(JsonArrayComposer),
        }
    }

    /// 🧵 Offload page composition to the tokio blocking thread pool.
    ///
    /// 🧠 SUPERSEDED: TransformWorker now calls `compose()` directly on a blocking thread.
    /// This method remains for tests that need the async + spawn_blocking wrapper.
    /// In production, the entire TransformWorker recv→compose→send loop runs on a single
    /// blocking thread — no per-page spawn_blocking hop needed.
    ///
    /// "You take the blue pill, you stay in the async runtime. You take the red pill,
    /// you spawn_blocking and I show you how deep the thread pool goes." — Morpheus, probably 🦆
    #[cfg(test)]
    pub(crate) async fn compose_page(
        &self,
        pages: Vec<PoolBuffer>,
        transformer: &DocumentTransformer,
    ) -> Result<PoolBuffer> {
        use anyhow::Context;
        let the_composer_clone = self.clone();
        let the_transformer_clone = transformer.clone();

        tokio::task::spawn_blocking(move || {
            the_composer_clone.compose(&pages, &the_transformer_clone)
        })
        .await
        .context(
            "💀 spawn_blocking panicked during compose — the thread pool is having an existential crisis 🧵",
        )?
    }
}

impl Composer for ComposerBackend {
    #[inline]
    fn compose(&self, pages: &[PoolBuffer], transformer: &DocumentTransformer) -> Result<PoolBuffer> {
        // -- 🎭 Dispatch to the concrete composer — the match arm that wins is the one that deserves to
        // -- TODO: win the lottery, retire, replace this with a lookup table. Just kidding. This is fine.
        match self {
            Self::Ndjson(c) => c.compose(pages, transformer),
            Self::JsonArray(c) => c.compose(pages, transformer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool;
    use crate::transforms::passthrough::Passthrough;

    // -- 🔧 Passthrough transformer helper — the identity function of the transform world
    fn passthrough_transformer() -> DocumentTransformer {
        DocumentTransformer::Passthrough(Passthrough)
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
        });
        let composer = ComposerBackend::from_sink_config(&config);
        assert!(matches!(composer, ComposerBackend::Ndjson(_)));
    }

    #[test]
    fn backend_the_one_where_inmemory_resolves_to_json_array() {
        let config = SinkConfig::InMemory(());
        let composer = ComposerBackend::from_sink_config(&config);
        assert!(matches!(composer, ComposerBackend::JsonArray(_)));
    }

    #[test]
    fn backend_the_one_where_file_resolves_to_ndjson() {
        use crate::backends::file::FileSinkConfig;
        let config = SinkConfig::File(FileSinkConfig {
            file_name: "output.json".into(),
        });
        let composer = ComposerBackend::from_sink_config(&config);
        assert!(matches!(composer, ComposerBackend::Ndjson(_)));
    }

    #[test]
    fn backend_the_one_where_compose_dispatches_correctly() -> Result<()> {
        // 🧪 ComposerBackend dispatches to the right concrete composer
        let composer = ComposerBackend::from_sink_config(&SinkConfig::InMemory(()));
        let pages = vec![
            buffer_pool::rent_from_string(String::from(r#"{"a":1}"#)),
            buffer_pool::rent_from_string(String::from(r#"{"b":2}"#)),
        ];
        let result = composer.compose(&pages, &passthrough_transformer())?;
        assert_eq!(result.as_str().unwrap(), r#"[{"a":1},{"b":2}]"#);
        Ok(())
    }

    /// 🧪 OpenSearch sink config resolves to NdjsonComposer — same wire format as ES.
    /// The fork kept the bulk API. The composer doesn't care about your licensing drama.
    #[test]
    fn backend_the_one_where_opensearch_resolves_to_ndjson() {
        use crate::backends::opensearch::OpenSearchSinkConfig;
        let config = SinkConfig::OpenSearch(OpenSearchSinkConfig {
            url: "https://localhost:9200".into(),
            username: None,
            password: None,
            api_key: None,
            index: Some("test-idx".into()),
            danger_accept_invalid_certs: true,
        });
        let composer = ComposerBackend::from_sink_config(&config);
        assert!(
            matches!(composer, ComposerBackend::Ndjson(_)),
            "OpenSearch sink → NdjsonComposer: the bulk API speaks NDJSON, always has, always will"
        );
    }

    /// 🧪 compose_page offloads to spawn_blocking and returns the same result as compose.
    /// "I used to compose on the main thread... but then I took a spawn_blocking to the knee." 🧵
    #[tokio::test]
    async fn backend_the_one_where_compose_page_works_like_compose_but_threaded() -> Result<()> {
        let composer = ComposerBackend::from_sink_config(&SinkConfig::InMemory(()));
        let pages = vec![
            buffer_pool::rent_from_string(String::from(r#"{"a":1}"#)),
            buffer_pool::rent_from_string(String::from(r#"{"b":2}"#)),
        ];
        let result = composer
            .compose_page(pages, &passthrough_transformer())
            .await?;
        assert_eq!(result.as_str().unwrap(), r#"[{"a":1},{"b":2}]"#);
        Ok(())
    }
}
