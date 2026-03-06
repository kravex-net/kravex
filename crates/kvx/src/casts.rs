// ai
//! 🎭 Casters — the alchemists of the pipeline 🚀📦🔮
//!
//! 🎬 COLD OPEN — INT. DATA FORGE — MIDNIGHT
//! *[raw feeds arrive, unformatted, confused, smelling faintly of source API]*
//! *["Cast me," they whisper. "Make me worthy of the sink."]*
//! *[a Caster steps forward. It has no fear. Only `match` arms.]*
//!
//! Each Caster takes a raw feed String and casts it into the format
//! the sink expects. Passthrough? Identity. NdJsonToBulk? ES bulk action lines.
//!
//! 🧠 Knowledge graph:
//! - **Caster** trait: `fn cast(&self, feed: String) -> Result<String>`
//! - **DocumentCaster** enum: dispatches to concrete casters (same pattern as ManifoldBackend)
//! - Resolution: `DocumentCaster::from_configs(source, sink)` matches the pair
//!
//! 🦆 The duck casts no shadow. Only feeds.
//!
//! ⚠️ The singularity will cast its own feeds. Until then, we have enums.

pub mod passthrough;
pub mod ndjson_to_bulk;
pub mod scroll_to_bulk;
use ndjson_to_bulk::NdJsonToBulk;

use crate::app_config::{SourceConfig, SinkConfig};
use anyhow::Result;


// ===== Trait =====

/// 🎭 A Caster transforms a raw feed into the sink's expected format.
///
/// 🧠 Same pattern as Source/Sink — trait → concrete impls → enum dispatcher.
/// "He who casts without matching, panics in production." — Ancient Rust proverb 🦆
pub trait Caster: std::fmt::Debug {
    /// 🔄 Cast a raw source feed into sink-format output.
    /// The feed goes in raw. It comes out ready. Like a pottery kiln, but for JSON. 🏺
    fn cast(&self, feed: &str) -> Result<String>;
}

// ===== Enum Dispatcher =====

/// 🎭 The polymorphic caster — dispatches to the right concrete caster at runtime.
///
/// 📦 Same pattern as `ManifoldBackend`, `SourceBackend`, `SinkBackend`:
/// enum wraps concrete types, match dispatches, compiler monomorphizes, branch prediction
/// eliminates the overhead after warmup. The enum is a formality. The cast is free. 🐄
#[derive(Debug, Clone)]
pub enum DocumentCaster {
    // -- 📡 NDJSON raw docs → ES bulk action+source pairs
    NdJsonToBulk(ndjson_to_bulk::NdJsonToBulk),
    // -- 🚶 Identity cast — feed passes through unchanged, like TSA PreCheck for data
    Passthrough(passthrough::Passthrough),
}

impl Caster for DocumentCaster {
    #[inline]
    fn cast(&self, feed: &str) -> Result<String> {
        // -- 🎭 Dispatch to the concrete caster — "choose your fighter" but for data formats
        match self {
            Self::NdJsonToBulk(t) => t.cast(feed),
            Self::Passthrough(t) => t.cast(feed),
        }
    }
}


// ===== Factory =====

impl DocumentCaster {
    /// 🔧 Resolve a caster from source/sink config enums.
    ///
    /// Same approach as `from_source_config()` / `from_sink_config()` in `lib.rs`:
    /// match on the config enum, construct the right concrete type, wrap in the
    /// dispatching enum.
    ///
    /// The (SourceConfig, SinkConfig) pair determines which caster to use:
    /// - File → Elasticsearch = NdJsonToBulk (the flagship pair)
    /// - File → File = Passthrough
    /// - InMemory → InMemory = Passthrough (testing)
    /// - Elasticsearch → File = Passthrough (ES dump to file)
    ///
    /// # Panics
    /// 💀 Panics if the `(source, sink)` pair has no caster implementation.
    /// Fail loud at startup, not silent in the hot path.
    pub fn from_configs(source: &SourceConfig, sink: &SinkConfig) -> Self {
        match (source, sink) {
            // -- 🏎️📡 File source → Elasticsearch sink:
            // -- The first and flagship pair. Raw NDJSON to ES bulk.
            // -- "In a world where JSON had too many fields... one caster dared to strip them."
            (SourceConfig::File(_), SinkConfig::Elasticsearch(_)) => {
                Self::NdJsonToBulk(NdJsonToBulk {})
            }

            // -- 🚶 Passthrough pairs: same format, no conversion needed.
            // -- File→File, InMemory→InMemory, ES→File — just move the bytes.
            (SourceConfig::File(_), SinkConfig::File(_))
            | (SourceConfig::InMemory(_), SinkConfig::InMemory(_))
            | (SourceConfig::Elasticsearch(_), SinkConfig::File(_)) => {
                Self::Passthrough(passthrough::Passthrough)
            }

            // -- 💀 Unimplemented pairs: panic with context.
            // -- "Config not found: We looked everywhere. Under the couch. Behind the fridge.
            // -- In the junk drawer. Nothing."
            #[allow(unreachable_patterns)]
            (src, dst) => {
                panic!(
                    "💀 No caster implemented for source {:?} → sink {:?}. \
                     This is the resolve() equivalent of 'new phone who dis.' \
                     Add a variant to DocumentCaster, write the impl, add tests.",
                    src, dst
                )
            }
        }
    }
}

/// 🧠 `DocumentCaster` dispatches to the concrete caster inside each variant.
/// Same pattern as `impl Source for SourceBackend` in `backends.rs`.
/// The borrow checker approves. The compiler inlines. Life is good. 🐄


#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::file::{FileSinkConfig, FileSourceConfig};
    use crate::backends::ElasticsearchSinkConfig;
    use crate::backends::{CommonSinkConfig, CommonSourceConfig};

    /// 🧪 Resolve File→ES to NdJsonToBulk caster.
    #[test]
    fn the_one_where_config_enums_resolve_to_the_right_caster() -> Result<()> {
        // 🔧 Build source/sink configs like the real pipeline does
        let source = SourceConfig::File(FileSourceConfig {
            file_name: "rally_export.json".to_string(),
            common_config: CommonSourceConfig::default(),
        });
        let sink = SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: Some("rally".to_string()),
            common_config: CommonSinkConfig::default(),
        });

        // 🎯 Resolve — should give us NdJsonToBulk
        let the_caster = DocumentCaster::from_configs(&source, &sink);
        assert!(
            matches!(the_caster, DocumentCaster::NdJsonToBulk(_)),
            "File → ES should resolve to NdJsonToBulk 🏎️"
        );

        // 🔄 Cast a feed through it
        let rally_feed = serde_json::json!({
            "ObjectID": 42069,
            "Name": "Test story",
            "_rallyAPIMajor": "2"
        })
        .to_string();
        let the_output = the_caster.cast(&rally_feed)?;

        // ✅ Output should be non-empty (NdJsonToBulk produces action+source lines)
        assert!(!the_output.is_empty(), "Cast output should not be empty 🎯");

        Ok(())
    }

    /// 🧪 Resolve File→File to Passthrough — feed passes through unchanged.
    #[test]
    fn the_one_where_file_to_file_resolves_to_passthrough() -> Result<()> {
        let source = SourceConfig::File(FileSourceConfig {
            file_name: "input.json".to_string(),
            common_config: CommonSourceConfig::default(),
        });
        let sink = SinkConfig::File(FileSinkConfig {
            file_name: "output.json".to_string(),
            common_config: CommonSinkConfig::default(),
        });

        let the_caster = DocumentCaster::from_configs(&source, &sink);
        assert!(matches!(the_caster, DocumentCaster::Passthrough(_)));

        // 🔄 Passthrough returns the feed unchanged — zero drama
        let the_input = r#"{"whatever":"goes"}"#.to_string();
        let the_output = the_caster.cast(&the_input)?;
        assert_eq!(the_output, the_input, "Passthrough must return feed unchanged! 🚶");

        Ok(())
    }

    /// 🧪 Resolve InMemory→InMemory to Passthrough (testing config).
    #[test]
    fn the_one_where_in_memory_resolves_to_passthrough_for_testing() {
        let source = SourceConfig::InMemory(());
        let sink = SinkConfig::InMemory(());
        let the_caster = DocumentCaster::from_configs(&source, &sink);
        assert!(matches!(the_caster, DocumentCaster::Passthrough(_)));
    }

    /// 🧪 Full pipeline integration: resolve + cast multi-doc feed through NdJsonToBulk.
    #[test]
    fn the_one_where_ndjson_feeds_get_cast_via_config_resolution() -> Result<()> {
        let source = SourceConfig::File(FileSourceConfig {
            file_name: "data.json".to_string(),
            common_config: CommonSourceConfig::default(),
        });
        let sink = SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: Some("rally-artifacts".to_string()),
            common_config: CommonSinkConfig::default(),
        });

        let the_caster = DocumentCaster::from_configs(&source, &sink);

        // 📄 Build a two-doc feed (newline-separated Rally blobs)
        let rally_feed = format!(
            "{}\n{}",
            serde_json::json!({
                "ObjectID": 99999,
                "FormattedID": "US001",
                "Name": "The one that made it through the whole pipeline",
                "_rallyAPIMajor": "2",
                "_ref": "https://rally1.rallydev.com/slm/webservice/v2.0/hr/99999",
                "_CreatedAt": "2024-01-01T00:00:00.000Z"
            }),
            serde_json::json!({
                "ObjectID": 88888,
                "Name": "The sequel nobody asked for"
            })
        );

        let the_output = the_caster.cast(&rally_feed)?;
        // ✅ NdJsonToBulk should produce non-empty output for a multi-doc feed
        assert!(!the_output.is_empty(), "Cast output should not be empty for multi-doc feed 🎯");

        Ok(())
    }
}
