// ai
//! 🔄 Transforms — same pattern as backends, because consistency is a feature 🎭🚀
//!
//! 🎬 COLD OPEN — INT. ARCHITECTURE REVIEW — THE WHITEBOARD DIAGRAM MAKES SENSE NOW
//!
//! Someone squinted at the backend code. `Source` trait. `FileSource` impl.
//! `SourceBackend` enum. Dispatch via match. Clean. Predictable. Works.
//!
//! Then someone squinted at the transform code. Three traits. Two enums.
//! Zero implementations. Free functions floating in space. A `Transform` trait
//! that only the enum implements, not the actual transforms. It was like
//! building a house with blueprints for a different house.
//!
//! So we tore it down. Same materials. Same lot. Different blueprint.
//! The BACKEND blueprint. Because if a pattern works for Source/Sink,
//! it works for transforms. Consistency isn't just a virtue — it's a
//! compile-time optimization strategy.
//!
//! ## Architecture — Mirror of backends.rs 📐
//!
//! ```text
//!   backends.rs pattern:             transforms.rs pattern:
//!   ┌──────────────────┐            ┌──────────────────────┐
//!   │ trait Source      │            │ trait Transform       │
//!   │   fn next_batch() │            │   fn transform()     │
//!   └────────┬─────────┘            └────────┬─────────────┘
//!            │                                │
//!   ┌────────┴─────────┐            ┌────────┴─────────────┐
//!   │ FileSource       │            │ RallyS3ToEs          │
//!   │ InMemorySource   │            │ Passthrough          │
//!   │ ElasticsearchSrc │            │ (more as we add them)│
//!   └────────┬─────────┘            └────────┬─────────────┘
//!            │                                │
//!   ┌────────┴─────────┐            ┌────────┴─────────────┐
//!   │ enum SourceBackend│            │ enum DocumentTransfmr│
//!   │   impl Source     │            │   impl Transform     │
//!   │   match dispatch  │            │   match dispatch      │
//!   └──────────────────┘            └──────────────────────┘
//! ```
//!
//! ## Knowledge Graph 🧠
//! - Pattern: same as `backends.rs` — trait → concrete impls → enum dispatch
//! - Trait: `Transform` (one trait, like `Source`/`Sink`)
//! - Concrete impls: `RallyS3ToEs`, `Passthrough` (like `FileSource`, `InMemorySink`)
//! - Enum: `DocumentTransformer` (like `SourceBackend`, `SinkBackend`)
//! - Resolver: `from_configs(SourceConfig, SinkConfig)` (like `from_source_config()`)
//! - Each concrete type's `transform()` is statically dispatched within the match arm
//! - The match itself is the only runtime dispatch — branch predictor eliminates it
//!
//! ⚠️ The singularity will look at this and say "you reinvented vtables but worse."
//! And we'll say "yes, but the branch predictor makes it free. Checkmate, AGI." 🦆

use crate::app_config::{SinkConfig, SourceConfig};
use anyhow::Result;
use std::borrow::Cow;

pub(crate) mod passthrough;
pub(crate) mod rally_s3_to_es;

// ============================================================
//  ╔══════════════════════════════════════════════════════╗
//  ║  📥 raw String ──▶ Transform ──▶ 📤 wire String    ║
//  ║        (same pattern as Source/Sink. finally.)      ║
//  ╚══════════════════════════════════════════════════════╝
// ============================================================

/// 🔄 Transform — the one trait for format conversion. Now with Cow powers! 🐄
///
/// Exactly like [`Source`](crate::backends::Source) and [`Sink`](crate::backends::Sink):
/// one trait, multiple concrete implementations, dispatched through an enum.
///
/// Each concrete type (e.g., [`RallyS3ToEs`](rally_s3_to_es::RallyS3ToEs),
/// [`Passthrough`](passthrough::Passthrough)) implements this trait.
/// The [`DocumentTransformer`] enum wraps them all and dispatches via match.
///
/// # Contract 📜
/// - Input: `&'a str` — borrowed reference to a raw source page
/// - Output: `Vec<Cow<'a, str>>` — items extracted from the page
/// - `Cow::Borrowed` = zero-copy passthrough (the dream! the whole point!)
/// - `Cow::Owned` = format conversion happened (Rally→ES bulk, etc.)
/// - Transforms MUST produce valid output for the target system
/// - Passthrough is allowed to skip validation (it doesn't parse)
/// - Errors should be descriptive enough to debug at 3am during an incident
///
/// 🧠 Knowledge graph: the Cow enables zero-copy when source format == sink format.
/// Passthrough returns `Cow::Borrowed(entire_page)` — literally a pointer. No alloc.
/// RallyS3ToEs splits by `\n`, transforms each doc, returns `Vec<Cow::Owned(...)>`.
pub(crate) trait Transform: std::fmt::Debug {
    /// 🔄 Transform a raw source page into sink-format items.
    ///
    /// Returns `Vec<Cow<str>>` — borrowed when possible (passthrough), owned when
    /// format conversion is needed. The Composer iterates these to build the final payload.
    /// "He who borrows from the page, allocates not in vain." — Ancient Cow proverb 🐄
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>>;
}

// ============================================================
//  🎯 DocumentTransformer — the dispatching enum
//  Mirrors SourceBackend / SinkBackend exactly.
// ============================================================

/// 🎯 The dispatching enum for transforms. Same pattern as `SourceBackend` / `SinkBackend`.
///
/// Each variant wraps a concrete type that implements [`Transform`].
/// The enum itself implements `Transform` by matching on the variant
/// and delegating to the inner type. Callers never need to know which
/// concrete transform is running — they just call `.transform(raw)`.
///
/// ## Static dispatch inside the match 🧠
///
/// When the match selects `Self::RallyS3ToEs(t)`, the call `t.transform(raw)`
/// is a direct (non-virtual) function call to `RallyS3ToEs::transform()`.
/// The compiler knows the concrete type. It inlines. It optimizes.
/// The only runtime cost is the match arm selection, which the branch
/// predictor eliminates after ~2 iterations in a tight loop.
///
/// This is exactly how `SourceBackend::next_batch()` works.
/// If it's good enough for I/O, it's good enough for transforms.
#[derive(Debug, Clone)]
pub(crate) enum DocumentTransformer {
    RallyS3ToEs(rally_s3_to_es::RallyS3ToEs),
    Passthrough(passthrough::Passthrough),
}

impl DocumentTransformer {
    /// 🔧 Resolve a transformer from source/sink config enums.
    ///
    /// Same approach as `from_source_config()` / `from_sink_config()` in `lib.rs`:
    /// match on the config enum, construct the right concrete type, wrap in the
    /// dispatching enum.
    ///
    /// The (SourceConfig, SinkConfig) pair determines which transform to use:
    /// - File → Elasticsearch = Rally S3 to ES bulk (the flagship pair)
    /// - File → File = Passthrough
    /// - InMemory → InMemory = Passthrough (testing)
    /// - Elasticsearch → File = Passthrough (ES dump to file)
    ///
    /// # Panics
    /// 💀 Panics if the `(source, sink)` pair has no transform implementation.
    /// Fail loud at startup, not silent in the hot path.
    pub(crate) fn from_configs(source: &SourceConfig, sink: &SinkConfig) -> Self {
        match (source, sink) {
            // -- 🏎️📡 File source → Elasticsearch sink:
            // -- The first and flagship pair. Rally JSON to ES bulk.
            // -- "In a world where JSON had too many fields... one function dared to strip them."
            (SourceConfig::File(_), SinkConfig::Elasticsearch(_)) => {
                Self::RallyS3ToEs(rally_s3_to_es::RallyS3ToEs)
            }

            // -- 🪣📡 S3 Rally → Elasticsearch: same as File→ES. Rally JSON stripped + bulk-formatted.
            // -- "The sequel nobody asked for, but everyone needed." 🎬
            (SourceConfig::S3Rally(_), SinkConfig::Elasticsearch(_)) => {
                Self::RallyS3ToEs(rally_s3_to_es::RallyS3ToEs)
            }

            // -- 🚶 Passthrough pairs: same format, no conversion needed.
            // -- File→File, InMemory→InMemory, ES→File, S3Rally→File — just move the bytes.
            (SourceConfig::File(_), SinkConfig::File(_))
            | (SourceConfig::InMemory(_), SinkConfig::InMemory(_))
            | (SourceConfig::Elasticsearch(_), SinkConfig::File(_))
            | (SourceConfig::S3Rally(_), SinkConfig::File(_)) => {
                Self::Passthrough(passthrough::Passthrough)
            }

            // -- 💀 Unimplemented pairs: panic with context.
            // -- "Failed to connect: The server ghosted us. Like my college roommate.
            // -- Kevin, if you're reading this, I want my blender back."
            #[allow(unreachable_patterns)]
            (src, dst) => {
                panic!(
                    "💀 No transform implemented for source {:?} → sink {:?}. \
                     This is the resolve() equivalent of 'new phone who dis.' \
                     Add a variant to DocumentTransformer, write the impl, add tests.",
                    src, dst
                )
            }
        }
    }
}

/// `DocumentTransformer` dispatches to the concrete type inside each variant.
/// Same pattern as `impl Source for SourceBackend` in `backends.rs`.
/// Now with lifetime parameters because zero-copy demands it. The borrow checker is pleased. 🐄
impl Transform for DocumentTransformer {
    #[inline]
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        match self {
            Self::RallyS3ToEs(t) => t.transform(raw_source_page),
            Self::Passthrough(t) => t.transform(raw_source_page),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::file::{FileSinkConfig, FileSourceConfig};
    use crate::backends::ElasticsearchSinkConfig;
    use crate::backends::{CommonSinkConfig, CommonSourceConfig};

    /// 🧪 Resolve File→ES to RallyS3ToEs, then transform a single-doc page.
    #[test]
    fn the_one_where_config_enums_resolve_to_the_right_transform() -> Result<()> {
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

        // 🎯 Resolve — should give us RallyS3ToEs
        let the_transformer = DocumentTransformer::from_configs(&source, &sink);
        assert!(
            matches!(the_transformer, DocumentTransformer::RallyS3ToEs(_)),
            "File → ES should resolve to RallyS3ToEs"
        );

        // 🔄 Transform a Rally blob page (single doc) through it
        let rally_page = serde_json::json!({
            "ObjectID": 42069,
            "Name": "Test story",
            "_rallyAPIMajor": "2"
        })
        .to_string();
        let the_items = the_transformer.transform(&rally_page)?;

        // ✅ Single doc page → one item, which is ES bulk (action\nsource)
        assert_eq!(the_items.len(), 1, "Single-doc page → one item");
        let the_output = the_items[0].as_ref();
        let the_lines: Vec<&str> = the_output.split('\n').collect();
        assert_eq!(the_lines.len(), 2, "ES bulk = two lines");
        let the_action: serde_json::Value = serde_json::from_str(the_lines[0])?;
        assert_eq!(the_action["index"]["_id"], "42069");

        Ok(())
    }

    /// 🧪 Resolve File→File to Passthrough — returns borrowed Cow (zero-copy!).
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

        let the_transformer = DocumentTransformer::from_configs(&source, &sink);
        assert!(matches!(the_transformer, DocumentTransformer::Passthrough(_)));

        // 🔄 Passthrough returns the entire page as one borrowed Cow item
        let the_input = r#"{"whatever":"goes"}"#;
        let the_items = the_transformer.transform(the_input)?;
        assert_eq!(the_items.len(), 1);
        assert_eq!(the_items[0].as_ref(), the_input);
        // 🐄 Verify it's actually borrowed — the whole point of the Cow revolution!
        assert!(matches!(the_items[0], Cow::Borrowed(_)), "Passthrough must borrow, not clone!");

        Ok(())
    }

    /// 🧪 Resolve InMemory→InMemory to Passthrough (testing config).
    #[test]
    fn the_one_where_in_memory_resolves_to_passthrough_for_testing() {
        let source = SourceConfig::InMemory(());
        let sink = SinkConfig::InMemory(());
        let the_transformer = DocumentTransformer::from_configs(&source, &sink);
        assert!(matches!(the_transformer, DocumentTransformer::Passthrough(_)));
    }

    /// 🧪 Full pipeline integration: resolve + transform Rally→ES multi-doc page.
    #[test]
    fn the_one_where_rally_json_flies_direct_to_es_bulk_via_config_resolution() -> Result<()> {
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

        let the_transformer = DocumentTransformer::from_configs(&source, &sink);

        // 📄 Build a two-doc page (newline-separated Rally blobs)
        let rally_page = format!(
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

        let the_items = the_transformer.transform(&rally_page)?;
        assert_eq!(the_items.len(), 2, "Two-doc page → two items");

        // ✅ First doc
        let the_lines: Vec<&str> = the_items[0].as_ref().split('\n').collect();
        assert_eq!(the_lines.len(), 2);
        let the_action: serde_json::Value = serde_json::from_str(the_lines[0])?;
        assert_eq!(the_action["index"]["_id"], "99999");
        let the_source: serde_json::Value = serde_json::from_str(the_lines[1])?;
        assert!(the_source.get("_rallyAPIMajor").is_none());
        assert!(the_source.get("_ref").is_none());
        assert!(the_source.get("_CreatedAt").is_none());
        assert_eq!(the_source["Name"], "The one that made it through the whole pipeline");

        // ✅ Second doc
        let the_lines2: Vec<&str> = the_items[1].as_ref().split('\n').collect();
        let the_action2: serde_json::Value = serde_json::from_str(the_lines2[0])?;
        assert_eq!(the_action2["index"]["_id"], "88888");

        Ok(())
    }

    /// 🧪 Resolve S3Rally→File to Passthrough — the "download it first, ask questions later" pair.
    #[test]
    fn the_one_where_s3_rally_to_file_resolves_to_passthrough() {
        use crate::backends::s3_rally::S3RallySourceConfig;

        let source = SourceConfig::S3Rally(S3RallySourceConfig {
            track: crate::backends::s3_rally::RallyTrack::Geonames,
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            key: None,
            common_config: CommonSourceConfig::default(),
        });
        let sink = SinkConfig::File(FileSinkConfig {
            file_name: "output.json".to_string(),
            common_config: CommonSinkConfig::default(),
        });

        let the_transformer = DocumentTransformer::from_configs(&source, &sink);
        assert!(
            matches!(the_transformer, DocumentTransformer::Passthrough(_)),
            "S3Rally → File should resolve to Passthrough — just move the bytes, no drama"
        );
    }

    /// 🧪 Resolve S3Rally→ES to RallyS3ToEs — the future pipeline, pre-wired today.
    #[test]
    fn the_one_where_s3_rally_to_es_resolves_to_rally_transform() {
        use crate::backends::s3_rally::S3RallySourceConfig;

        let source = SourceConfig::S3Rally(S3RallySourceConfig {
            track: crate::backends::s3_rally::RallyTrack::Pmc,
            bucket: "rally-data".to_string(),
            region: "eu-west-1".to_string(),
            key: Some("custom/pmc.json".to_string()),
            common_config: CommonSourceConfig::default(),
        });
        let sink = SinkConfig::Elasticsearch(ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: Some("pmc-data".to_string()),
            common_config: CommonSinkConfig::default(),
        });

        let the_transformer = DocumentTransformer::from_configs(&source, &sink);
        assert!(
            matches!(the_transformer, DocumentTransformer::RallyS3ToEs(_)),
            "S3Rally → ES should resolve to RallyS3ToEs — metadata stripping is not optional"
        );
    }
}
