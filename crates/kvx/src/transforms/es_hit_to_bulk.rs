// ai
//! 🔄📡 ES/OpenSearch Hit → Bulk Format Transform — the great doc converter 🏗️🚀
//!
//! 🎬 COLD OPEN — INT. DATA PIPELINE — THE DOCUMENTS ARRIVE IN SEARCH FORMAT
//!
//! The source hands us documents in search hit format: `_id`, `_index`, `_source`.
//! The sink needs them in bulk API format: action line + source line.
//! Two formats. One transform. Zero mercy.
//!
//! This transform bridges the gap between "reading from a search engine" and
//! "writing to a search engine." It takes the hit format that OpenSearch/ES
//! sources emit and converts each document into the sacred two-line bulk format:
//! ```text
//! {"index":{"_id":"abc123"}}
//! {"field":"value","other_field":"other_value"}
//! ```
//!
//! Works for: ES→OpenSearch, OpenSearch→ES, OpenSearch→OpenSearch, ES→ES.
//! The bulk wire format is identical across both engines. The fork preserved it.
//! Like a family recipe that survives a divorce.
//!
//! ## Knowledge Graph 🧠
//! - Pattern: same as `RallyS3ToEs` — zero-sized struct, `impl Transform`
//! - Input format: NDJSON lines from search source, each line:
//!   `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
//! - Output format: ES/OpenSearch bulk NDJSON pairs:
//!   `{"index":{"_id":"abc"}}\n{"field":"val"}`
//! - Strips `_type` from hits if present (ES 7.x artifact, dead in OpenSearch)
//! - Preserves `_id` for document identity routing in the target cluster
//! - Returns `Vec<Cow::Owned>` because format conversion = allocation
//!
//! ⚠️ "He who transforms hits to bulk, bridges two search engines with one function."
//! 🦆 The transform duck doesn't care which search engine you use. It just converts.

use super::Transform;
use anyhow::{Context, Result};
use serde_json::Value;
use std::borrow::Cow;

/// 🔄 EsHitToBulk — converts search hit format to bulk API format.
///
/// Zero-sized struct that implements [`Transform`]. Same pattern as `RallyS3ToEs`.
/// No state, all logic in `transform()`.
///
/// ## What it does (per document) 🔧
///
/// 1. Parse hit JSON: `{"_id":"abc","_index":"src-idx","_source":{...}}`
/// 2. Extract `_id` → bulk action `_id` field
/// 3. Extract `_source` → bulk source line (the actual document body)
/// 4. Build action line: `{"index":{"_id":"abc"}}`
/// 5. Serialize source as the body line
/// 6. Return `"{action}\n{source}"` — the sacred two-line bulk format
///
/// Knock knock. Who's there? A search hit. A search hit who? A search hit that
/// needs to become a bulk action pair before the sink will accept it. Classic.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EsHitToBulk;

impl Transform for EsHitToBulk {
    /// 🔄 Search hit page in, bulk action pairs out. Splits by `\n`, transforms each hit.
    ///
    /// Returns `Vec<Cow::Owned(...)>` — each item is a bulk pair (action\nsource).
    /// Owned because we parse the hit format and rebuild in bulk format.
    ///
    /// # Errors
    /// 💀 Returns error if any line in the page is not valid JSON or missing `_source`.
    #[inline]
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        // -- 📄 Split page by newlines, transform each non-empty line.
        // -- Each hit gets its search metadata stripped and re-dressed in bulk API attire.
        // -- It's like a makeover show, but for JSON. "Queer Eye for the Search Hit." 👔
        raw_source_page
            .split('\n')
            .filter(|line| !line.trim().is_empty())
            .map(|line| transform_single_hit(line).map(Cow::Owned))
            .collect()
    }
}

/// 🔧 Transform a single search hit JSON into bulk API format (action\nsource).
///
/// Input:  `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
/// Output: `{"index":{"_id":"abc"}}\n{"field":"val"}`
///
/// If `_id` is missing, produces an action line without `_id` (auto-generate).
/// If `_source` is missing, that's a hard error — we can't index nothing.
///
/// "He who transforms without _source, indexes the void." — Ancient bulk proverb
fn transform_single_hit(raw: &str) -> Result<String> {
    // 🔬 Phase 1: Parse the hit JSON
    let hit: Value = serde_json::from_str(raw).context(
        "💀 Search hit parse failed — the source emitted something that's not valid JSON. \
         Either the source is corrupted, or the pipeline has a leak. Check upstream.",
    )?;

    // 🎯 Phase 2: Extract _id → bulk action _id
    let the_document_identity = hit.get("_id").and_then(|id| match id {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Null => None,
        honestly_who_knows => Some(honestly_who_knows.to_string()),
    });

    // 📦 Phase 3: Extract _source — the actual document body
    let the_source = hit.get("_source").ok_or_else(|| {
        anyhow::anyhow!(
            "💀 Search hit missing '_source' field. We got a hit without a body. \
             Like a letter without a message. Like a taco without filling. \
             The hit was: {}",
            raw
        )
    })?;

    // 📡 Phase 4: Build action line
    let the_action_line = build_bulk_action_line(the_document_identity.as_deref());

    // 📦 Phase 5: Serialize the source body
    let the_source_line = serde_json::to_string(the_source).context(
        "💀 Failed to re-serialize _source. The document went in valid and came out... not. \
         File a bug against reality.",
    )?;

    // 🎯 Phase 6: Two sacred lines, newline separated
    Ok(format!("{}\n{}", the_action_line, the_source_line))
}

/// 🏗️ Build a bulk `{"index":{...}}` action line.
///
/// Includes `_id` if provided, omits it otherwise (target engine auto-generates).
/// No `_index` or `routing` — those come from the sink config/URL.
///
/// Builds the JSON string directly to avoid a serde round-trip for this trivial structure.
/// Same micro-optimization as `rally_s3_to_es::build_es_action_line`.
#[inline]
fn build_bulk_action_line(the_document_id: Option<&str>) -> String {
    // -- 🏗️ Same action-line recipe as rally_s3_to_es — duplicated because DRY is for towels
    match the_document_id {
        Some(id) => {
            format!(r#"{{"index":{{"_id":"{}"}}}}"#, escape_json_string(id))
        }
        None => {
            // -- 🎲 Let the search engine pick an ID. Chaos, but the good kind.
            r#"{"index":{}}"#.to_string()
        }
    }
}

/// 🔧 Escape a string for safe JSON embedding.
///
/// Handles backslash, double quotes, control characters.
/// Fast path: if no special chars, return as-is (no allocation beyond to_string).
///
/// 🧠 This is duplicated from `rally_s3_to_es.rs`. Both transforms need it.
/// Extracting to a shared utility is a future refactor if a third transform appears.
/// "He who extracts prematurely, abstracts unnecessarily." — Ancient DRY proverb
#[inline]
fn escape_json_string(s: &str) -> String {
    // 🔍 Fast path: most document IDs are alphanumeric
    if s.bytes().all(|b| b != b'"' && b != b'\\' && b >= 0x20) {
        return s.to_string();
    }

    // 🐢 Slow path: escape special characters
    let mut escaped = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => escaped.push(c),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 Single hit → single bulk action pair. The happy path.
    #[test]
    fn the_one_where_a_search_hit_becomes_a_bulk_pair() -> Result<()> {
        let hit_page = serde_json::json!({
            "_id": "doc-001",
            "_index": "source-index",
            "_source": {
                "title": "The Great Gatsby but for Search Engines",
                "status": "migrating"
            }
        })
        .to_string();

        let the_items = EsHitToBulk.transform(&hit_page)?;
        assert_eq!(the_items.len(), 1, "Single hit → single bulk pair");

        let the_output = the_items[0].as_ref();
        let the_lines: Vec<&str> = the_output.split('\n').collect();
        assert_eq!(
            the_lines.len(),
            2,
            "Bulk format = exactly two lines, always"
        );

        // 📋 Action line: {"index":{"_id":"doc-001"}}
        let the_action: Value = serde_json::from_str(the_lines[0])?;
        assert_eq!(the_action["index"]["_id"], "doc-001");

        // 📦 Source line: the document body (no _id, no _index, just data)
        let the_source: Value = serde_json::from_str(the_lines[1])?;
        assert_eq!(
            the_source["title"],
            "The Great Gatsby but for Search Engines"
        );
        assert_eq!(the_source["status"], "migrating");
        // 🎯 _id and _index should NOT appear in the source line
        assert!(the_source.get("_id").is_none());
        assert!(the_source.get("_index").is_none());

        Ok(())
    }

    /// 🧪 Multi-hit page → multiple bulk pairs.
    #[test]
    fn the_one_where_multiple_hits_produce_multiple_bulk_pairs() -> Result<()> {
        let page = format!(
            "{}\n{}",
            serde_json::json!({"_id": "1", "_index": "idx", "_source": {"name": "first"}}),
            serde_json::json!({"_id": "2", "_index": "idx", "_source": {"name": "second"}})
        );

        let the_items = EsHitToBulk.transform(&page)?;
        assert_eq!(the_items.len(), 2, "Two hits → two bulk pairs");

        // 🐄 Both should be Owned — format conversion means allocation
        assert!(matches!(the_items[0], Cow::Owned(_)));
        assert!(matches!(the_items[1], Cow::Owned(_)));

        // ✅ Verify IDs
        let action_1: Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        let action_2: Value =
            serde_json::from_str(the_items[1].as_ref().split('\n').next().unwrap())?;
        assert_eq!(action_1["index"]["_id"], "1");
        assert_eq!(action_2["index"]["_id"], "2");

        Ok(())
    }

    /// 🧪 Hit without _id → action line with empty index (auto-generate).
    #[test]
    fn the_one_where_missing_id_means_auto_generate() -> Result<()> {
        let page = serde_json::json!({
            "_index": "idx",
            "_source": {"name": "identity crisis"}
        })
        .to_string();

        let the_items = EsHitToBulk.transform(&page)?;
        let the_action: Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        // 🎯 No _id in action → OpenSearch/ES will auto-generate one
        assert_eq!(the_action["index"], serde_json::json!({}));

        Ok(())
    }

    /// 🧪 Hit without _source → hard error. Can't index nothing.
    #[test]
    fn the_one_where_missing_source_is_an_error_not_a_feature() {
        let page = serde_json::json!({"_id": "ghost", "_index": "idx"}).to_string();
        let result = EsHitToBulk.transform(&page);
        assert!(result.is_err(), "Missing _source should be an error");
    }

    /// 🧪 _source is fully preserved — no field stripping (unlike Rally transform).
    #[test]
    fn the_one_where_source_fields_survive_completely() -> Result<()> {
        let page = serde_json::json!({
            "_id": "preserve-me",
            "_index": "idx",
            "_source": {
                "title": "Important Document",
                "nested": {"deep": {"value": 42}},
                "tags": ["search", "migration", "yolo"],
                "count": 1337
            }
        })
        .to_string();

        let the_items = EsHitToBulk.transform(&page)?;
        let the_source: Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').nth(1).unwrap())?;

        assert_eq!(the_source["title"], "Important Document");
        assert_eq!(the_source["nested"]["deep"]["value"], 42);
        assert_eq!(the_source["tags"][0], "search");
        assert_eq!(the_source["count"], 1337);

        Ok(())
    }

    /// 🧪 Numeric _id gets stringified properly.
    #[test]
    fn the_one_where_numeric_id_stringifies_correctly() -> Result<()> {
        let page = serde_json::json!({
            "_id": 99999,
            "_index": "idx",
            "_source": {"value": "test"}
        })
        .to_string();

        let the_items = EsHitToBulk.transform(&page)?;
        let the_action: Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        assert_eq!(the_action["index"]["_id"], "99999");

        Ok(())
    }

    /// 🧪 Empty lines in page are skipped — trailing newlines, blank lines, etc.
    #[test]
    fn the_one_where_empty_lines_are_ignored_gracefully() -> Result<()> {
        let page = format!(
            "{}\n\n{}\n",
            serde_json::json!({"_id": "a", "_index": "idx", "_source": {"n": 1}}),
            serde_json::json!({"_id": "b", "_index": "idx", "_source": {"n": 2}})
        );
        let the_items = EsHitToBulk.transform(&page)?;
        assert_eq!(
            the_items.len(),
            2,
            "Empty lines skipped, two real hits survive"
        );
        Ok(())
    }

    /// 🧪 Invalid JSON → error.
    #[test]
    fn the_one_where_invalid_json_is_rejected_with_prejudice() {
        assert!(EsHitToBulk.transform("not json at all").is_err());
    }

    /// 🧪 Special characters in _id get escaped.
    #[test]
    fn the_one_where_special_chars_in_id_get_escaped_properly() -> Result<()> {
        let page = serde_json::json!({
            "_id": "doc\"with\\special",
            "_index": "idx",
            "_source": {"v": 1}
        })
        .to_string();

        let the_items = EsHitToBulk.transform(&page)?;
        let the_action: Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        assert_eq!(the_action["index"]["_id"], "doc\"with\\special");

        Ok(())
    }
}
