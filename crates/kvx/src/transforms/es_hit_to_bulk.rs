// ai
//! 🔄📡 ES/OpenSearch Hit → Bulk Format Transform — zero-copy edition 🏗️🚀
//!
//! 🎬 COLD OPEN — INT. DATA PIPELINE — THE DOCUMENTS REFUSE TO BE PARSED
//!
//! The source hands us documents in search hit format: `_id`, `_index`, `_source`.
//! The sink needs them in bulk API format: action line + source line.
//! The old code parsed every document into a `Value` tree, then re-serialized it.
//! That's like translating English to French by first converting to interpretive dance.
//!
//! This version uses `serde_json::RawValue` to borrow `_source` directly from the
//! input bytes. Zero parsing of the document body. Zero re-serialization. The `_id`
//! is also borrowed raw — already JSON-escaped from the source, ready to embed.
//!
//! ## Knowledge Graph 🧠
//! - Pattern: same as `RallyS3ToEs` — zero-sized struct, `impl Transform`
//! - Key optimization: `RawValue` borrows `_source` and `_id` from input — no `Value` tree
//! - `_source` (often kilobytes) is memcpy'd, never parsed. The biggest win.
//! - `_id` raw JSON token is embedded directly in action line — no escape function needed
//! - Single pre-sized `String` allocation per hit (was: multiple format!/to_string calls)
//! - `escape_json_string` eliminated — raw JSON tokens are already escaped by definition
//! - `build_bulk_action_line` inlined — one fewer function call in the hot path
//!
//! ⚠️ "He who borrows from the input, allocates not in vain." — Ancient serde proverb
//! 🦆 The optimization duck quacks in O(memcpy) instead of O(parse+serialize).

use super::Transform;
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::fmt::Write;

/// 🔍 Zero-copy search hit — borrows `_id` and `_source` directly from the input JSON.
///
/// Uses `RawValue` to skip parsing the document body entirely.
/// The `_source` field (often kilobytes of nested JSON) is never deserialized —
/// we just grab a reference to the raw bytes and pass them through.
///
/// `_id` is also raw — the JSON token (e.g. `"abc"` with quotes) is already escaped.
/// We embed it directly into the action line. No escaping function. No allocation.
///
/// Before this struct existed, we parsed the whole hit into `Value`. Every field.
/// Every nested object. Every array element. Like reading an entire novel to find the ISBN.
#[derive(Deserialize)]
struct SearchHit<'a> {
    /// 📋 Document identity — raw JSON token, already escaped. String, number, or absent.
    #[serde(borrow)]
    _id: Option<&'a RawValue>,
    /// 📦 Document body — raw JSON, never parsed, never re-serialized.
    #[serde(borrow)]
    _source: Option<&'a RawValue>,
}

/// 🔄 EsHitToBulk — converts search hit format to bulk API format.
///
/// Zero-sized struct that implements [`Transform`]. Same pattern as `RallyS3ToEs`.
/// No state, all logic in `transform()`.
///
/// ## What it does (per document) 🔧
///
/// 1. Typed deserialize: borrow `_id` and `_source` as `RawValue` references
/// 2. `_id` → embed raw JSON token in action line (already escaped)
/// 3. `_source` → memcpy raw JSON text as the body line (never parsed)
/// 4. Output: `"{action}\n{source}"` — the sacred two-line bulk format
///
/// ## Performance vs old approach 📊
///
/// | Step | Before | After |
/// |------|--------|-------|
/// | Parse hit | Full `Value` tree (recursive) | Typed struct, borrows 2 fields |
/// | `_source` | Parse → `Value` → `to_string()` | Raw memcpy from input |
/// | `_id` | Parse → match type → `escape_json_string()` → `format!()` | Embed raw token |
/// | Output | Multiple `format!()` + `to_string()` | Single pre-sized `String` |
///
/// The singularity won't happen before we finish parsing documents at this rate. But it'll be close.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EsHitToBulk;

impl Transform for EsHitToBulk {
    /// 🔄 Search hit page in, bulk action pairs out. Splits by `\n`, transforms each hit.
    ///
    /// Returns `Vec<Cow::Owned(...)>` — each item is a bulk pair (action\nsource).
    /// Owned because we construct new strings (even though _source isn't parsed,
    /// we still need a new String for the action_line+newline+source combo).
    ///
    /// # Errors
    /// 💀 Returns error if any line is not valid JSON or missing `_source`.
    #[inline]
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        raw_source_page
            .split('\n')
            .filter(|line| !line.trim().is_empty())
            .map(|line| transform_single_hit(line).map(Cow::Owned))
            .collect()
    }

    /// 🚀 Stream search-hit→bulk transform directly into the NDJSON output buffer.
    /// Skips Vec<Cow> — same streaming pattern as RallyS3ToEs. 🦆
    #[inline]
    fn transform_into_ndjson(&self, raw_source_page: &str, output: &mut String) -> Result<()> {
        for line in raw_source_page.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            let the_bulk_pair = transform_single_hit(line)?;
            output.push_str(&the_bulk_pair);
            output.push('\n');
        }
        Ok(())
    }
}

/// 🔧 Transform a single search hit JSON into bulk API format (action\nsource).
///
/// Uses zero-copy deserialization — `_source` is never parsed into a `Value` tree.
/// The raw JSON text is borrowed from the input and written directly to the output.
/// `_id` is also borrowed as its raw JSON token and embedded in the action line.
///
/// Input:  `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
/// Output: `{"index":{"_id":"abc"}}\n{"field":"val"}`
///
/// ## Per-document allocation budget 💰
/// - 1 `SearchHit` struct (2 pointers + discriminants — stack, ~32 bytes)
/// - 1 `String` output (pre-sized to input length + ~50 bytes overhead)
/// - 0 `Value` nodes (was: entire recursive document tree)
/// - 0 `escape_json_string` calls (was: per-document string scan + potential alloc)
///
/// "He who copies bytes instead of parsing trees, deploys before Friday." — Ancient bulk proverb
#[inline]
fn transform_single_hit(raw: &str) -> Result<String> {
    // 🔬 Phase 1: Typed deserialize — borrows _id and _source as raw JSON slices.
    // -- serde walks the input once, grabs two pointers. The rest of the hit? Ignored.
    // -- Like reading a book's title and ISBN without reading the actual book. Efficient.
    let hit: SearchHit = serde_json::from_str(raw).context(
        "💀 Search hit parse failed — the source emitted non-JSON. \
         Either corrupted upstream or the pipeline sprang a leak. \
         Check upstream. Call a plumber. File an insurance claim.",
    )?;

    // 📦 Phase 2: Grab _source — the raw JSON text, never parsed.
    // -- This is the big win. A 10KB document body? Just a &str slice. No tree. No nodes.
    let the_source_json = hit
        ._source
        .ok_or_else(|| {
            anyhow::anyhow!(
                "💀 Search hit missing '_source'. A hit without a body. \
                 Like a burrito without filling. Like a commit without a diff. \
                 Like a Monday without coffee. The hit was: {}",
                raw
            )
        })?
        .get();

    // 🏗️ Phase 3: Build output — one allocation, pre-sized.
    // -- Action line ~50 bytes + \n + source body. Slight over-allocation beats realloc.
    let mut the_sacred_output = String::with_capacity(50 + the_source_json.len());

    // 📡 Phase 4: Action line — embed raw _id directly (already JSON-escaped from source).
    // -- Old code: parse _id → match type → escape_json_string() → format!()
    // -- New code: grab raw token → embed. That's it. That's the optimization.
    match hit._id {
        Some(raw_id) => {
            let id_json = raw_id.get();
            if id_json == "null" {
                // -- 🎲 Null _id? Let the engine auto-generate. Chaos, but the good kind.
                the_sacred_output.push_str(r#"{"index":{}}"#);
            } else if id_json.starts_with('"') {
                // -- 📋 String _id — already quoted + escaped in the raw JSON. Embed as-is.
                // -- This is the hot path. Most document IDs are strings. Zero escaping needed.
                write!(the_sacred_output, r#"{{"index":{{"_id":{id_json}}}}}"#).unwrap();
            } else {
                // -- 🔢 Numeric or exotic _id — wrap in quotes for the bulk action line.
                write!(the_sacred_output, r#"{{"index":{{"_id":"{id_json}"}}}}"#).unwrap();
            }
        }
        None => {
            // -- 🎲 No _id field at all. Auto-generate. Living dangerously.
            the_sacred_output.push_str(r#"{"index":{}}"#);
        }
    }

    // 🎯 Phase 5: The sacred newline separator + source body (raw copy, zero parsing).
    // -- push_str on a pre-sized String = memcpy. The fastest kind of "serialization."
    the_sacred_output.push('\n');
    the_sacred_output.push_str(the_source_json);

    Ok(the_sacred_output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

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

    /// 🧪 Special characters in _id get preserved through the raw JSON round-trip.
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
