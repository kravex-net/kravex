// ai
//! 🏎️📡 Rally S3 JSON → Elasticsearch Bulk — direct, no layover 🔄✈️
//!
//! 🎬 COLD OPEN — INT. DEPARTURE GATE — RALLY→ES DIRECT FLIGHT — NOW BOARDING
//!
//! "Attention passengers: your layover through Hit International has been cancelled.
//! You will now fly direct. Do not collect an intermediate struct. Do not
//! allocate $200 of heap memory. Please proceed to Elasticsearch."
//!
//! This module owns the `RallyS3ToEs` struct which implements [`Transform`].
//! Same pattern as `FileSource` implementing `Source` in `backends/file.rs`.
//! The struct lives here. The `impl Transform` lives here. The tests live here.
//! Everything the transform needs is self-contained in this module.
//!
//! ## Knowledge Graph 🧠
//! - Struct: `RallyS3ToEs` — zero-sized, `impl Transform`
//! - Pattern: same as `FileSource impl Source` in `backends/file.rs`
//! - Input: Rally S3 JSON blob (ObjectID, FormattedID, metadata fields)
//! - Output: ES bulk NDJSON (two lines: action + source)
//! - Rally fields extracted: `ObjectID` → `_id`
//! - Rally fields stripped: `_rallyAPIMajor/Minor`, `_ref`, `_refObjectUUID`,
//!   `_objectVersion`, `_CreatedAt` (top-level only, nested refs survive)
//!
//! ⚠️ Rally was acquired by CA was acquired by Broadcom. The JSON outlived them all.
//! When the singularity arrives, it will find this struct still parsing ObjectIDs. 🦆

use super::Transform;
use anyhow::{Context, Result};
use serde_json::Value;
use std::borrow::Cow;

/// 🗑️ Rally API metadata fields stripped during transform.
///
/// Tribal knowledge: Rally's REST API wraps every object with these.
/// S3 exports sometimes include them. We strip unconditionally at the
/// top level — nested objects (Project._ref, Iteration._ref) survive
/// because they're relational data, not API cruft.
const THE_METADATA_FIELDS_WE_DONT_NEED: &[&str] = &[
    "_rallyAPIMajor",
    "_rallyAPIMinor",
    "_ref",
    "_refObjectUUID",
    "_objectVersion",
    "_CreatedAt",
];

/// 🏎️📡 RallyS3ToEs — parses Rally JSON, formats as ES bulk. Direct.
///
/// Zero-sized struct that implements [`Transform`]. Same pattern as
/// `FileSource` implementing `Source`. The struct holds no state — all
/// the logic lives in the `transform()` method.
///
/// ## What it does (all in one `transform()` call) 🔧
///
/// 1. Parse Rally JSON blob
/// 2. Extract `ObjectID` → ES `_id` (stringified)
/// 3. Strip 6 Rally API metadata fields (top-level only)
/// 4. Build ES bulk action line: `{"index":{"_id":"..."}}`
/// 5. Re-serialize cleaned body as source line
/// 6. Return `"{action}\n{source}"` — the sacred two-line format
#[derive(Debug, Clone, Copy)]
pub(crate) struct RallyS3ToEs;

impl Transform for RallyS3ToEs {
    /// 🔄 Rally JSON page in, ES bulk items out. Splits by `\n`, transforms each doc.
    ///
    /// Returns `Vec<Cow::Owned(...)>` — each item is an ES bulk pair (action\nsource).
    /// Owned because we parse, strip metadata, and re-serialize. No zero-copy here,
    /// but that's okay — Rally→ES is a format *conversion*, not a passthrough.
    ///
    /// # Errors
    /// 💀 Returns error if any line in the page is not valid JSON.
    #[inline]
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        // -- 📄 Split page by newlines, transform each non-empty line.
        // -- Like a conveyor belt at a sushi restaurant, except every plate is JSON
        // -- and the chef (transform_single_rally_doc) rewrites the menu on each one. 🍣
        raw_source_page
            .split('\n')
            .filter(|line| !line.trim().is_empty())
            .map(|line| transform_single_rally_doc(line).map(Cow::Owned))
            .collect()
    }
}

/// 🔧 Transform a single Rally JSON doc into ES bulk format (action\nsource).
///
/// 🧠 Extracted from the old `Transform::transform()` impl so the page-splitting
/// logic in `transform()` can call this per-line. Same 6-phase pipeline:
/// parse → extract _id → strip metadata → re-serialize → build action → format.
///
/// "He who extracts a helper, tests it twice." — Ancient refactoring proverb 📜
fn transform_single_rally_doc(raw: &str) -> Result<String> {
    // 🔬 Phase 1: Parse the Rally JSON blob
    let mut doc: Value = serde_json::from_str(raw).context(
        "💀 Rally JSON parse failed — the blob from S3 is not valid JSON. \
         Either the export is corrupted, someone uploaded a JPEG to the JSON bucket, \
         or Mercury is in retrograde. Check the source data and try again.",
    )?;

    // 🎯 Phase 2: Extract ObjectID → _id
    // Rally can't decide if ObjectID is a number or a string. We handle both.
    let the_document_identity = doc.get("ObjectID").map(|oid| match oid {
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        // 🦆 Bool? Array? Null? Just stringify it and move on.
        honestly_who_knows => honestly_who_knows.to_string(),
    });

    // 🗑️ Phase 3: Strip Rally API metadata (top-level only)
    if let Value::Object(ref mut map) = doc {
        for doomed_field in THE_METADATA_FIELDS_WE_DONT_NEED {
            map.remove(*doomed_field);
        }
    }

    // 📦 Phase 4: Re-serialize cleaned body
    let the_cleaned_body = serde_json::to_string(&doc).context(
        "💀 Failed to re-serialize cleaned Rally JSON. \
         The JSON went in valid and came out... not. File a bug against physics.",
    )?;

    // 📡 Phase 5: Build ES bulk action line
    let the_action_line = build_es_action_line(the_document_identity.as_deref());

    // 🎯 Phase 6: Two sacred lines, newline separated
    Ok(format!("{}\n{}", the_action_line, the_cleaned_body))
}

/// 🏗️ Build an ES bulk `{"index":{...}}` action line.
///
/// Includes `_id` if provided, omits it otherwise (ES auto-generates).
/// No `_index` or `routing` — those come from the sink config/URL.
///
/// Builds the JSON string directly to avoid a serde round-trip for this
/// trivial structure. Micro-optimization that matters at millions of docs.
#[inline]
fn build_es_action_line(the_document_id: Option<&str>) -> String {
    // -- 🏗️ The action line: ES bulk API's opening act. Like a cover letter, but for JSON.
    match the_document_id {
        Some(id) => {
            format!(r#"{{"index":{{"_id":"{}"}}}}"#, escape_json_string(id))
        }
        None => {
            // -- 🎲 No ID? ES will auto-generate one. Living dangerously.
            r#"{"index":{}}"#.to_string()
        }
    }
}

/// 🔧 Escape a string for safe JSON embedding.
///
/// Handles backslash, double quotes, control characters.
/// Fast path: if no special chars, return as-is (no allocation beyond to_string).
#[inline]
fn escape_json_string(s: &str) -> String {
    // -- 🔍 Fast path: most document IDs are alphanumeric
    if s.bytes().all(|b| b != b'"' && b != b'\\' && b >= 0x20) {
        return s.to_string();
    }

    // -- 🐢 Slow path: escape special characters
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

    #[test]
    fn the_one_where_rally_json_becomes_es_bulk_in_one_shot() -> Result<()> {
        // 🧪 The headline act. Single-doc page → one ES bulk item. Direct.
        let the_transformer = RallyS3ToEs;
        let rally_page = serde_json::json!({
            "ObjectID": 12345,
            "FormattedID": "US789",
            "Name": "As a user, I want to migrate data without crying",
            "_type": "HierarchicalRequirement",
            "_rallyAPIMajor": "2",
            "_rallyAPIMinor": "0",
            "_ref": "https://rally1.rallydev.com/slm/webservice/v2.0/hr/12345",
            "_refObjectUUID": "abc-123-def-456",
            "_objectVersion": "7",
            "_CreatedAt": "2023-01-15T10:00:00.000Z",
            "ScheduleState": "Accepted"
        })
        .to_string();

        let the_items = the_transformer.transform(&rally_page)?;
        assert_eq!(the_items.len(), 1, "Single-doc page → one item");

        let the_output = the_items[0].as_ref();
        let the_lines: Vec<&str> = the_output.split('\n').collect();
        assert_eq!(the_lines.len(), 2, "ES bulk = two lines, always");

        // 📋 Action line
        let the_action: serde_json::Value = serde_json::from_str(the_lines[0])?;
        assert_eq!(the_action["index"]["_id"], "12345");

        // 📦 Source line — metadata stripped, real fields survive
        let the_source: serde_json::Value = serde_json::from_str(the_lines[1])?;
        assert!(the_source.get("_rallyAPIMajor").is_none());
        assert!(the_source.get("_ref").is_none());
        assert_eq!(
            the_source["Name"],
            "As a user, I want to migrate data without crying"
        );
        assert_eq!(the_source["_type"], "HierarchicalRequirement");

        Ok(())
    }

    #[test]
    fn the_one_where_multi_doc_page_produces_multiple_items() -> Result<()> {
        // 🧪 Two-doc page → two items. The page-splitting works!
        let rally_page = format!(
            "{}\n{}",
            serde_json::json!({"ObjectID": 111, "Name": "first"}),
            serde_json::json!({"ObjectID": 222, "Name": "second"})
        );
        let the_items = RallyS3ToEs.transform(&rally_page)?;
        assert_eq!(the_items.len(), 2, "Two docs on the page → two items");
        // 🐄 Both should be Owned — Rally→ES is a conversion, not passthrough
        assert!(matches!(the_items[0], Cow::Owned(_)));
        assert!(matches!(the_items[1], Cow::Owned(_)));
        Ok(())
    }

    #[test]
    fn the_one_where_string_object_id_works() -> Result<()> {
        let page = serde_json::json!({"ObjectID": "67890", "Name": "test"}).to_string();
        let the_items = RallyS3ToEs.transform(&page)?;
        let the_action: serde_json::Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        assert_eq!(the_action["index"]["_id"], "67890");
        Ok(())
    }

    #[test]
    fn the_one_where_no_object_id_produces_empty_action() -> Result<()> {
        let page = serde_json::json!({"Name": "no identity"}).to_string();
        let the_items = RallyS3ToEs.transform(&page)?;
        let the_action: serde_json::Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        assert_eq!(the_action["index"], serde_json::json!({}));
        Ok(())
    }

    #[test]
    fn the_one_where_invalid_json_returns_error() {
        assert!(RallyS3ToEs.transform("not json").is_err());
    }

    #[test]
    fn the_one_where_nested_refs_survive_the_top_level_purge() -> Result<()> {
        let page = serde_json::json!({
            "ObjectID": 11111,
            "_rallyAPIMajor": "2",
            "Project": {"_ref": "https://rally1.rallydev.com/project/222", "Name": "Alpha"}
        })
        .to_string();
        let the_items = RallyS3ToEs.transform(&page)?;
        let the_source: serde_json::Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').nth(1).unwrap())?;
        assert!(
            the_source.get("_rallyAPIMajor").is_none(),
            "Top-level stripped"
        );
        assert!(the_source["Project"]["_ref"].is_string(), "Nested survives");
        Ok(())
    }

    #[test]
    fn the_one_where_special_chars_in_id_get_escaped() -> Result<()> {
        let page = serde_json::json!({"ObjectID": "doc\"with\\quotes"}).to_string();
        let the_items = RallyS3ToEs.transform(&page)?;
        let parsed: serde_json::Value =
            serde_json::from_str(the_items[0].as_ref().split('\n').next().unwrap())?;
        assert_eq!(parsed["index"]["_id"], "doc\"with\\quotes");
        Ok(())
    }

    #[test]
    fn the_one_where_escape_json_string_handles_the_usual_suspects() {
        assert_eq!(escape_json_string("normal"), "normal");
        assert_eq!(escape_json_string(r#"has"quotes"#), r#"has\"quotes"#);
        assert_eq!(escape_json_string("has\\backslash"), "has\\\\backslash");
        assert_eq!(escape_json_string("has\nnewline"), "has\\nnewline");
    }

    #[test]
    fn the_one_where_empty_lines_in_page_are_skipped() -> Result<()> {
        // 🧪 Pages may have trailing newlines or blank lines — they should be ignored
        let page = format!(
            "{}\n\n{}\n",
            serde_json::json!({"ObjectID": 1}),
            serde_json::json!({"ObjectID": 2})
        );
        let the_items = RallyS3ToEs.transform(&page)?;
        assert_eq!(
            the_items.len(),
            2,
            "Empty lines skipped, two real docs survive"
        );
        Ok(())
    }
}
