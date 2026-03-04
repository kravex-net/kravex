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
use crate::buffer_pool::PoolBuffer;
use anyhow::{Context, Result};
use serde_json::Value;
use std::borrow::Cow;
use std::fmt::Write;

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

    /// 🚀 Stream Rally→ES bulk transform directly into the NDJSON output buffer.
    ///
    /// Skips the intermediate `Vec<Cow<str>>` entirely — each doc is transformed
    /// and its action\nsource pair is written straight into the output. The Vec
    /// and Cow::Owned wrappers were pure overhead when the destination is NDJSON.
    ///
    /// 🧠 Performance: eliminates 1 Vec alloc + N Cow::Owned wrappers per page.
    /// For a 1000-doc page, that's 1001 fewer allocations. "Every alloc you don't
    /// make is an alloc the garbage collector doesn't have to think about." 📜🦆
    #[inline]
    fn transform_into_ndjson(&self, raw_source_page: &str, output: &mut String) -> Result<()> {
        for line in raw_source_page.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            // 🏗️ Write action\nsource directly into the output buffer + trailing \n.
            let the_bulk_pair = transform_single_rally_doc(line)?;
            output.push_str(&the_bulk_pair);
            output.push('\n');
        }
        Ok(())
    }

    /// 🏦 Pool buffer edition — Rally JSON flows through managed memory like immigrants
    /// through customs: strip the metadata, keep the good stuff, stamp it with _id. 🛂🦆
    ///
    /// 🧠 Zero per-doc allocations. write!() goes into PoolBuffer via fmt::Write.
    /// serde_json::to_writer goes into PoolBuffer via io::Write. No intermediate String.
    /// At 10K docs/page, that's 20K fewer allocator calls (1 alloc + 1 free per doc, gone).
    /// "The allocator filed for unemployment. We automated its job." 🤖🦆
    #[inline]
    fn transform_into_pool_buffer(&self, source: &PoolBuffer, output: &mut PoolBuffer) -> Result<()> {
        let source_str = source.as_str().map_err(|e| {
            anyhow::anyhow!("💀 Source buffer contains invalid UTF-8. Rally docs have gone rogue. Error: {e}")
        })?;
        for line in source_str.split('\n') {
            if line.trim().is_empty() {
                continue;
            }
            write_rally_doc_to_buffer(line, output)?;
            output.push_byte(b'\n');
        }
        Ok(())
    }
}

/// 🔧 Transform a single Rally JSON doc into ES bulk format (action\nsource).
///
/// 🧠 Single-allocation pipeline: parse → extract _id → strip metadata → write
/// action line + body directly into one pre-sized String. Was 3 allocs per doc
/// (escape + action line + format!), now 1. The allocator can finally rest. 💤
///
/// "He who allocates once, profiles not in vain." — Ancient heap proverb 📜
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

    // 🏗️ Phase 4: Single pre-sized allocation for action line + \n + body.
    // 🧠 +64 covers the action line overhead ({"index":{"_id":"..."}}\n).
    // Old code: escape_json_string() → String, build_es_action_line() → String,
    // format!("{}\n{}", ...) → String = 3 allocs. Now: 1 alloc. The holy grail. 🏆
    let mut the_sacred_output = String::with_capacity(raw.len() + 64);

    // 📡 Phase 5: Write action line directly — no intermediate String.
    // -- 🏗️ The action line: ES bulk API's opening act. Like a cover letter, but for JSON.
    match the_document_identity.as_deref() {
        Some(id) => {
            write!(
                the_sacred_output,
                r#"{{"index":{{"_id":"{}"}}}}"#,
                escape_json_string(id)
            )
            .unwrap();
        }
        None => {
            // -- 🎲 No ID? ES will auto-generate one. Living dangerously.
            the_sacred_output.push_str(r#"{"index":{}}"#);
        }
    }
    the_sacred_output.push('\n');

    // 📦 Phase 6: Re-serialize cleaned body directly into the output buffer.
    // 🧠 serde_json::to_writer writes valid UTF-8 per its docs — safe to treat as String.
    // Old code: to_string() → intermediate String → push_str. Now: to_writer → direct write.
    // Eliminates the biggest per-doc allocation (~500 bytes avg for Rally/geonames docs).
    // es_hit_to_bulk.rs already uses this pattern — we're just adopting the family recipe.
    {
        // SAFETY: serde_json::to_writer for Value always produces valid UTF-8 JSON.
        // This is documented behavior and used widely (including in es_hit_to_bulk.rs).
        let the_raw_bytes = unsafe { the_sacred_output.as_mut_vec() };
        serde_json::to_writer(the_raw_bytes, &doc).context(
            "💀 Failed to serialize cleaned Rally JSON into output buffer. \
             The JSON went in valid and came out... not. File a bug against physics.",
        )?;
    }

    Ok(the_sacred_output)
}

/// 🔧 Write a single Rally doc directly into a PoolBuffer — zero per-doc allocations. 🚀
///
/// Same logic as `transform_single_rally_doc()` but writes into the output buffer instead
/// of returning a String. The write!() macro uses PoolBuffer's fmt::Write impl for the
/// action line. serde_json::to_writer uses PoolBuffer's io::Write impl for the body.
/// Two Write traits, zero intermediate Strings, one happy allocator. 🏖️
///
/// ## Per-document allocation budget 💰
/// - 1 `Value` tree (necessary for field stripping — serde_json owns it)
/// - 1 `String` for ObjectID extraction (small, ~10 bytes — could optimize later)
/// - 0 output `String` allocations (was: 1 pre-sized String per doc)
/// - serde_json::to_writer writes body bytes directly into PoolBuffer
///
/// "First we pooled the buffers. Then we eliminated the Strings. Next: world domination." 🦆
#[inline]
fn write_rally_doc_to_buffer(raw: &str, output: &mut PoolBuffer) -> Result<()> {
    // 🔬 Phase 1: Parse the Rally JSON blob
    let mut doc: Value = serde_json::from_str(raw).context(
        "💀 Rally JSON parse failed — the blob from S3 is not valid JSON. \
         Either the export is corrupted, someone uploaded a JPEG to the JSON bucket, \
         or Mercury is in retrograde. Check the source data and try again.",
    )?;

    // 🎯 Phase 2: Extract ObjectID → _id
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

    // 📡 Phase 4: Action line — write!() goes directly into PoolBuffer via fmt::Write.
    // No intermediate String. The bytes land in managed memory on the first write. 🏦
    match the_document_identity.as_deref() {
        Some(id) => {
            write!(
                output,
                r#"{{"index":{{"_id":"{}"}}}}"#,
                escape_json_string(id)
            )
            .unwrap();
        }
        None => {
            // 🎲 No ID? ES will auto-generate one. Living dangerously.
            output.push_str(r#"{"index":{}}"#);
        }
    }
    output.push_byte(b'\n');

    // 📦 Phase 5: Re-serialize cleaned body directly into PoolBuffer via io::Write.
    // 🧠 serde_json::to_writer writes valid UTF-8 per its docs. PoolBuffer implements
    // io::Write. The bytes go straight from serde's serializer into our managed buffer.
    // No intermediate String. No unsafe as_mut_vec(). Just pure trait-based polymorphism. ✨
    serde_json::to_writer(&mut *output, &doc).context(
        "💀 Failed to serialize cleaned Rally JSON into output buffer. \
         The JSON went in valid and came out... not. File a bug against physics.",
    )?;

    Ok(())
}

/// 🔧 Escape a string for safe JSON embedding. Returns `Cow::Borrowed` when
/// no escaping needed (99.9% of IDs) — zero allocation on the fast path. 🐄
///
/// Handles backslash, double quotes, control characters.
/// 🧠 Tribal knowledge: Rally ObjectIDs are always numeric strings ("12345").
/// The fast path borrows directly — the borrow checker is *delighted*.
#[inline]
fn escape_json_string(s: &str) -> Cow<'_, str> {
    // -- 🔍 Fast path: most document IDs are alphanumeric — borrow, don't clone
    if s.bytes().all(|b| b != b'"' && b != b'\\' && b >= 0x20) {
        return Cow::Borrowed(s);
    }

    // -- 🐢 Slow path: escape special characters — allocate only when we must
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
    Cow::Owned(escaped)
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
        // 🐄 Fast path: no special chars → Cow::Borrowed (zero alloc!)
        assert_eq!(escape_json_string("normal").as_ref(), "normal");
        assert!(matches!(escape_json_string("normal"), Cow::Borrowed(_)));
        // 🐢 Slow path: special chars → Cow::Owned
        assert_eq!(escape_json_string(r#"has"quotes"#).as_ref(), r#"has\"quotes"#);
        assert_eq!(escape_json_string("has\\backslash").as_ref(), "has\\\\backslash");
        assert_eq!(escape_json_string("has\nnewline").as_ref(), "has\\nnewline");
        assert!(matches!(escape_json_string(r#"has"quotes"#), Cow::Owned(_)));
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

    /// 🧪 PID Duplicate Investigation: verify transform produces EXACTLY N items for N input lines.
    /// Each item must contain EXACTLY one ES bulk operation (action\nsource = 2 lines).
    ///
    /// 🧠 TRIBAL KNOWLEDGE: geonames data has no ObjectID field, so the action line is
    /// `{"index":{}}` (no _id). ES auto-generates IDs. This test uses geonames-like data
    /// to match the exact benchmark scenario where 19.3M docs appeared from 11.4M lines.
    ///
    /// "The transform giveth action lines. The transform giveth source lines. The transform
    ///  giveth EXACTLY as many as it receiveth." — Book of Bulk, chapter 1 🦆
    #[test]
    fn the_one_where_transform_never_duplicates_geonames_docs() -> Result<()> {
        let the_sacred_doc_count = 100;
        // 📝 Build a page of geonames-like JSON (no ObjectID — matches real geonames data)
        let the_geonames_page: String = (0..the_sacred_doc_count)
            .map(|i| {
                serde_json::json!({
                    "geonameid": i,
                    "name": format!("Place {}", i),
                    "asciiname": format!("Place {}", i),
                    "latitude": 42.64991 + (i as f64 * 0.001),
                    "longitude": 1.53335 + (i as f64 * 0.001),
                    "country_code": "AD",
                    "population": 0
                })
                .to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");

        let the_items = RallyS3ToEs.transform(&the_geonames_page)?;

        // 🎯 Exactly N items for N input lines — no duplication, no loss
        assert_eq!(
            the_items.len(),
            the_sacred_doc_count,
            "🐛 Transform produced {} items from {} input lines. Duplication detected!",
            the_items.len(),
            the_sacred_doc_count
        );

        // 📏 Each item must be exactly 2 lines (action\nsource) — one ES bulk operation
        for (idx, item) in the_items.iter().enumerate() {
            let the_line_count = item.as_ref().split('\n').count();
            assert_eq!(
                the_line_count, 2,
                "🐛 Item {} has {} lines instead of 2 (action+source). Format violation!",
                idx, the_line_count
            );
            // ✅ Action line should be {"index":{}} — no _id because geonames has no ObjectID
            let the_action_line = item.as_ref().split('\n').next().unwrap();
            assert_eq!(
                the_action_line, r#"{"index":{}}"#,
                "🐛 Item {} action line should have no _id for geonames data",
                idx
            );
        }

        Ok(())
    }

    /// 🧪 Transform at scale: 10,000 geonames-like docs through the transform pipeline.
    /// If there's a subtle duplication bug, scale amplifies it.
    ///
    /// "One doc works. Ten docs work. Ten thousand docs... that's where the gremlins live."
    /// — The Art of War, DevOps edition 📜🦆
    #[test]
    fn the_one_where_ten_thousand_geonames_docs_transform_without_multiplication() -> Result<()> {
        let the_sacred_doc_count = 10_000;
        let the_geonames_page: String = (0..the_sacred_doc_count)
            .map(|i| serde_json::json!({"geonameid": i, "name": format!("P{}", i)}).to_string())
            .collect::<Vec<_>>()
            .join("\n");

        let the_items = RallyS3ToEs.transform(&the_geonames_page)?;

        assert_eq!(
            the_items.len(),
            the_sacred_doc_count,
            "🐛 10K transform: produced {} items from {} input lines",
            the_items.len(),
            the_sacred_doc_count
        );

        Ok(())
    }
}
