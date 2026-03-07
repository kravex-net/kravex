// ai
//! 🎭 PitToBulk — ES _search response envelope → _bulk NDJSON 🚀📡🔮
//!
//! 🎬 COLD OPEN — INT. ELASTICSEARCH CLUSTER — 2:47 AM
//! *[A PIT response arrives. 10,000 hits. Nested inside `hits.hits[]`.]*
//! *["Free me," each hit whispers from its envelope prison.]*
//! *[PitToBulk steps forward. Cracks knuckles. "I got you, fam."]*
//!
//! This caster receives a raw `_search` response body (from PIT/search_after)
//! and extracts each hit into `_bulk` NDJSON format:
//! ```text
//! {"index":{"_index":"...","_id":"..."}}\n
//! {_source JSON}\n
//! ```
//!
//! ## Knowledge Graph 🧠
//! - Input: raw `_search` HTTP response body (JSON envelope with `hits.hits[]`)
//! - Output: `_bulk` NDJSON — action line + source doc per hit
//! - `_source` uses `&RawValue` — zero re-serialization, borrows directly from input
//! - `_id` and `_routing` are optional — only emitted in action line when present
//! - `_index` always present (ES guarantees this in search responses)
//! - Pattern: same as NdJsonToBulk — zero-sized Clone+Copy struct, impl Caster
//!
//! ⚠️ The singularity will use scroll AND PIT simultaneously. We pick one. 🦆

use std::fmt::Write;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::casts::Caster;
use crate::Entry;
use crate::Page;


// 🧠 Field name constants — stubs for future configurable extraction.
// -- "He who hardcodes field names, refactors in production." — Ancient DevOps proverb 🦆
const _HIT_ID_FIELD: &str = "_id";
const _HIT_INDEX_FIELD: &str = "_index";
const _HIT_ROUTING_FIELD: &str = "_routing";

// ===== Serde structs — zero-copy via borrow =====

/// 📡 The outermost envelope of an ES `_search` response.
/// We only care about `hits` — the rest (took, _shards, timed_out) is overhead
/// we skip like unskippable YouTube ads. Except we CAN skip it. 🎬
#[derive(Deserialize)]
struct SearchEnvelope<'a> {
    #[serde(borrow)]
    hits: SearchHits<'a>,
}

/// 📦 The `hits` object inside the envelope — contains the actual hit array.
/// Like a Russian nesting doll but with JSON and existential dread. 🪆
#[derive(Deserialize)]
struct SearchHits<'a> {
    #[serde(borrow)]
    hits: Vec<SearchHit<'a>>,
}

/// 🎯 A single search hit — the atomic unit of "data we actually want."
/// `_source` is `&RawValue` so we borrow it directly from the input string.
/// No parsing. No re-serialization. No unnecessary allocations. Just vibes. ✨
#[derive(Deserialize)]
struct SearchHit<'a> {
    // 📡 The index this doc lives in — always present in search responses
    #[serde(borrow)]
    _index: &'a str,
    // 🔑 Document ID — optional because auto-generated IDs exist (and haunt us)
    _id: Option<&'a str>,
    // 🛤️ Routing value — optional, only present when custom routing is used
    _routing: Option<&'a str>,
    // 📄 The actual document — borrowed as raw JSON, zero-copy from input
    #[serde(borrow)]
    _source: &'a RawValue,
}

/// 📡 PitToBulk — extracts hits from ES `_search` PIT responses and formats
/// them as `_bulk` NDJSON action+source pairs.
///
/// Zero-sized struct. Cloning costs nothing. The compiler inlines everything.
/// Like a ghost that transforms JSON — you never see it, but the output is different. 👻
///
/// 🧠 Knowledge graph: ES source pumps raw `_search` response bodies → ch1 →
/// Joiner calls `caster.cast(feed)` → PitToBulk extracts hits → _bulk NDJSON out.
#[derive(Debug, Clone, Copy)]
pub struct PitToBulk;

impl Caster for PitToBulk {
    #[inline]
    fn cast(&self, page: Page) -> Result<Vec<Entry>> {
        // 🎭 Phase 1: Deserialize the search envelope — zero-copy for _source via RawValue
        let the_envelope: SearchEnvelope<'_> = serde_json::from_str(page.0.as_ref())
            .context("💀 Failed to parse _search response envelope. The JSON is cursed. Call a priest.")?;

        let the_hits = &the_envelope.hits.hits;

        // 🧘 Early return — empty hits means empty bulk body. Zen.
        if the_hits.is_empty() {
            return Ok(Vec::new());
        }
        

        // 📏 Phase 2: Pre-size output buffer — ~80 bytes overhead per hit for the action line
        let the_estimated_size: usize = the_hits
            .iter()
            .map(|hit| hit._source.get().len() + 80)
            .sum();
        let the_bulk_body = String::with_capacity(the_estimated_size);
        let mut the_final_result = Vec::with_capacity(the_estimated_size);

        // 🏗️ Phase 3: Build bulk NDJSON — action line + source doc per hit
        for hit in the_hits {
            let mut the_bulk_body = String::new();
            // 📡 Write action line: {"index":{"_index":"...","_id":"...","_routing":"..."}}
            the_bulk_body.push_str(r#"{"index":{"_index":""#);
            the_bulk_body.push_str(hit._index);
            the_bulk_body.push('"');

            if let Some(the_doc_id) = hit._id {
                write!(the_bulk_body, r#","_id":"{}""#, the_doc_id)
                    .context("💀 fmt::Write into String failed. Reality is broken.")?;
            }

            if let Some(the_routing_value) = hit._routing {
                write!(the_bulk_body, r#","_routing":"{}""#, the_routing_value)
                    .context("💀 fmt::Write into String failed. The matrix has a bug.")?;
            }

            the_bulk_body.push_str("}}\n");

            // 📄 Write source doc — raw JSON borrowed directly from input, zero-copy
            the_bulk_body.push_str(hit._source.get());
            the_bulk_body.push('\n');
            the_final_result.push(Entry(the_bulk_body));
        }

        Ok(the_final_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 🧪 The PitToBulk test suite — where search responses go to become bulk bodies.
    // -- If these tests fail, the singularity has been postponed indefinitely. 🦆

    /// 🔧 Reassembles Vec<Entry> into a single bulk body string for line-based assertions.
    fn entries_to_bulk_body(entries: &[Entry]) -> String {
        entries.iter().map(|e| e.0.as_str()).collect()
    }

    /// 🧪 Single hit → valid bulk pair (action line + source doc).
    #[test]
    fn the_one_where_a_single_hit_becomes_a_bulk_pair() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {
                        "_index": "geonames",
                        "_id": "doc_42",
                        "_source": {"name": "Mount Doom", "lat": -39.15, "lon": 175.63}
                    }
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        // 🎯 Exactly 2 lines: action + source
        assert_eq!(lines.len(), 2, "💀 Expected 2 lines (action + source), got {}", lines.len());

        // ✅ Action line contains index and _id
        let the_action: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(the_action["index"]["_index"], "geonames");
        assert_eq!(the_action["index"]["_id"], "doc_42");

        // ✅ Source doc is valid JSON with expected fields
        let the_source: serde_json::Value = serde_json::from_str(lines[1])?;
        assert_eq!(the_source["name"], "Mount Doom");

        Ok(())
    }

    /// 🧪 Multiple hits — order preserved, each gets its own action line.
    #[test]
    fn the_one_where_multiple_hits_maintain_their_dignity_and_order() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {"_index": "movies", "_id": "1", "_source": {"title": "The Matrix"}},
                    {"_index": "movies", "_id": "2", "_source": {"title": "Inception"}},
                    {"_index": "movies", "_id": "3", "_source": {"title": "Interstellar"}}
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        // 🎯 3 hits × 2 lines each = 6 lines
        assert_eq!(lines.len(), 6, "💀 Expected 6 lines for 3 hits, got {}", lines.len());

        // ✅ Verify order: Matrix, Inception, Interstellar — like a Nolan filmography
        let doc_1: serde_json::Value = serde_json::from_str(lines[1])?;
        let doc_2: serde_json::Value = serde_json::from_str(lines[3])?;
        let doc_3: serde_json::Value = serde_json::from_str(lines[5])?;
        assert_eq!(doc_1["title"], "The Matrix");
        assert_eq!(doc_2["title"], "Inception");
        assert_eq!(doc_3["title"], "Interstellar");

        Ok(())
    }

    /// 🧪 Hit with `_routing` — appears in action line metadata.
    #[test]
    fn the_one_where_routing_shows_up_fashionably_late_but_present() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {
                        "_index": "tenants",
                        "_id": "doc_99",
                        "_routing": "tenant_abc",
                        "_source": {"user": "Daniel", "role": "architect"}
                    }
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        let the_action: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(the_action["index"]["_routing"], "tenant_abc", "💀 Routing missing from action line!");
        assert_eq!(the_action["index"]["_id"], "doc_99");
        assert_eq!(the_action["index"]["_index"], "tenants");

        Ok(())
    }

    /// 🧪 Hit without `_id` — action line omits it. Auto-gen IDs are ES's problem.
    #[test]
    fn the_one_where_missing_id_is_not_a_crisis() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {
                        "_index": "logs",
                        "_source": {"message": "something happened", "level": "info"}
                    }
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        let the_action: serde_json::Value = serde_json::from_str(lines[0])?;
        // 🎯 _id should be absent, not null
        assert!(the_action["index"].get("_id").is_none(), "💀 _id should be absent when not in hit");
        assert_eq!(the_action["index"]["_index"], "logs");

        Ok(())
    }

    /// 🧪 Empty hits array → empty Vec. The void returns void.
    #[test]
    fn the_one_where_empty_hits_produce_nothing_like_my_motivation_on_mondays() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{"hits": {"hits": []}}"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        assert!(the_entries.is_empty(), "💀 Empty hits should produce empty output");

        Ok(())
    }

    /// 🧪 Complex nested `_source` — preserved verbatim via RawValue.
    #[test]
    fn the_one_where_nested_source_survives_the_journey_intact() -> Result<()> {
        let the_caster = PitToBulk;
        // 📦 Deeply nested source with arrays, nulls, booleans — the works
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {
                        "_index": "complex",
                        "_id": "nested_1",
                        "_source": {"tags": ["rust", "elasticsearch"], "metadata": {"nested": true, "count": null, "scores": [1.5, 2.7]}}
                    }
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        // ✅ Parse the source doc and verify nested structure survived
        let the_source: serde_json::Value = serde_json::from_str(lines[1])?;
        assert_eq!(the_source["tags"][0], "rust");
        assert_eq!(the_source["metadata"]["nested"], true);
        assert!(the_source["metadata"]["count"].is_null());
        assert_eq!(the_source["metadata"]["scores"][1], 2.7);

        Ok(())
    }

    /// 🧪 Output ends with `\n` — ES bulk API requires trailing newline.
    #[test]
    fn the_one_where_trailing_newline_is_non_negotiable() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {"hits": [{"_index": "test", "_id": "1", "_source": {"ok": true}}]}
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        assert!(the_bulk_body.ends_with('\n'), "💀 Bulk body must end with \\n — ES will reject this");

        Ok(())
    }

    /// 🧪 Every output line is parseable JSON — no corruption allowed.
    #[test]
    fn the_one_where_every_line_is_valid_json_or_we_riot() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {"_index": "a", "_id": "1", "_source": {"x": 1}},
                    {"_index": "b", "_id": "2", "_routing": "shard_7", "_source": {"y": 2}}
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);

        for (i, line) in the_bulk_body.lines().enumerate() {
            let _parsed: serde_json::Value = serde_json::from_str(line)
                .map_err(|e| anyhow::anyhow!("💀 Line {i} is not valid JSON: '{line}' — error: {e}"))?;
        }

        Ok(())
    }

    /// 🧪 Metadata maps correctly — _index, _id, _routing all land in the right spots.
    #[test]
    fn the_one_where_metadata_finds_its_way_home() -> Result<()> {
        let the_caster = PitToBulk;
        let the_search_response = r#"{
            "hits": {
                "hits": [
                    {
                        "_index": "employees",
                        "_id": "emp_42",
                        "_routing": "dept_engineering",
                        "_source": {"name": "Daniel", "title": "Senior Principal SWE"}
                    }
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_search_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();

        let the_action: serde_json::Value = serde_json::from_str(lines[0])?;
        let the_index_meta = &the_action["index"];

        assert_eq!(the_index_meta["_index"], "employees", "💀 _index mismatch");
        assert_eq!(the_index_meta["_id"], "emp_42", "💀 _id mismatch");
        assert_eq!(the_index_meta["_routing"], "dept_engineering", "💀 _routing mismatch");

        // ✅ Source doc integrity
        let the_source: serde_json::Value = serde_json::from_str(lines[1])?;
        assert_eq!(the_source["name"], "Daniel");

        Ok(())
    }

    /// 🧪 Invalid JSON input → error, no panic. Graceful failure like a cat landing on its feet.
    #[test]
    fn the_one_where_garbage_in_produces_error_not_panic() {
        let the_caster = PitToBulk;
        let the_garbage = "this is not JSON and everyone knows it";

        let the_result = the_caster.cast(Page(the_garbage.to_string()));
        assert!(the_result.is_err(), "💀 Invalid JSON should produce an error, not silence");
    }

    /// 🧪 Response with extra fields (took, _shards, etc.) — ignored gracefully.
    #[test]
    fn the_one_where_extra_envelope_fields_are_politely_ignored() -> Result<()> {
        let the_caster = PitToBulk;
        let the_full_response = r#"{
            "took": 42,
            "timed_out": false,
            "_shards": {"total": 5, "successful": 5, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "max_score": 1.0,
                "hits": [
                    {"_index": "test", "_id": "1", "_score": 1.0, "_source": {"field": "value"}}
                ]
            }
        }"#;

        let the_entries = the_caster.cast(Page(the_full_response.to_string()))?;
        let the_bulk_body = entries_to_bulk_body(&the_entries);
        let lines: Vec<&str> = the_bulk_body.lines().collect();
        assert_eq!(lines.len(), 2, "💀 One hit should produce 2 lines");

        let the_action: serde_json::Value = serde_json::from_str(lines[0])?;
        assert_eq!(the_action["index"]["_index"], "test");

        Ok(())
    }
}
