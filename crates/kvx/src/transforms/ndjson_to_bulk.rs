// ai
//! 📦 NdjsonToBulk — zero-parse bulk wrapper for raw NDJSON 🚀⚡🦆
//!
//! 🎬 COLD OPEN — INT. BENCHMARK LAB — THE PROFILER SHOWS THE TRUTH
//!
//! The team stared at the flamegraph. serde_json::from_str was eating 37% of
//! allocations on data that didn't need parsing. Geonames has no Rally metadata.
//! No ObjectID. No fields to strip. Yet every doc was getting the full
//! parse-strip-reserialize spa treatment. Like sending a clean shirt to the
//! dry cleaner "just in case."
//!
//! "What if we just... didn't parse it?" someone whispered.
//! The borrow checker nodded. The allocator wept with joy.
//! And NdjsonToBulk was born.
//!
//! ## Knowledge Graph 🧠
//! - Purpose: wrap raw NDJSON lines in ES/OS bulk format without JSON parsing
//! - Input: raw NDJSON page (one doc per line)
//! - Output: `{"index":{}}\n<raw_line>` pairs — zero parse, zero serialize
//! - Use when: source data is already valid JSON and needs no field manipulation
//! - Contrast with RallyS3ToEs: that parses every doc to strip metadata fields
//! - The bulk action line is always `{"index":{}}` — no _id routing (ES auto-generates)
//! - If you need _id extraction or field stripping, use RallyS3ToEs instead
//!
//! ## When to use which transform
//! ```text
//!   Need to strip fields or extract _id?  → RallyS3ToEs
//!   Raw NDJSON, no transform needed?       → NdjsonToBulk
//!   Source format == sink format?           → Passthrough
//!   ES/OS search hits → bulk?              → EsHitToBulk
//! ```

use super::Transform;
use crate::buffer_pool::PoolBuffer;
use anyhow::Result;
use std::borrow::Cow;

/// 📦 The bulk action line — pre-baked, static, immutable, eternal.
/// 🧠 No _id means ES auto-generates UUIDs. Fast. Simple. No parse needed.
/// "He who avoids parsing, allocates not in vain." — Ancient zero-copy proverb 🐄
const THE_BULK_ACTION_LINE: &str = "{\"index\":{}}";

/// 📦 NdjsonToBulk — wraps each NDJSON line in ES bulk format without touching the JSON.
///
/// Zero-sized struct. Clone + Copy. The compiler inlines this to basically a memcpy
/// with `{"index":{}}\n` sprinkled between lines. No serde. No allocator drama.
/// No existential crisis about whether ObjectID is a Number or String.
///
/// 🧠 Knowledge graph: this is the "fair fight" transform for benchmarking against
/// naive Python implementations that do json.loads + json.dumps. Except we don't
/// even do that — we just prepend a static string. Rust superiority isn't about
/// doing the same work faster, it's about not doing unnecessary work at all.
///
/// 🦆 The duck approves of transforms that transform nothing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NdjsonToBulk;

impl Transform for NdjsonToBulk {
    /// 🔄 Transform a page of raw NDJSON into bulk action pairs.
    ///
    /// Each line becomes: `Cow::Owned("{\"index\":{}}\n<raw_line>")`
    /// No JSON parsing. No field inspection. Just string surgery.
    ///
    /// "What's the DEAL with parsing JSON you're not going to modify?
    /// It's like reading the terms of service — nobody does it,
    /// and the ones who do are wasting their time." — Seinfeld, probably
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        // -- 🚀 Pre-allocate: count newlines to estimate doc count
        let the_doc_count_guess = raw_source_page.bytes().filter(|&b| b == b'\n').count() + 1;
        let mut the_bulk_pairs = Vec::with_capacity(the_doc_count_guess);

        for line in raw_source_page.split('\n') {
            if line.is_empty() {
                continue;
            }
            // -- 📦 Build "{\"index\":{}}\n<raw_line>" — one allocation per doc
            let mut the_bulk_pair = String::with_capacity(THE_BULK_ACTION_LINE.len() + 1 + line.len());
            the_bulk_pair.push_str(THE_BULK_ACTION_LINE);
            the_bulk_pair.push('\n');
            the_bulk_pair.push_str(line);
            the_bulk_pairs.push(Cow::Owned(the_bulk_pair));
        }

        Ok(the_bulk_pairs)
    }

    /// 🚀 Streaming override — writes bulk pairs directly into the output buffer.
    /// Skips the intermediate Vec<Cow> entirely. Pure push_str into pre-allocated String.
    ///
    /// 🧠 Knowledge graph: this is the hot path. NdjsonComposer calls this instead of
    /// transform() + iterate. Each doc = 2 push_str + 2 push('\n'). No Vec. No Cow.
    /// No allocator. Just vibes and memcpy. The way Ritchie intended. 🦆
    fn transform_into_ndjson(&self, raw_source_page: &str, output: &mut String) -> Result<()> {
        for line in raw_source_page.split('\n') {
            if line.is_empty() {
                continue;
            }
            // -- ⚡ Action line + newline + source line + newline — four push ops, zero alloc
            output.push_str(THE_BULK_ACTION_LINE);
            output.push('\n');
            output.push_str(line);
            output.push('\n');
        }
        Ok(())
    }

    /// 🏦📦 Pool buffer override — same logic as transform_into_ndjson but writes into PoolBuffer.
    /// Four push ops per doc, zero intermediate allocation, zero String intermediary.
    /// The bytes enter the pool and never leave until the sink says so. Like a data roach motel. 🪳
    fn transform_into_pool_buffer(&self, source: &PoolBuffer, output: &mut PoolBuffer) -> Result<()> {
        let source_str = source.as_str().map_err(|e| {
            anyhow::anyhow!("💀 Source buffer not valid UTF-8 in NdjsonToBulk. The bytes have betrayed us. {e}")
        })?;
        for line in source_str.split('\n') {
            if line.is_empty() {
                continue;
            }
            output.push_str(THE_BULK_ACTION_LINE);
            output.push_byte(b'\n');
            output.push_str(line);
            output.push_byte(b'\n');
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 Single doc gets wrapped in bulk format without parsing.
    /// The JSON could be garbage and we wouldn't know. That's the point.
    #[test]
    fn the_one_where_a_single_doc_gets_bulk_wrapped_without_judgment() {
        let the_input = r#"{"name":"Earth","population":8000000000}"#;
        let the_result = NdjsonToBulk.transform(the_input).unwrap();

        assert_eq!(the_result.len(), 1, "🎯 One doc in, one bulk pair out");
        let the_pair = the_result[0].as_ref();
        let the_lines: Vec<&str> = the_pair.split('\n').collect();
        assert_eq!(the_lines[0], r#"{"index":{}}"#, "🎯 Action line is correct");
        assert_eq!(the_lines[1], the_input, "🎯 Source line is untouched — zero parse");
    }

    /// 🧪 Multi-line page produces one bulk pair per line.
    #[test]
    fn the_one_where_a_multi_doc_page_becomes_multiple_bulk_pairs() {
        let the_input = "{\"a\":1}\n{\"b\":2}\n{\"c\":3}";
        let the_result = NdjsonToBulk.transform(the_input).unwrap();

        assert_eq!(the_result.len(), 3, "🎯 Three lines, three pairs");
        for (i, pair) in the_result.iter().enumerate() {
            assert!(
                pair.starts_with(r#"{"index":{}}"#),
                "🎯 Pair {} starts with action line",
                i
            );
        }
    }

    /// 🧪 Empty lines are skipped — no ghost bulk pairs.
    #[test]
    fn the_one_where_empty_lines_dont_create_phantom_docs() {
        let the_input = "{\"a\":1}\n\n{\"b\":2}\n\n";
        let the_result = NdjsonToBulk.transform(the_input).unwrap();

        assert_eq!(the_result.len(), 2, "🎯 Only real lines produce pairs");
    }

    /// 🧪 Empty input produces empty output — the null migration.
    #[test]
    fn the_one_where_nothing_in_nothing_out() {
        let the_result = NdjsonToBulk.transform("").unwrap();
        assert!(the_result.is_empty(), "🎯 Empty in, empty out, existential peace");
    }

    /// 🧪 Streaming path produces identical output to transform() path.
    #[test]
    fn the_one_where_streaming_and_batch_agree_on_reality() {
        let the_input = "{\"x\":1}\n{\"y\":2}\n{\"z\":3}";

        // -- 🔄 Batch path
        let the_batch_items = NdjsonToBulk.transform(the_input).unwrap();
        let mut the_batch_output = String::new();
        for item in &the_batch_items {
            the_batch_output.push_str(item.as_ref());
            the_batch_output.push('\n');
        }

        // -- 🚀 Streaming path
        let mut the_stream_output = String::new();
        NdjsonToBulk
            .transform_into_ndjson(the_input, &mut the_stream_output)
            .unwrap();

        assert_eq!(
            the_batch_output, the_stream_output,
            "🎯 Both paths must produce byte-identical output — the universe demands consistency"
        );
    }

    /// 🧪 Invalid JSON passes through without complaint. We don't parse. We don't judge.
    /// "He who validates at the transform layer, validates twice." — Ancient proverb
    #[test]
    fn the_one_where_invalid_json_passes_through_because_yolo() {
        let the_garbage = "this is not json at all lmao";
        let the_result = NdjsonToBulk.transform(the_garbage).unwrap();

        assert_eq!(the_result.len(), 1);
        assert!(the_result[0].as_ref().contains(the_garbage));
    }
}
