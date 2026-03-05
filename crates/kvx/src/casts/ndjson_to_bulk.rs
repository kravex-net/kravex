// ai
// 🧠 The lines of NDJSON are raw json docs — they have no bulk action metadata.
// 📡 This caster adds the ES bulk index action line before each doc.
use anyhow::Result;

use crate::casts::Caster;
const THE_BULK_ACTION_LINE: &str = "{\"index\":{}}\n";

/// 📡 Casts raw NDJSON docs into ES bulk format (action line + source doc).
/// Like a bouncer at a club — "you can't come in without your action line, buddy." 🦆
#[derive(Debug, Clone, Copy)]
pub struct NdJsonToBulk {}

impl Caster for NdJsonToBulk {
    #[inline]
    fn cast(&self, feed: &str) -> Result<String> {
        // 📄 Split feed by newlines, cast each non-empty line into bulk format.
        // 🧠 Each line becomes: action_line\n{json_document}
        // -- "He who casts without an action line, gets a 400 from Elasticsearch." 💀
        // TODO: actually implement the bulk action line generation
        // -- for now, pass through like a speed bump that forgot to bump 🦆
        let mut result = String::with_capacity(feed.len() + feed.len() / 2);
        for line in feed.split('\n') {
            if !line.is_empty() {
                result.push_str(THE_BULK_ACTION_LINE);
                result.push_str(line);
                result.push('\n');
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- 🧪 The test suite that finally validates the bulk body format.
    // -- The singularity will happen before we get 100% coverage, but we try anyway. 🦆

    /// 🧪 One doc in, one action+doc pair out. The simplest heist in town.
    #[test]
    fn the_one_where_a_single_doc_becomes_a_valid_bulk_pair() -> Result<()> {
        // 🔧 Assemble — a lonely JSON doc, seeking its action line soulmate
        let caster = NdJsonToBulk {};
        let the_lone_doc = r#"{"ObjectID":42,"Name":"The answer to everything"}"#;

        // 🚀 Act — cast it into the bulk dimension
        let the_bulk_body = caster.cast(the_lone_doc)?;

        // 🎯 Assert — must be exactly: action_line\ndoc\n
        let expected = format!("{}{}\n", THE_BULK_ACTION_LINE, the_lone_doc);
        assert_eq!(
            the_bulk_body, expected,
            "💀 Single doc bulk body didn't match. The bouncer let in the wrong person."
        );

        // ✅ Verify line count: action line + doc line = 2 lines
        let line_count = the_bulk_body.lines().count();
        assert_eq!(line_count, 2, "💀 Expected 2 lines (action + doc), got {line_count}");

        Ok(())
    }

    /// 🧪 Multiple docs — each gets its own action line escort. Like a VIP list.
    #[test]
    fn the_one_where_multiple_docs_each_get_their_own_action_line() -> Result<()> {
        let caster = NdJsonToBulk {};
        // 📄 Three docs walk into a bulk endpoint...
        let doc_a = r#"{"id":1,"name":"Alpha"}"#;
        let doc_b = r#"{"id":2,"name":"Bravo"}"#;
        let doc_c = r#"{"id":3,"name":"Charlie"}"#;
        let the_ndjson_feed = format!("{doc_a}\n{doc_b}\n{doc_c}");

        let the_bulk_body = caster.cast(&the_ndjson_feed)?;

        // 🎯 Should produce 3 action+doc pairs = 6 lines
        let lines: Vec<&str> = the_bulk_body.lines().collect();
        assert_eq!(lines.len(), 6, "💀 Expected 6 lines for 3 docs, got {}", lines.len());

        // ✅ Verify the interleaving pattern: action, doc, action, doc, action, doc
        let the_action_line = r#"{"index":{}}"#;
        for i in (0..lines.len()).step_by(2) {
            assert_eq!(
                lines[i], the_action_line,
                "💀 Line {i} should be the action line, got: {}", lines[i]
            );
        }
        assert_eq!(lines[1], doc_a, "💀 Line 1 should be doc_a");
        assert_eq!(lines[3], doc_b, "💀 Line 3 should be doc_b");
        assert_eq!(lines[5], doc_c, "💀 Line 5 should be doc_c");

        Ok(())
    }

    /// 🧪 Empty input — the void returns void. Zen mode.
    #[test]
    fn the_one_where_emptiness_begets_emptiness() -> Result<()> {
        let caster = NdJsonToBulk {};
        let the_void = "";

        let the_bulk_body = caster.cast(the_void)?;

        // 🎯 Empty in, empty out — no phantom action lines
        assert!(
            the_bulk_body.is_empty(),
            "💀 Empty input should produce empty output, but got: '{the_bulk_body}'"
        );

        Ok(())
    }

    /// 🧪 Trailing newline — the sneaky empty string at the end shouldn't spawn a ghost action line.
    #[test]
    fn the_one_where_trailing_newlines_dont_spawn_ghost_actions() -> Result<()> {
        let caster = NdJsonToBulk {};
        let doc = r#"{"id":1,"confession":"I added a trailing newline on purpose"}"#;
        // 📄 Note the trailing \n — split will produce an empty last element
        let the_feed_with_trailing_newline = format!("{doc}\n");

        let the_bulk_body = caster.cast(&the_feed_with_trailing_newline)?;

        // 🎯 Should still be exactly 1 action+doc pair, no ghost at the end
        let lines: Vec<&str> = the_bulk_body.lines().collect();
        assert_eq!(
            lines.len(), 2,
            "💀 Trailing newline created ghost lines. Expected 2, got {}", lines.len()
        );

        Ok(())
    }

    /// 🧪 Blank lines scattered through the feed — the caster ignores them like I ignore my IDE warnings.
    #[test]
    fn the_one_where_blank_lines_are_ghosted_harder_than_my_last_tinder_match() -> Result<()> {
        let caster = NdJsonToBulk {};
        let doc_a = r#"{"id":1}"#;
        let doc_b = r#"{"id":2}"#;
        // 📄 Feed with empty lines everywhere — chaos mode
        let the_chaotic_feed = format!("\n\n{doc_a}\n\n\n{doc_b}\n\n");

        let the_bulk_body = caster.cast(&the_chaotic_feed)?;

        // 🎯 Only 2 real docs = 4 lines total (2 action + 2 doc)
        let lines: Vec<&str> = the_bulk_body.lines().collect();
        assert_eq!(
            lines.len(), 4,
            "💀 Blank lines leaked through! Expected 4 lines, got {}", lines.len()
        );

        Ok(())
    }

    /// 🧪 The ultimate validation — output is a valid ES _bulk body where every doc line is parseable JSON.
    #[test]
    fn the_one_where_the_output_is_actually_valid_bulk_api_format() -> Result<()> {
        let caster = NdJsonToBulk {};
        // 📄 Real-ish documents, like the ones that haunt my dreams at 3am
        let docs = vec![
            r#"{"ObjectID":99999,"FormattedID":"US001","Name":"The hero's journey"}"#,
            r#"{"ObjectID":88888,"FormattedID":"DE001","Name":"The bug that got away"}"#,
            r#"{"ObjectID":77777,"FormattedID":"TA001","Name":"The task that never ends"}"#,
        ];
        let the_feed = docs.join("\n");

        let the_bulk_body = caster.cast(&the_feed)?;

        // 🎯 Parse every line as JSON — both action lines and doc lines must be valid
        let lines: Vec<&str> = the_bulk_body.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).map_err(|e| {
                anyhow::anyhow!("💀 Line {i} is not valid JSON: '{line}' — error: {e}")
            })?;

            if i % 2 == 0 {
                // 📡 Action lines must have {"index":{}}
                assert!(
                    parsed.get("index").is_some(),
                    "💀 Action line {i} missing 'index' key: {line}"
                );
            } else {
                // 📄 Doc lines must have real content
                assert!(
                    parsed.is_object(),
                    "💀 Doc line {i} is not a JSON object: {line}"
                );
            }
        }

        // ✅ Bulk body must end with \n (ES requires trailing newline)
        assert!(
            the_bulk_body.ends_with('\n'),
            "💀 Bulk body must end with newline — ES will reject this faster than a bad PR"
        );

        Ok(())
    }
}
