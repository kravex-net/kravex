// ai
//! 🎬 *[the items arrive. they are many. they need brackets. they need commas.]*
//! *[serde was not invited. it doesn't even know this function exists.]*
//! *["wrap me," said the items. "wrap me in valid JSON." we obliged.]*
//!
//! 📦 **JsonArrayManifold** — casts feeds and joins them into `[item1,item2,item3]` without serde.
//!
//! 🧠 Knowledge graph:
//! - Used by: InMemory sinks — tests want valid JSON arrays to assert against
//! - Zero serde on the framing: just `[`, commas, `]`, assembled by hand like artisans
//! - Items inside are already valid JSON strings from casters — we trust them
//! - Capacity math: 2 (brackets) + sum(item lengths) + (n-1) commas — exact, no vibes needed
//!
//! 🦆 The duck asked why we don't use serde. We said "trust the process." It nodded.

use super::Manifold;
use crate::casts::{DocumentCaster, Caster};
use anyhow::Result;

// -- ┌─────────────────────────────────────────────────────────┐
// -- │  JsonArrayManifold                                       │
// -- │  Struct → impl Manifold → tests                          │
// -- └─────────────────────────────────────────────────────────┘

/// 📦 JSON Array format — `[item1,item2,item3]` — for when you want valid JSON output.
///
/// Casts each feed, collects all results, wraps in `[...]` with commas.
/// Zero serde on the framing. Just brackets and commas, assembled by hand.
///
/// 🧠 Used for in-memory sinks where tests want valid JSON arrays to assert against.
/// The items inside are already valid JSON strings from the casters — we just
/// frame them as an array without re-parsing. Trust the casters. They did their job.
///
/// Conspiracy theory: the borrow checker is sentient, and it WANTS you to use serde.
/// We resist. We concatenate manually. We are free. 🐄
#[derive(Debug, Clone, Copy)]
pub struct JsonArrayManifold;

impl Manifold for JsonArrayManifold {
    #[inline]
    fn join(&self, feeds: &[String], caster: &DocumentCaster) -> Result<String> {
        // -- 📦 First, cast all feeds and collect results
        let mut all_items: Vec<String> = Vec::with_capacity(feeds.len());
        for feed in feeds {
            let cast_result = caster.cast(feed)?;
            all_items.push(cast_result);
        }

        // -- 🧮 Pre-allocate: brackets(2) + sum of items + commas(max n-1).
        // -- This is exact capacity — no growth, no realloc, no drama.
        // -- No cap this capacity math slaps fr fr 🎯
        let commas = if all_items.is_empty() {
            0
        } else {
            all_items.len() - 1
        };
        let estimated_size: usize =
            2 + all_items.iter().map(|s| s.len()).sum::<usize>() + commas;
        let mut payload = String::with_capacity(estimated_size);
        payload.push('[');
        for (i, item) in all_items.iter().enumerate() {
            if i > 0 {
                // -- 🔗 The comma: JSON's way of saying "and there's more where that came from."
                // -- Without this comma, the JSON validator weeps. With it, it beams with pride.
                payload.push(',');
            }
            payload.push_str(item);
        }
        payload.push(']');
        // -- ✅ Valid JSON array. No serde was harmed in the making of this string.
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::casts::passthrough::Passthrough;

    // -- 🔧 Helper: passthrough caster — casts by doing absolutely nothing. Inspirational.
    fn passthrough_caster() -> DocumentCaster {
        DocumentCaster::Passthrough(Passthrough)
    }

    #[test]
    fn json_array_the_one_where_feeds_become_an_array() -> Result<()> {
        // 🧪 Three feeds, each passthrough → [feed1,feed2,feed3]
        let manifold = JsonArrayManifold;
        let feeds = vec![
            String::from(r#"{"doc":1}"#),
            String::from(r#"{"doc":2}"#),
            String::from(r#"{"doc":3}"#),
        ];
        let result = manifold.join(&feeds, &passthrough_caster())?;
        assert_eq!(result, r#"[{"doc":1},{"doc":2},{"doc":3}]"#);
        Ok(())
    }

    #[test]
    fn json_array_the_one_where_empty_feeds_is_empty_array() -> Result<()> {
        // 🧪 No feeds → []. Still valid JSON. Still technically correct. The best kind of correct.
        let manifold = JsonArrayManifold;
        let result = manifold.join(&[], &passthrough_caster())?;
        assert_eq!(result, "[]");
        Ok(())
    }

    #[test]
    fn json_array_the_one_where_single_feed_has_no_commas() -> Result<()> {
        // 🧪 One feed, no commas. Like a party with one guest. Awkward but valid.
        let manifold = JsonArrayManifold;
        let feeds = vec![String::from(r#"{"lonely":true}"#)];
        let result = manifold.join(&feeds, &passthrough_caster())?;
        assert_eq!(result, r#"[{"lonely":true}]"#);
        Ok(())
    }
}
