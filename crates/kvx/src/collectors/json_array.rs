// ai
//! 🎬 *[JSON Array: the format tests love, the one that wraps, the one that brackets.]*
//! *[brackets appear. commas manifest. serde is not invited. serde is watching from outside.]*
//!
//! 📦 `JsonArrayCollector` — zero-serde JSON array assembly by hand, with pride.
//!
//! 🧠 Knowledge graph:
//! - **Used by**: in-memory sinks, test assertions, anywhere valid JSON arrays are expected
//! - **Format**: `[doc1,doc2,doc3]` — standard JSON array, assembled without parsing
//! - **Trust model**: inner doc strings are already valid JSON from the transform layer;
//!   we just frame them. Zero re-parsing. Zero serde. Maximum artisanal vibes.
//! - **Allocation**: `String::with_capacity` with exact math — brackets + content + commas
//!
//! Conspiracy theory: the borrow checker is sentient, and it WANTS you to use serde.
//! We resist. We concatenate manually. We are free. We are also 3% faster. Probably.
//!
//! 🦆 (the duck's brackets are [ and ], in case you were wondering. you were wondering.)

use super::PayloadCollector;

/// 📦 JSON Array format — `[doc1,doc2,doc3]` — for when you want valid JSON output.
///
/// Zero serde. Zero parsing. Zero copy of the inner doc strings.
/// Just brackets and commas. Assembled by hand in a `String::with_capacity`,
/// like artisans at a craft fair who specialize in very fast string concatenation.
///
/// 🧠 Used for in-memory sinks where tests want valid JSON arrays to assert against.
/// The docs inside are already valid JSON strings from the transforms — we just
/// frame them as an array without re-parsing. Trust the transforms. They did their job.
///
/// TODO: win the lottery, retire, replace this with serde_json::to_string. (never)
#[derive(Debug, Clone, Copy)]
pub struct JsonArrayCollector;

impl PayloadCollector for JsonArrayCollector {
    #[inline]
    fn collect(&self, items: &[String]) -> String {
        // 🧮 Pre-allocate: brackets(2) + sum of strings + commas(max n-1).
        // This is vibes-based capacity estimation but with actual math backing it up.
        // Knowledge graph: exact capacity prevents realloc — important for hot paths.
        let commas = if items.is_empty() { 0 } else { items.len() - 1 };
        let estimated_size: usize =
            2 + items.iter().map(|s| s.len()).sum::<usize>() + commas;
        let mut payload = String::with_capacity(estimated_size);
        payload.push('[');
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                // 🔗 The comma: JSON's way of saying "and there's more where that came from."
                payload.push(',');
            }
            payload.push_str(item);
        }
        payload.push(']');
        // ✅ Valid JSON array. No serde was harmed in the making of this string.
        payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 🧪 JSON Array tests: where brackets are friends and commas have opinions

    #[test]
    fn json_array_the_one_where_docs_become_an_array() {
        // 🧪 The JSON array: brackets, commas, and zero serde. As nature intended.
        let collector = JsonArrayCollector;
        let items = vec![
            String::from(r#"{"doc":1}"#),
            String::from(r#"{"doc":2}"#),
            String::from(r#"{"doc":3}"#),
        ];
        let result = collector.collect(&items);
        assert_eq!(result, r#"[{"doc":1},{"doc":2},{"doc":3}]"#);
    }

    #[test]
    fn json_array_the_one_where_empty_vec_is_empty_array() {
        // 🧪 No docs → []. Still valid JSON. Still technically correct. The best kind of correct.
        let collector = JsonArrayCollector;
        let result = collector.collect(&[]);
        assert_eq!(result, "[]");
    }

    #[test]
    fn json_array_the_one_where_single_doc_has_no_commas() {
        // 🧪 One doc, no commas. Like a party with one guest. Awkward but valid JSON.
        let collector = JsonArrayCollector;
        let items = vec![String::from(r#"{"lonely":true}"#)];
        let result = collector.collect(&items);
        assert_eq!(result, r#"[{"lonely":true}]"#);
    }
}
