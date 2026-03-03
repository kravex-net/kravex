// ai
//! 🎬 *[a dark and stormy deploy. the sink demands newlines. the transformer obliges.]*
//! *[every line, alone. no brackets. no comfort. just `\n`. this is NDJSON.]*
//!
//! 📡 **NdjsonComposer** — transforms pages into newline-delimited JSON payloads.
//!
//! 🧠 Knowledge graph:
//! - Used by: ES `/_bulk` and file sinks — both want `item\nitem\n` format
//! - For ES bulk: transformer emits two lines per doc (action + source)
//! - Zero-copy path: passthrough transform borrows from the buffered page string
//! - Trailing `\n` is mandatory for ES bulk, appreciated by file sinks, ignored by nobody
//!
//! 🦆 The duck asked what NDJSON stands for. We told it. It left anyway.

use super::Composer;
use crate::transforms::{DocumentTransformer, Transform};
use anyhow::Result;

// -- ┌─────────────────────────────────────────────────────────┐
// -- │  NdjsonComposer                                         │
// -- │  Struct → impl Composer → tests                         │
// -- └─────────────────────────────────────────────────────────┘

/// 📡 Newline-Delimited JSON — the format ES `/_bulk` demands and files prefer.
///
/// Transforms each page, collects all items, joins with `\n`, trailing `\n`.
/// For ES bulk, each item is "action\nsource" (two NDJSON lines per doc).
/// After compose: "action1\nsource1\naction2\nsource2\n" — valid `/_bulk` payload.
///
/// For file passthrough: "doc1\ndoc2\n" — valid newline-delimited file content.
///
/// What's the DEAL with NDJSON? It's JSON but unfriendly. Every line is lonely.
/// No brackets to hold them. No commas to connect them. Just newlines. And silence.
/// Like my social life after deploying to production on a Friday. 🦆
#[derive(Debug, Clone, Copy)]
pub(crate) struct NdjsonComposer;

impl Composer for NdjsonComposer {
    #[inline]
    fn compose(&self, pages: &[String], transformer: &DocumentTransformer) -> Result<String> {
        // -- 🧮 Pre-allocate based on total page bytes — a vibes-based estimate that's usually close
        // -- Knowledge graph: +64 per page accounts for transform overhead (action lines in ES bulk)
        let estimated_size: usize = pages.iter().map(|p| p.len() + 64).sum();
        let mut payload = String::with_capacity(estimated_size);

        for page in pages {
            // -- 🔄 Transform this page → Vec<Cow<str>> items
            // -- Passthrough: Cow::Borrowed (zero alloc). ES bulk: Cow::Owned (action+source pairs).
            let items = transformer.transform(page)?;
            for item in &items {
                payload.push_str(item.as_ref());
                payload.push('\n');
            }
        }

        // -- ✅ Trailing \n included — ES bulk requires it, files appreciate it, nobody complains.
        // -- Ancient proverb: "He who omits the trailing newline, debugs at 3am."
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transforms::passthrough::Passthrough;

    // -- 🔧 Helper: build a passthrough transformer — the laziest transformer that ever lived
    fn passthrough_transformer() -> DocumentTransformer {
        DocumentTransformer::Passthrough(Passthrough)
    }

    #[test]
    fn ndjson_the_one_where_single_page_composes_to_ndjson() -> Result<()> {
        // 🧪 One page with content → content + trailing newline
        let composer = NdjsonComposer;
        let pages = vec![String::from(r#"{"doc":1}"#)];
        let result = composer.compose(&pages, &passthrough_transformer())?;
        assert_eq!(result, "{\"doc\":1}\n");
        Ok(())
    }

    #[test]
    fn ndjson_the_one_where_multiple_pages_compose() -> Result<()> {
        // 🧪 Two pages → two lines, each with trailing \n
        let composer = NdjsonComposer;
        let pages = vec![String::from(r#"{"doc":1}"#), String::from(r#"{"doc":2}"#)];
        let result = composer.compose(&pages, &passthrough_transformer())?;
        assert_eq!(result, "{\"doc\":1}\n{\"doc\":2}\n");
        Ok(())
    }

    #[test]
    fn ndjson_the_one_where_empty_pages_produces_nothing() -> Result<()> {
        // 🧪 No pages, no payload. The void stares back. It is empty. 🦆
        let composer = NdjsonComposer;
        let result = composer.compose(&[], &passthrough_transformer())?;
        assert!(result.is_empty(), "Empty input → empty output. Zen.");
        Ok(())
    }
}
