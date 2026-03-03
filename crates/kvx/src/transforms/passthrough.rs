// ai
//! 🚶 Passthrough — zero-copy identity transform 🔄✈️
//!
//! 🎬 COLD OPEN — INT. TSA PRECHECK — 6:00 AM — YOU DON'T EVEN SLOW DOWN
//!
//! Everyone else: shoes off, laptop out, dignity abandoned.
//! You: walk through. Don't stop. Don't unpack. Already at the gate.
//!
//! Same pattern as `InMemorySource` in `backends/in_mem.rs` — the simplest
//! possible implementation of the trait. Exists for testing, file-to-file
//! copies, and proving that not everything needs to be complicated.
//!
//! ## Knowledge Graph 🧠
//! - Struct: `Passthrough` — zero-sized, `impl Transform`
//! - Pattern: same as `InMemorySource impl Source`
//! - Cost: zero allocation (ownership transfer of input `String`)
//! - Used for: File→File, InMemory→InMemory, ES→File, testing, benchmarking
//!
//! ⚠️ The singularity won't even notice this module exists. 🦆

use super::Transform;
use anyhow::Result;
use std::borrow::Cow;

/// 🚶 Passthrough — returns the entire page as a borrowed Cow. Zero alloc. Zero copy. Zero drama.
///
/// Zero-sized struct. Same pattern as `InMemorySource` — the simplest
/// concrete type that implements the trait. The compiler may inline
/// this to literally nothing. One `Cow::Borrowed` and we're done.
///
/// 🧠 Knowledge graph: Passthrough returns `vec![Cow::Borrowed(raw_source_page)]` — the page
/// passes through as-is. The Composer then assembles it into the wire format. For NDJSON→NDJSON
/// scenarios (e.g., file-to-file copy), this means true zero-copy from source buffer to sink payload.
/// The borrow checker isn't just satisfied — it's *proud*. 🐄
#[derive(Debug, Clone, Copy)]
pub(crate) struct Passthrough;

impl Transform for Passthrough {
    /// 🔄 Identity function. `f(x) = [&x]`. The mathematicians would be proud.
    /// Returns the entire page as one borrowed Cow item — no allocation, no parse, no copy.
    /// "What do you do?" "I borrow the input." "That's it?" "That's everything." 🐄
    #[inline]
    fn transform<'a>(&self, raw_source_page: &'a str) -> Result<Vec<Cow<'a, str>>> {
        // -- 🚶 TSA PreCheck for data. Walk right through. Don't even slow down.
        Ok(vec![Cow::Borrowed(raw_source_page)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_one_where_passthrough_is_the_identity_function() -> Result<()> {
        // 🧪 f(x) = [&x]. If this fails, mathematics is broken. And so is Cow.
        let the_input = r#"{"untouched":"perfection"}"#;
        let the_items = Passthrough.transform(the_input)?;
        assert_eq!(the_items.len(), 1);
        assert_eq!(the_items[0].as_ref(), the_input);
        // 🐄 The WHOLE POINT: verify it's actually borrowed, not cloned
        assert!(matches!(the_items[0], Cow::Borrowed(_)));
        Ok(())
    }

    #[test]
    fn the_one_where_empty_string_passes_through() -> Result<()> {
        let the_items = Passthrough.transform("")?;
        assert_eq!(the_items.len(), 1);
        assert_eq!(the_items[0].as_ref(), "");
        Ok(())
    }

    #[test]
    fn the_one_where_non_json_also_passes_because_we_dont_validate() -> Result<()> {
        // 🧪 Passthrough doesn't parse. Doesn't validate. Doesn't care. Doesn't allocate.
        let not_json = "this is not json and that's fine";
        let the_items = Passthrough.transform(not_json)?;
        assert_eq!(the_items[0].as_ref(), not_json);
        assert!(
            matches!(the_items[0], Cow::Borrowed(_)),
            "Still borrowed! Still free!"
        );
        Ok(())
    }

    #[test]
    fn the_one_where_multi_line_page_stays_as_one_item() -> Result<()> {
        // 🧪 Passthrough treats the whole page as one item — it's the Composer's job to split
        let multi_line = "line1\nline2\nline3";
        let the_items = Passthrough.transform(multi_line)?;
        assert_eq!(
            the_items.len(),
            1,
            "Passthrough doesn't split — one page, one item"
        );
        assert_eq!(the_items[0].as_ref(), multi_line);
        Ok(())
    }
}
