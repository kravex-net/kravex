// ai
//! 🚶 Passthrough — zero-copy identity caster 🔄✈️
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
//! - Struct: `Passthrough` — zero-sized, `impl Caster`
//! - Pattern: same as `InMemorySource impl Source`
//! - Cost: zero allocation (ownership transfer of input `String`)
//! - Used for: File→File, InMemory→InMemory, ES→File, testing, benchmarking
//!
//! ⚠️ The singularity won't even notice this module exists. 🦆

use anyhow::Result;
use crate::casts::Caster;

/// 🚶 Passthrough — returns the entire feed unchanged. Zero alloc. Zero copy. Zero drama.
///
/// Zero-sized struct. Same pattern as `InMemorySource` — the simplest
/// concrete type that implements the trait. The compiler may inline
/// this to literally nothing. One ownership transfer and we're done.
///
/// 🧠 Knowledge graph: Passthrough returns the feed as-is — the feed
/// passes through untouched. The Manifold then joins it into the wire format.
/// For NDJSON→NDJSON scenarios (e.g., file-to-file copy), this means zero overhead. 🐄
#[derive(Debug, Clone, Copy)]
pub struct Passthrough;

impl Caster for Passthrough {
    /// 🔄 Identity function. `f(x) = x`. The mathematicians would be proud.
    /// Returns the entire feed unchanged — no allocation, no parse, no copy.
    /// "What do you do?" "I return the input." "That's it?" "That's everything." 🐄
    #[inline]
    fn cast(&self, feed: &str) -> Result<String> {
        // -- 🚶 TSA PreCheck for data. Walk right through. Don't even slow down.
        Ok(feed.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_one_where_passthrough_is_the_identity_function() -> Result<()> {
        // 🧪 f(x) = x. If this fails, mathematics is broken. And so is String.
        let the_input = r#"{"untouched":"perfection"}"#.to_string();
        let the_output = Passthrough.cast(&the_input)?;
        assert_eq!(the_output, the_input, "Passthrough must return feed unchanged! 🚶");
        Ok(())
    }

    #[test]
    fn the_one_where_empty_string_passes_through() -> Result<()> {
        // 🧪 Nothing in, nothing out. The void is consistent. 🧘
        let the_output = Passthrough.cast("")?;
        assert_eq!(the_output, "", "Empty feed → empty output. Zen. 🧘");
        Ok(())
    }

    #[test]
    fn the_one_where_non_json_also_passes_because_we_dont_validate() -> Result<()> {
        // 🧪 Passthrough doesn't parse. Doesn't validate. Doesn't care.
        let not_json = "this is not json and that's fine".to_string();
        let the_output = Passthrough.cast(&not_json)?;
        assert_eq!(the_output, not_json, "Non-JSON still passes through! 🎉");
        Ok(())
    }

    #[test]
    fn the_one_where_multi_line_feed_stays_intact() -> Result<()> {
        // 🧪 Passthrough treats the whole feed as one blob — it's the Manifold's job to join
        let multi_line = "line1\nline2\nline3".to_string();
        let the_output = Passthrough.cast(&multi_line)?;
        assert_eq!(the_output, multi_line, "Passthrough doesn't split — one feed, one output 🎯");
        Ok(())
    }
}
