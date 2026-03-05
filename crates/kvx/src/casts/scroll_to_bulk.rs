use anyhow::Result;
use crate::casts::Caster;

// ai
// 🧠 This is for casting an ES _search { hits : [] } response into bulk NDJSON format.
// 📡 Extracts hits from scroll/PIT response and formats them as bulk index actions.
// -- "The scroll giveth, and the bulk taketh." — Ancient Elasticsearch proverb 🦆
#[derive(Debug, Clone, Copy)]
pub struct ScrollToBulk {
}

impl Caster for ScrollToBulk {
    #[inline]
    fn cast(&self, feed: &str) -> Result<String> {
        // -- 💀 TODO: actually extract hits from scroll response and bulk-ify them
        // -- For now this is a stub — like a movie trailer with no movie behind it 🦆
        Ok(String::new())
    }
}
