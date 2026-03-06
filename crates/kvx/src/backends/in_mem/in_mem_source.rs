use anyhow::Result;
use async_trait::async_trait;

use crate::Page;
use crate::backends::Source;

/// 📦 The world's most optimistic data source — now feed-aware! 📄
///
/// `InMemorySource` is the "Hello, World!" of [`Source`] implementations.
/// It knows exactly four documents. They are `{"doc":1}` through `{"doc":4}`.
/// Returns them as a single newline-delimited feed, because sources are
/// maximally ignorant now — they don't split docs, they just pour raw feeds. 🚰
///
/// 🎯 Designed entirely for testing. Not for feelings. Feelings are unindexed.
///
/// 🧠 Knowledge graph: Source returns `Option<String>` (raw feed), not `Vec<String>` (parsed docs).
/// The Manifold downstream handles splitting + casting via the Caster.
/// This enables zero-copy passthrough when formats match. The source doesn't care. It shouldn't.
#[derive(Debug, Default)]
pub struct InMemorySource {
    /// 🔒 The virginity of this source — once yielded, forever yielded.
    /// Like watching a movie spoiler. Can't un-yield it.
    /// The borrow checker wished it could reject this concept. It could not.
    has_yielded: bool, // -- true = "I already gave you everything I had, please stop asking"
}

impl InMemorySource {
    /// 🚀 Constructs a new `InMemorySource` ready to disappoint exactly once.
    ///
    /// No I/O. No config. No environment variables lurking in the shadows.
    /// You call `new()`, you get a fresh source, hat tips are exchanged.
    /// It's async because we respect the trait contract, not because we need it.
    /// Ancient proverb: "He who makes everything async learns nothing, but ships faster."
    pub async fn new() -> Result<Self> {
        // -- ✅ No config to load, no server to ping, no prayers to send.
        // -- This is the most peaceful constructor in the entire codebase.
        // -- Cherish this moment.
        Ok(Self { has_yielded: false })
    }
}

#[async_trait]
impl Source for InMemorySource {
    /// 📄 Returns the one and only feed this source will ever produce.
    ///
    /// Call it once: you get the goods as a newline-delimited feed.
    /// Call it again: `None`. Go home. The snack cabinet is empty. 🍪
    ///
    /// ⚠️ What's the DEAL with `has_yielded`? It's a boolean. A single boolean.
    /// This is the entire state machine. One field. One decision. One life.
    /// Seinfeld would have a bit about this and honestly he'd be right.
    ///
    /// 🧠 Knowledge graph: the source joins its 4 docs with `\n` into one raw feed.
    /// The Manifold+Caster downstream will split and process them.
    /// Source is ignorant. Source is bliss. Source is a faucet. 🚰
    async fn next_page(&mut self) -> Result<Option<Page>> {
        if self.has_yielded {
            return Ok(None);
        }

        self.has_yielded = true;

        // 📦 The sacred test corpus. Four docs, joined with newlines into one raw feed.
        // 🧠 Sources return raw feeds now — the Manifold handles doc splitting.
        // "I don't always return data, but when I do, it's newline-delimited." — This source, probably.
        let the_sacred_page = [
            r#"{"doc":1}"#,
            r#"{"doc":2}"#,
            r#"{"doc":3}"#,
            r#"{"doc":4}"#,
        ]
        .join("\n");

        Ok(Some(Page(the_sacred_page)))
    }
}
