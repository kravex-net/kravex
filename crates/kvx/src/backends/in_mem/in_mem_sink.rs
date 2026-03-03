// ai
//! 📦🔒🧪 InMemorySink — the Vec that remembers everything. Unlike Kevin.
//!
//! 🎬 COLD OPEN — INT. THE HEAP — AN ARC<MUTEX<VEC>> AWAITS ITS DESTINY
//!
//! *The Vec was empty. Fresh. Full of promise. "One day," it whispered to the Mutex,
//! "I will hold payloads." The Mutex said nothing. The Mutex never says anything.
//! It just locks. And unlocks. And locks again. A simple life. An honest life.*
//!
//! *The Arc watched from above, counting references. Always counting.* 🦆
use anyhow::Result;
use async_trait::async_trait;

use crate::backends::Sink;
/// 📦 A sink that never forgets. Unlike my dad, who forgot my soccer game in 1998.
///
/// `InMemorySink` receives fully rendered payload strings and hoards them in a shared Vec
/// wrapped in a Mutex wrapped in an Arc. It's types all the way down.
///
/// 🔒 The `Arc<Mutex<Vec<String>>>` is an existential nesting doll:
/// "I need to share ownership of a thing that must be accessed one thread at a
/// time and that thing is a list of payloads." Simpler than before — no Hit structs,
/// no HitBatch wrappers, just raw payload strings the SinkWorker already rendered.
///
/// 🧠 Knowledge graph: Sinks are I/O-only now. The SinkWorker does transform + binary collect.
/// This sink just stores the final payload strings for test assertions.
///
/// Clone-able because tests need to peek inside after handing `self` off to the
/// pipeline. The `Arc` means everyone shares the same Vec. Communist data, but
/// in a good way. The borrow checker approved. Barely. It had notes.
#[derive(Debug, Default, Clone)]
pub(crate) struct InMemorySink {
    /// 🔒 The vault. The evidence locker. Each entry = one fully rendered payload string.
    /// Arc so multiple owners can hold a reference. Mutex so only one panics at a time.
    pub(crate) received: std::sync::Arc<tokio::sync::Mutex<Vec<String>>>,
}

impl InMemorySink {
    /// 🚀 Spins up a brand new sink, ready to absorb batches like a paper towel
    /// in a infomercial — except this one actually works and isn't $19.99 plus S&H.
    ///
    /// Conspiracy theory: `tokio::sync::Mutex` is just `std::sync::Mutex` wearing
    /// a trench coat to look taller in async contexts. I have no proof. I have
    /// strong feelings.
    pub(crate) async fn new() -> Result<Self> {
        // -- ✅ Birth of the sink. An empty Vec, full of potential, unmarred by batches.
        // -- This is the most hopeful a Vec will ever be. Downhill from here.
        Ok(Self {
            received: std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }
}

#[async_trait]
impl Sink for InMemorySink {
    /// 📡 Stores a fully rendered payload. Lock, push, done. Like a fax machine but for bytes. 🦆
    ///
    /// 🎯 I/O-only: the SinkWorker already transformed and binary-collected the payload.
    /// We just stash it for test assertions. No parsing. No judgment. Just storage.
    async fn drain(&mut self, payload: String) -> Result<()> {
        // 🔒 The Mutex is load-bearing. Do not remove. I know it looks optional. It isn't.
        self.received.lock().await.push(payload);
        Ok(())
    }

    /// 🗑️ Closes the sink with all the ceremony of closing a browser tab.
    ///
    /// There is nothing to clean up. We live in RAM. When this drops, the OS
    /// reclaims everything faster than HR reclaims your badge on your last day.
    /// We don't hold file handles. We don't hold sockets. We hold batches and
    /// vibes, and the vibes are ref-counted.
    ///
    /// Dad joke mandatory by AGENTS.md section 4, paragraph "comedy density":
    /// Why did the in-memory sink go to therapy? It had trouble letting go.
    /// (The Arc kept bumping the ref count. It never actually dropped.)
    async fn close(&mut self) -> Result<()> {
        // -- 🗑️ Cleanup routine: [REDACTED — there is nothing here]
        // -- The singularity will have already occurred by the time we need real
        // -- teardown logic in an in-memory backend. We'll deal with it then.
        // -- The singularity can file a PR.
        Ok(())
    }
}
