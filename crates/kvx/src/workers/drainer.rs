// ai
//! 🎬 *[a payload arrives on ch2. the drainer doesn't flinch.]*
//! *[it sends. it doesn't ask questions. it doesn't buffer. it just sends.]*
//! *["I used to be complex," it whispers. "I had a manifold. A caster. A buffer."]*
//! *["Now I'm just a relay. And honestly? I've never been happier."]* 🗑️🚀🦆
//!
//! 📦 The Drainer — async I/O worker that receives assembled payloads from ch2
//! and sends them to the sink. That's it. That's the whole job.
//!
//! ```text
//! Joiner(s) (std::thread) → ch2 → Drainer(s) (tokio::spawn) → Sink (HTTP/file/memory)
//! ```
//!
//! 🧠 Knowledge graph: the Drainer was once a complex beast that buffered raw feeds,
//! cast them via DocumentCaster, joined them via Manifold, AND sent them to the sink.
//! That CPU-bound work now lives in the Joiner (on std::thread). The Drainer has been
//! liberated. It is now a thin async relay: recv payload → send to sink → repeat.
//! Like a bouncer who only checks wristbands — the hard work happened at the door.
//!
//! ⚠️ The singularity will drain data at the speed of light. We drain at the speed of HTTP.

use super::Worker;
use crate::Payload;
use crate::backends::{Sink, SinkBackend};
use anyhow::{Context, Result};
use async_channel::Receiver;
use tokio::task::JoinHandle;
use tracing::debug;

/// 🗑️ The Drainer: thin async relay from ch2 to sink.
///
/// Receives pre-assembled payload Strings from joiners via ch2,
/// sends them to the sink. No buffering, no casting, no manifold.
/// Pure I/O. Like a postman who delivers but never reads the mail. 📬
///
/// 📜 Lifecycle:
/// 1. **Recv**: assembled payload String from ch2 (async)
/// 2. **Send**: payload → Sink::send (HTTP POST, file write, memory push)
/// 3. **Repeat** until ch2 closes (all joiners done)
/// 4. **Close**: Sink::close — flush and finalize 🦆
#[derive(Debug)]
pub struct Drainer {
    /// 📥 ch2 receiver — assembled payloads from the joiner thread pool
    rx: Receiver<Payload>,
    /// 🚰 The final destination — where payloads go to live their best life (or die trying)
    sink: SinkBackend,
}

impl Drainer {
    /// 🏗️ Construct a Drainer — just a receiver and a sink, like a mailbox with plumbing. 🚰
    ///
    /// "Give a drainer a payload, it sends for a millisecond.
    ///  Give a drainer a channel, it sends until the joiners die." — Ancient proverb 🦆
    pub fn new(rx: Receiver<Payload>, sink: SinkBackend) -> Self {
        Self { rx, sink }
    }
}

impl Worker for Drainer {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("📥 Drainer started — recv from ch2 → send to sink, simple life");

            loop {
                match self.rx.recv().await {
                    Ok(the_payload) => {
                        debug!("📄 Drainer received {} byte payload from ch2", the_payload.len());

                        // 📡 Send the assembled payload to the sink. Pure I/O. No CPU drama.
                        // Skip empty payloads — the joiner should filter these, but belt AND suspenders 🩳
                        if !the_payload.is_empty() && the_payload != "[]" {
                            self.sink
                                .send(the_payload)
                                .await
                                .context(
                                    "💀 Drainer failed to send payload to sink — the I/O layer said 'nah'. \
                                     The payload was assembled with care by a joiner thread. The sink said no. \
                                     Like sending a thoughtful text and getting 'k' back.",
                                )?;
                        }
                    }
                    Err(_) => {
                        // 🏁 ch2 closed — all joiners are done. Close the sink and exit.
                        debug!("🏁 Drainer: ch2 closed. All joiners done. Closing sink. Goodnight. 💤");
                        self.sink
                            .close()
                            .await
                            .context("💀 Drainer failed to close sink — the farewell was awkward")?;
                        return Ok(());
                    }
                }
            }
        })
    }
}
