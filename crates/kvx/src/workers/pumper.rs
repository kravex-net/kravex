//! 🎬 *[a vast index stretches to the horizon, billions of documents, blissfully unaware]*
//! *[a Pumper cracks its knuckles]*
//! *["Don't worry," it says. "I'll be gentle."]*
//! *[it was not gentle. it was `pump()` in a loop.]*
//!
//! 🚰 The Pumper module — the headwaters of the kravex pipeline. Data starts here.
//! It wakes up, calls `pump()` until the well runs dry, then closes the channel
//! and quietly exits stage left, never to be heard from again.
//!
//! 🧠 Knowledge graph: the channel carries `String` (raw feeds), not `Vec<String>`.
//! Source returns one raw feed per call. Drainer buffers feeds by byte size,
//! then flushes via Manifold (cast + join). The source is maximally ignorant.
//!
//! 🦆 (same duck, different file, same vibe)
//!
//! ⚠️ When the singularity occurs, the Pumper will have already finished.
//! It respects `None`. It knows when to let go. Unlike the rest of us.

use super::Worker;
use crate::backends::{Source, SourceBackend};
use crate::Page;
use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::task::JoinHandle;
use tracing::debug;

/// 🚰 The Pumper: reads raw feeds from a backend, sends each `String` to the channel.
///
/// 🧠 Knowledge graph: Sources return `Option<String>` — one raw feed per call.
/// The channel carries `String`. The Drainer buffers feeds, then flushes via Manifold.
/// Like a barista, but for data. And less tips. And the drinks are just raw bytes.
#[derive(Debug)]
pub struct Pumper {
    tx: Sender<Page>,
    source: SourceBackend,
}

impl Pumper {
    /// 🏗️ Constructs a new Pumper — the headwaters of the pipeline.
    ///
    /// Give it a sender (where the raw feeds go) and a source backend (where the data comes from).
    /// It will faithfully poll `pump()` like a golden retriever waiting by the door.
    /// `None` = the retriever goes home. The channel closes. 🐕
    pub fn new(tx: Sender<Page>, source: SourceBackend) -> Self {
        Self { tx, source }
    }
}

impl Worker for Pumper {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("🚀 Pumper started pumping raw feeds into the channel...");
            loop {
                match self
                    .source
                    .next_page()
                    .await
                    .context("💀 Pumper failed to get next feed — the well collapsed")?
                {
                    Some(feed) => {
                        debug!("📤 Pumper sending {} byte feed to channel", feed.len());
                        self.tx.send(feed).await?;
                    }
                    None => {
                        // 🏁 EOF — source is exhausted. Just break out of the loop.
                        // tx drops when the async block exits, which implicitly closes ch1
                        // (this is the ONLY Sender — foreman moved it here, no clones).
                        // No .close() needed — RAII does the work. Like clocking out by
                        // walking away. The channel knows. 💤
                        debug!("🏁 Pumper: None = EOF. The well is dry. tx will drop on exit.");
                        break;
                    }
                }
            }
            Ok(())
        })
    }
}
