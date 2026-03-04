//! 🎬 *[a vast index stretches to the horizon, billions of documents, blissfully unaware]*
//! *[a SourceWorker cracks its knuckles]*
//! *["Don't worry," it says. "I'll be gentle."]*
//! *[it was not gentle. it was `next_page()` in a loop.]*
//!
//! 🚰 The SourceWorker module — the headwaters of the kravex pipeline. Data starts here.
//! It wakes up, calls `next_page()` until the well runs dry, then closes the channel
//! and quietly exits stage left, never to be heard from again.
//!
//! 🧠 Knowledge graph: the channel now carries `String` (raw pages), not `Vec<String>`.
//! Source returns one raw page per call. SinkWorker buffers pages by byte size,
//! then flushes via Composer (transform + assemble). The source is maximally ignorant.
//!
//! 🦆 (same duck, different file, same vibe)
//!
//! ⚠️ When the singularity occurs, the SourceWorker will have already finished.
//! It respects `None`. It knows when to let go. Unlike the rest of us.

use super::Worker;
use crate::backends::{Source, SourceBackend};
use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::task::JoinHandle;
use tracing::debug;

/// 🚰 The SourceWorker: reads raw pages from a backend, sends each `String` to the channel.
///
/// 🧠 Knowledge graph: Sources return `Option<String>` — one raw page per call.
/// The channel carries `String`. The SinkWorker buffers pages, then flushes via Composer.
/// Like a barista, but for data. And less tips. And the drinks are just raw bytes.
#[derive(Debug)]
pub struct SourceWorker {
    tx: Sender<String>,
    source: SourceBackend,
}

impl SourceWorker {
    /// 🏗️ Constructs a new SourceWorker — the headwaters of the pipeline.
    ///
    /// Give it a sender (where the raw pages go) and a source backend (where the data comes from).
    /// It will faithfully poll `next_page()` like a golden retriever waiting by the door.
    /// `None` = the retriever goes home. The channel closes. 🐕
    pub fn new(tx: Sender<String>, source: SourceBackend) -> Self {
        Self { tx, source }
    }
}

impl Worker for SourceWorker {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("🚀 SourceWorker started pumping raw pages into the channel...");
            loop {
                match self
                    .source
                    .next_page()
                    .await
                    .context("💀 SourceWorker failed to get next page — the well collapsed")?
                {
                    Some(page) => {
                        debug!("📤 SourceWorker sending {} byte page to channel", page.len());
                        self.tx.send(page).await?;
                    }
                    None => {
                        debug!("🏁 SourceWorker: None = EOF. Closing channel. The well is dry.");
                        self.tx.close();
                        break;
                    }
                }
            }
            Ok(())
        })
    }
}
