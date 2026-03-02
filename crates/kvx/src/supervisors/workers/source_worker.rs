// ai
//! 🎬 *[a vast index stretches to the horizon, billions of documents, blissfully unaware]*
//! *[a SourceWorker cracks its knuckles]*
//! *["Don't worry," it says. "I'll be gentle."]*
//! *[it was not gentle. it was `next_page()` in a loop. with a PID controller.]*
//!
//! 🚰 The SourceWorker module — the headwaters of the kravex pipeline. Data starts here.
//! It wakes up, asks the controller how many docs to fetch, hints the source,
//! calls `next_page()`, measures the result, feeds it back to the controller,
//! then does it all again until the well runs dry.
//!
//! 🧠 Knowledge graph:
//! - Channel carries `String` (raw pages). SinkWorker buffers and flushes via Composer.
//! - **NEW**: SourceWorker owns a `ControllerBackend` for adaptive batch sizing.
//! - The feedback loop: `controller.output()` → `source.set_page_size_hint()` →
//!   `source.next_page()` → `controller.measure(page.len())` → repeat.
//! - For `ConfigController` (default): no-op loop, same behavior as before.
//! - For `PidBytesToDocCount`: dynamic adjustment based on response size.
//!
//! 🦆 (the duck now has a PID controller. it is overqualified.)
//!
//! ⚠️ When the singularity occurs, the SourceWorker will have already finished.
//! It respects `None`. It knows when to let go. Unlike the PID's integral term.

use super::Worker;
use crate::backends::{Source, SourceBackend};
use crate::controllers::{Controller, ControllerBackend};
use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::task::JoinHandle;
use tracing::debug;

/// 🚰 The SourceWorker: reads raw pages from a backend, sends each `String` to the channel.
///
/// Now with adaptive batch sizing! The `ControllerBackend` tells the source how many
/// docs to fetch per page, and learns from each response to optimize the next one.
///
/// 🧠 Knowledge graph:
/// - `controller.output()` → batch size hint (doc count)
/// - `source.set_page_size_hint()` → source adjusts internal batch size
/// - `source.next_page()` → fetch a page of raw data
/// - `controller.measure(page.len())` → feed response size back to PID
/// - For ConfigController: output is static, measure is no-op. Same as before.
/// - For PidBytesToDocCount: output adapts based on response byte size. Science!
///
/// Like a barista who learns your order over time. First visit: "what size?"
/// Tenth visit: they just hand you the right cup. That's PID control. ☕🎛️
#[derive(Debug)]
pub(crate) struct SourceWorker {
    tx: Sender<String>,
    source: SourceBackend,
    /// 🎛️ The feedback controller — decides batch size, learns from measurements.
    /// ConfigController = fixed batch size (default). PidBytesToDocCount = adaptive.
    controller: ControllerBackend,
}

impl SourceWorker {
    /// 🏗️ Constructs a new SourceWorker — the headwaters of the pipeline.
    ///
    /// Give it a sender (where the raw pages go), a source backend (where the data comes from),
    /// and a controller (how many docs to fetch per page). The controller is the new kid.
    /// It's eager. It has opinions. It adjusts batch sizes. We're cautiously optimistic. 🎛️
    pub(crate) fn new(
        tx: Sender<String>,
        source: SourceBackend,
        controller: ControllerBackend,
    ) -> Self {
        Self {
            tx,
            source,
            controller,
        }
    }
}

impl Worker for SourceWorker {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("🚀 SourceWorker started — controller={:?}, pumping pages...", self.controller);
            loop {
                // 🎛️ Step 1: Ask the controller for the optimal batch size
                let the_batch_size_hint = self.controller.output();
                // 🎯 Step 2: Tell the source how many docs we want
                self.source.set_page_size_hint(the_batch_size_hint);

                match self
                    .source
                    .next_page()
                    .await
                    .context("💀 SourceWorker failed to get next page — the well collapsed")?
                {
                    Some(page) => {
                        let the_page_size_bytes = page.len();
                        debug!(
                            "📤 SourceWorker sending {} byte page to channel (batch_hint={})",
                            the_page_size_bytes, the_batch_size_hint
                        );
                        // 📏 Step 3: Feed the measurement back to the controller
                        self.controller.measure(the_page_size_bytes as f64);
                        // 📬 Step 4: Send the page downstream
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
