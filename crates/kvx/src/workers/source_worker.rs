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
//! - SourceWorker owns a `ControllerBackend` for adaptive batch sizing.
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
use crate::throttlers::{Controller, ControllerBackend};
use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
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
    /// 🛑 The escape hatch — when cancelled, close channel and stop pumping.
    /// Like a fire alarm, but for data. Everyone exits orderly. 🔥🦆
    cancellation_token: CancellationToken,
}

impl SourceWorker {
    /// 🏗️ Constructs a new SourceWorker — the headwaters of the pipeline.
    ///
    /// Give it a sender (where the raw pages go), a source backend (where the data comes from),
    /// a controller (how many docs to fetch per page), and a cancellation token (the kill switch).
    /// The controller is eager. The token is patient. Together, they manage throughput and graceful exits. 🎛️🛑
    pub(crate) fn new(
        tx: Sender<String>,
        source: SourceBackend,
        controller: ControllerBackend,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            tx,
            source,
            controller,
            cancellation_token,
        }
    }
}

impl Worker for SourceWorker {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!(
                "🚀 SourceWorker started — controller={:?}, pumping pages...",
                self.controller
            );
            loop {
                // 🎛️ Step 1: Ask the controller for the optimal batch size
                let the_batch_size_hint = self.controller.output();

                // 🛑 Step 2: Race pump vs cancellation — whoever wins, we're done (or continuing)
                // 🧠 tokio::select! picks the first future to complete. If cancellation wins,
                // we close the channel and break. The sink workers will drain remaining pages.
                tokio::select! {
                    pump_result = self.source.pump(the_batch_size_hint) => {
                        match pump_result
                            .context("💀 SourceWorker failed to pump page — the well collapsed")?
                        {
                            Some(page) => {
                                let the_page_size_bytes = page.len();
                                debug!(
                                    "📤 SourceWorker sending {} byte page to channel (batch_hint={})",
                                    the_page_size_bytes, the_batch_size_hint
                                );
                                // 📏 Step 3: Feed the measurement back to the controller BEFORE sending.
                                // 🧠 TRIBAL KNOWLEDGE: measure happens BEFORE send in SourceWorker, but
                                // AFTER send in SinkWorker. The asymmetry is intentional:
                                //   - SourceWorker measures *response size* (bytes received) → controller
                                //     adjusts doc count for the NEXT fetch. No timing needed, just size.
                                //   - SinkWorker measures *send duration* (ms elapsed) → PID controller
                                //     adjusts byte budget. Timing matters, so it measures AFTER the I/O.
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
                    _ = self.cancellation_token.cancelled() => {
                        debug!("🛑 SourceWorker: cancellation received. Closing channel. The fire alarm was pulled. 🔥");
                        self.tx.close();
                        break;
                    }
                }
            }
            Ok(())
        })
    }
}
