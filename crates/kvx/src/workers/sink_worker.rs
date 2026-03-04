// ai
//! 🎬 *[a channel fills with raw pages. somewhere, a sink waits.]*
//! *[the clock on the wall reads 2:47am.]*
//! *[nobody asked for this data migration. and yet, here we are.]*
//!
//! 🗑️ The SinkWorker module — now with page buffering and Composer powers!
//!
//! It receives raw pages from the channel, buffers them by byte size,
//! then flushes via `Composer.compose(buffer, transformer)` which transforms
//! each page and assembles the items into the sink's wire format.
//!
//! 🧠 Knowledge graph: SinkWorker is the bridge between raw source pages and
//! the sink's I/O abstraction. The Composer handles both transformation AND assembly:
//! - **Composer**: iterates buffered pages → calls transformer per page → assembles wire format
//! - **Sink**: pure I/O (HTTP POST, file write, memory push)
//!
//! ```text
//!   channel(String) → SinkWorker buffers Vec<String> → composer.compose(&buffer, &transformer) → Sink::send
//! ```
//!
//! 🐄 The Cow lives here (spiritually). The Composer calls `transformer.transform(page)` which
//! returns `Vec<Cow<str>>` — borrowed for passthrough, owned for format conversion. Zero-copy
//! when source format == sink format. The dream. The whole point. The Cow. 🐄
//!
//! 🦆 (the duck has been promoted to buffer management. it is overwhelmed but coping.)
//!
//! ⚠️ When the singularity occurs, the SinkWorker will still be buffering pages.
//! It will not notice. It does not notice things. It only buffers, composes, and sinks.

use super::Worker;
use crate::backends::{Sink, SinkBackend};
use crate::composers::{Composer, ComposerBackend};
use crate::transforms::DocumentTransformer;
use anyhow::{Context, Result};
use async_channel::Receiver;
use tokio::task::JoinHandle;
use tracing::debug;

/// 🧮 Epsilon buffer — headroom to avoid going over the max request size.
/// 64 KiB of breathing room because payloads expand during transformation
/// (ES bulk adds action lines, etc.) and we'd rather flush one page early
/// than send a 💀 413 Request Entity Too Large to the sink.
///
/// "He who buffers without epsilon, 413s in production." — Ancient HTTP proverb 📡
const BUFFER_EPSILON_BYTES: usize = 64 * 1024; // -- 64 KiB of safety net for the tightrope walk

/// 🗑️ The SinkWorker: receives raw pages, buffers them by byte size, composes via
/// Composer (transform + assemble), and sends the payload to the sink.
///
/// 🧠 Holds its own `DocumentTransformer` and `ComposerBackend` — each SinkWorker
/// gets clones. Since both are zero-sized structs under the hood, cloning is free.
/// The compiler inlines everything. Branch prediction handles the enum matches.
///
/// 📜 The lifecycle:
/// 1. **Receive**: raw page String from channel
/// 2. **Buffer**: accumulate pages until byte size threshold approached
/// 3. **Flush**: `composer.compose(&buffer, &transformer)` → payload String
/// 4. **Send**: payload → Sink (HTTP POST, file write, memory push)
/// 5. **Repeat** until channel closes, then flush remaining buffer
#[derive(Debug)]
pub struct SinkWorker {
    rx: Receiver<String>,
    sink: SinkBackend,
    /// 🔄 Per-page format conversion — resolves from (SourceConfig, SinkConfig).
    transformer: DocumentTransformer,
    /// 🎼 Payload assembly — resolves from SinkConfig. Transforms + assembles in one shot.
    composer: ComposerBackend,
    /// 📏 Max request size from sink config — flush when buffer approaches this.
    max_request_size_bytes: usize,
}

impl SinkWorker {
    /// 🏗️ Constructs a new SinkWorker with receiver, sink, transformer, composer, and size limit.
    ///
    /// The transformer decides HOW to format each doc (Rally→ES bulk, passthrough)
    /// The composer decides HOW to assemble them (NDJSON newlines, JSON array brackets)
    /// The sink decides WHERE to send it (HTTP POST, file write, memory push)
    /// The worker decides WHEN — "when the buffer is full enough, no cap." 🦆
    pub fn new(
        rx: Receiver<String>,
        sink: SinkBackend,
        transformer: DocumentTransformer,
        composer: ComposerBackend,
        max_request_size_bytes: usize,
    ) -> Self {
        Self {
            rx,
            sink,
            transformer,
            composer,
            max_request_size_bytes,
        }
    }
}

impl Worker for SinkWorker {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("📥 SinkWorker started — buffer → compose → sink, let's go");
            // 📦 The page buffer — accumulates raw pages until byte threshold approached
            let mut buffer: Vec<String> = Vec::new();
            let mut buffer_bytes: usize = 0;

            loop {
                let receive_result = self.rx.recv().await;
                match receive_result {
                    Ok(page) => {
                        debug!("📄 SinkWorker received {} byte page from channel", page.len());

                        // 📏 Accumulate page into the buffer
                        buffer_bytes += page.len();
                        buffer.push(page);

                        // 🧮 Flush if buffer + epsilon approaches max request size.
                        // The epsilon accounts for transformation overhead (action lines, etc.)
                        if buffer_bytes + BUFFER_EPSILON_BYTES >= self.max_request_size_bytes {
                            debug!(
                                "🚿 SinkWorker flushing {} pages ({} bytes) — approaching max request size",
                                buffer.len(),
                                buffer_bytes
                            );
                            flush_buffer(
                                &mut buffer,
                                &mut buffer_bytes,
                                &self.composer,
                                &self.transformer,
                                &mut self.sink,
                            )
                            .await?;
                        }
                    }
                    Err(_) => {
                        // 🏁 Channel closed — flush remaining buffer, then close sink
                        if !buffer.is_empty() {
                            debug!(
                                "🚿 SinkWorker final flush: {} pages ({} bytes) — channel closed, sending last payload",
                                buffer.len(),
                                buffer_bytes
                            );
                            flush_buffer(
                                &mut buffer,
                                &mut buffer_bytes,
                                &self.composer,
                                &self.transformer,
                                &mut self.sink,
                            )
                            .await?;
                        }
                        debug!("🏁 SinkWorker: Channel closed. Closing sink. Goodnight. 💤");
                        self.sink
                            .close()
                            .await
                            .context("💀 SinkWorker failed to close sink — the farewell was awkward")?;
                        return Ok(());
                    }
                }
            }
        })
    }
}

/// 🚿 Flush the page buffer: compose → send → clear.
///
/// Extracted as a function because the SinkWorker flushes from two places:
/// 1. When the buffer is full enough (byte threshold)
/// 2. When the channel closes (final flush)
///
/// "He who duplicates flush logic, debugs it in two places at 3am." — Ancient proverb 💀
async fn flush_buffer(
    buffer: &mut Vec<String>,
    buffer_bytes: &mut usize,
    composer: &ComposerBackend,
    transformer: &DocumentTransformer,
    sink: &mut SinkBackend,
) -> Result<()> {
    // 🎼 Compose: transform each page → collect items → assemble wire-format payload
    let payload = composer.compose(buffer, transformer).context(
        "💀 SinkWorker compose failed — the pages went in and chaos came out. \
         Check the transform logic and the source data quality. \
         Or blame the Cow. The Cow is always suspicious. 🐄",
    )?;

    // 📡 Send the fully rendered payload to the sink. Pure I/O.
    if !payload.is_empty() && payload != "[]" {
        sink.send(payload).await.context(
            "💀 SinkWorker failed to send payload to sink — the I/O layer rejected our offering. \
             The payload was composed with care. The sink said no. Like my prom date.",
        )?;
    }

    // 🧹 Reset buffer state
    buffer.clear();
    *buffer_bytes = 0;

    Ok(())
}
