// ai
//! 🎬 *[a channel fills with composed payloads. somewhere, a sink waits.]*
//! *[the clock on the wall reads 2:47am.]*
//! *[nobody asked for this data migration. and yet, here we are.]*
//!
//! 🗑️ The SinkWorker module — pure I/O, no CPU work, just vibes.
//!
//! It receives pre-composed payloads from Channel2 (TransformWorkers already
//! did the CPU-bound transform + compose work), buffers them by byte size,
//! concatenates into a single payload, and drains to the sink.
//!
//! 🧠 Knowledge graph: SinkWorker is the last mile between composed data and the
//! sink's I/O abstraction. Transform responsibility moved to TransformWorker.
//! - **TransformWorker** (upstream): CPU-bound transform + compose → composed PoolBuffer
//! - **SinkWorker** (this): buffer → concatenate → drain. That's it. That's the job.
//! - **Sink**: pure I/O (HTTP POST, file write, memory push)
//!
//! ```text
//!   Channel2(composed PoolBuffer) → SinkWorker buffers Vec<PoolBuffer> → concatenate → Sink::drain
//! ```
//!
//! 🦆 (the duck has been demoted from CPU work. it now just pushes bytes to HTTP. it is relieved.)
//!
//! ⚠️ When the singularity occurs, the SinkWorker will still be buffering payloads.
//! It will not notice. It does not notice things. It only buffers, concatenates, and drains.

use super::Worker;
use crate::backends::{Sink, SinkBackend};
use crate::buffer_pool::{self, PoolBuffer};
use crate::throttlers::{ThrottleController, ThrottleControllerBackend};
use anyhow::{Context, Result};
use async_channel::Receiver;
use std::time::Instant;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// 🧮 Epsilon buffer — headroom to avoid going over the max request size.
/// 64 KiB of breathing room because payloads expand during transformation
/// (ES bulk adds action lines, etc.) and we'd rather flush one page early
/// than send a 💀 413 Request Entity Too Large to the sink.
///
/// "He who buffers without epsilon, 413s in production." — Ancient HTTP proverb 📡
const BUFFER_EPSILON_BYTES: usize = 64 * 1024; // -- 64 KiB of safety net for the tightrope walk

/// 🗑️ The SinkWorker: receives pre-composed payloads, buffers them by byte size,
/// concatenates into a single payload, and drains to the sink. Pure I/O, zero CPU work.
///
/// 🧠 Transform + compose responsibility moved to TransformWorker (upstream).
/// SinkWorker is now the simplest worker in the pipeline — buffer, concat, drain, repeat.
///
/// 📜 The lifecycle:
/// 1. **Receive**: pre-composed PoolBuffer payload from Channel2
/// 2. **Buffer**: accumulate payloads until byte size threshold approached
/// 3. **Concatenate**: assemble buffered payloads into a single PoolBuffer (the "second copy")
/// 4. **Drain**: payload → Sink (HTTP POST, file write, memory push)
/// 5. **Repeat** until channel closes, then flush remaining buffer
#[derive(Debug)]
pub(crate) struct SinkWorker {
    rx: Receiver<PoolBuffer>,
    sink: SinkBackend,
    /// 🧠 Throttle controller — decides the dynamic max request size.
    /// Static: fixed bytes (the classic). PID: adapts based on measured latency (the secret sauce). 🔒
    /// Each worker owns its controller exclusively — no shared state, no Mutex, no couples therapy.
    throttle_controller: ThrottleControllerBackend,
    /// 🛑 The escape hatch — when cancelled, flush remaining buffer and close the sink.
    /// Like last call at a bar: finish your drink, grab your coat, exit with dignity. 🍻🦆
    cancellation_token: CancellationToken,
}

impl SinkWorker {
    /// 🏗️ Constructs a new SinkWorker — the last mile of the pipeline.
    ///
    /// The sink decides WHERE to send it (HTTP POST, file write, memory push)
    /// The controller decides HOW MUCH — "this many bytes, based on science (or stubbornness)" 🧠
    /// The worker decides WHEN — "when the buffer is full enough, no cap."
    /// The token decides IF — "keep going or gracefully exit." 🛑🦆
    pub(crate) fn new(
        rx: Receiver<PoolBuffer>,
        sink: SinkBackend,
        throttle_controller: ThrottleControllerBackend,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            rx,
            sink,
            throttle_controller,
            cancellation_token,
        }
    }
}

impl Worker for SinkWorker {
    fn start(mut self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            debug!("📥 SinkWorker started — buffer → concat → drain → measure → adapt. No CPU work, just vibes.");
            // 📦 The payload buffer — accumulates pre-composed PoolBuffers until byte threshold
            // 🧠 These are already transformed+composed by TransformWorker upstream. We just concat and ship.
            let mut buffer: Vec<PoolBuffer> = Vec::new();
            let mut buffer_bytes: usize = 0;

            loop {
                // 🧠 Read the current target from the throttle controller.
                // For Static: always the same. For PID: may change after each flush cycle.
                let current_max_bytes = self.throttle_controller.output();

                // 🛑 Race recv vs cancellation — graceful shutdown or keep buffering
                tokio::select! {
                    receive_result = self.rx.recv() => {
                        match receive_result {
                            Ok(payload) => {
                                debug!("📄 SinkWorker received {} byte composed payload from channel", payload.len());

                                // 📏 Accumulate payload into the buffer
                                buffer_bytes += payload.len();
                                buffer.push(payload);

                                // 🧮 Flush if buffer + epsilon approaches dynamic max request size.
                                // Epsilon is still relevant: multiple composed payloads concatenated
                                // may overshoot the target slightly. Safety net stays. 🦺
                                if buffer_bytes + BUFFER_EPSILON_BYTES >= current_max_bytes {
                                    debug!(
                                        "🚿 SinkWorker flushing {} payloads ({} bytes) — approaching max request size ({} bytes, via {:?})",
                                        buffer.len(),
                                        buffer_bytes,
                                        current_max_bytes,
                                        self.throttle_controller,
                                    );
                                    flush_and_measure(
                                        &mut buffer,
                                        &mut buffer_bytes,
                                        &mut self.sink,
                                        &mut self.throttle_controller,
                                    )
                                    .await?;
                                }
                            }
                            Err(_) => {
                                // 🏁 Channel closed — flush remaining buffer, then close sink
                                if !buffer.is_empty() {
                                    debug!(
                                        "🚿 SinkWorker final flush: {} payloads ({} bytes) — channel closed, sending last batch",
                                        buffer.len(),
                                        buffer_bytes
                                    );
                                    flush_and_measure(
                                        &mut buffer,
                                        &mut buffer_bytes,
                                        &mut self.sink,
                                        &mut self.throttle_controller,
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
                    _ = self.cancellation_token.cancelled() => {
                        // 🛑 Cancellation received — flush what we have, close the sink, exit stage left
                        debug!("🛑 SinkWorker: cancellation received. Flushing remaining buffer and closing sink.");
                        if !buffer.is_empty() {
                            debug!(
                                "🚿 SinkWorker cancellation flush: {} payloads ({} bytes)",
                                buffer.len(), buffer_bytes
                            );
                            flush_and_measure(
                                &mut buffer,
                                &mut buffer_bytes,
                                &mut self.sink,
                                &mut self.throttle_controller,
                            )
                            .await?;
                        }
                        self.sink
                            .close()
                            .await
                            .context("💀 SinkWorker failed to close sink during cancellation — awkward even in emergencies")?;
                        return Ok(());
                    }
                }
            }
        })
    }
}

/// 🚿 Flush the payload buffer: concatenate → drain → measure → adapt.
///
/// The full feedback loop in one function:
/// 1. Concatenate buffered pre-composed payloads into a single PoolBuffer (the "second copy")
/// 2. Time the sink.drain() call — this is the measured process variable
/// 3. Feed the duration to the throttle controller
/// 4. Controller adjusts its output for the next cycle (PID) or does nothing (Static)
///
/// 🧠 TRIBAL KNOWLEDGE: compose step moved to TransformWorker. SinkWorker only concatenates
/// pre-composed payloads. The "second copy" assembles multiple wire-format chunks into one
/// HTTP body / file write. This is pure memcpy — no parsing, no transform, no CPU work.
///
/// Extracted as a function because the SinkWorker flushes from three places:
/// 1. When the buffer is full enough (byte threshold)
/// 2. When the channel closes (final flush)
/// 3. When cancellation fires (emergency flush)
///
/// "He who duplicates flush logic, debugs it in three places at 3am." — Ancient proverb 💀
async fn flush_and_measure(
    buffer: &mut Vec<PoolBuffer>,
    buffer_bytes: &mut usize,
    sink: &mut SinkBackend,
    throttle_controller: &mut ThrottleControllerBackend,
) -> Result<()> {
    // 📦 Assemble buffered payloads into a single contiguous PoolBuffer for the HTTP body.
    // 🧠 std::mem::take swaps the buffer out (O(1)) — payloads move, Vec resets. 🦆
    let mut the_payloads_to_concat = std::mem::take(buffer);
    *buffer_bytes = 0;

    // 🚀 Fast path: single payload → skip the concat entirely. Zero memcpy.
    // 🧠 TRIBAL KNOWLEDGE: when pages are large relative to max_request_size (common for
    // ES reindex with 1000-doc pages), the buffer usually holds exactly 1 payload.
    // Skipping the rent+extend_from_slice memcpy saves 5-10MB of copying per flush.
    // The slow path (multi-payload concat) handles the rare case of small pages accumulating.
    // "Why copy bytes when you can just... not?" — Ancient optimization proverb 🏋️🦆
    let payload = if the_payloads_to_concat.len() == 1 {
        the_payloads_to_concat.pop().unwrap()
    } else {
        // 🐢 Multi-payload: concatenate into one contiguous buffer for HTTP body
        let the_total_bytes: usize = the_payloads_to_concat.iter().map(|p| p.len()).sum();
        let mut combined = buffer_pool::rent(the_total_bytes);
        for chunk in &the_payloads_to_concat {
            combined.extend_from_slice(chunk.as_bytes());
        }
        combined
    };

    // 📡 Send the concatenated payload to the sink. Pure I/O.
    // ⏱️ Measure the drain duration — this is the feedback signal for the PID controller.
    // "Time is what we measure. Bytes are what we control. Wisdom is knowing the difference." 🧠
    // 🧠 TRIBAL KNOWLEDGE: "[]" is the empty payload sentinel from JsonArrayComposer.
    // When InMemory sink + JsonArrayComposer composes zero items, it produces "[]" — a valid
    // but empty JSON array. We skip sending it because POSTing an empty array to a bulk API
    // is pointless (and some backends might reject it). NdjsonComposer returns "" for empty,
    // so the is_empty() / len() == 0 check catches that case. Both paths converge here: skip empty payloads.
    // 🧠 PoolBuffer comparison: check as_str() for "[]" sentinel — JSON array composer writes brackets as UTF-8.
    let is_empty_payload = payload.is_empty() || payload.as_str().map(|s| s == "[]").unwrap_or(false);
    if !is_empty_payload {
        let payload_bytes = payload.len() as u64;
        // 📊 Count docs: for NDJSON bulk, each doc = 2 lines (action + source).
        // Newlines / 2 gives us document count. For non-bulk formats, this is approximate
        // but still useful for throughput tracking. "Close enough for jazz." 🎷🦆
        let the_newline_census = payload.as_bytes().iter().filter(|&&b| b == b'\n').count() as u64;
        let doc_count = the_newline_census / 2;

        let send_stopwatch = Instant::now();
        sink.drain(payload).await.context(
            "💀 SinkWorker failed to drain payload to sink — the I/O layer rejected our offering. \
             The payload was concatenated with care. The sink said no. Like my prom date.",
        )?;
        // 🔬 Capture nanos once, derive both ms for PID and nanos for diagnostics
        let the_drain_nanos = send_stopwatch.elapsed().as_nanos() as u64;
        let elapsed_ms = the_drain_nanos as f64 / 1_000_000.0;

        if crate::is_bench_mode() {
            crate::diag_report_drain(the_drain_nanos);
        }

        // 📊 Report to the bench counters — atomic, lockless, no overhead when not benchmarking
        crate::report_docs_drained(doc_count, payload_bytes);

        // 🧠 Feed the measured duration into the throttle controller.
        // For Static: this is a no-op (cool story bro).
        // For PID: this drives the feedback loop — error → gain → adjustment → new output.
        throttle_controller.measure(elapsed_ms);
        debug!(
            "⏱️ SinkWorker drain took {:.1}ms — throttle controller output now: {} bytes",
            elapsed_ms,
            throttle_controller.output()
        );
    }

    // 🧹 Buffer already emptied by std::mem::take above — nothing to clear, nothing to reset.
    // "The buffer is empty. The sink is fed. The bytes are at peace." — Ancient proverb 🧵

    Ok(())
}
