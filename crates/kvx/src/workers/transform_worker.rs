// ai
//! 🎬 *[the raw bytes arrive, trembling, still in source format]*
//! *[a TransformWorker steps forward, cracks its knuckles]*
//! *["I'm going to turn you into something beautiful," it whispers]*
//! *[the bytes scream as they're iterated, one by one, into bulk format]*
//! *[it was efficient. it was CPU-bound. it was... kind of violent.]*
//!
//! 🧠 The TransformWorker module — the CPU furnace of the kravex pipeline.
//! Sits between source and sink, consuming raw pages from Channel1,
//! transforming + composing them into wire-format payloads, and sending
//! the results to Channel2 for SinkWorkers to drain.
//!
//! 🧠 Knowledge graph:
//! - Pipeline: Source → Channel1(raw PoolBuffer) → TransformWorker → Channel2(composed PoolBuffer) → SinkWorker
//! - TransformWorker owns DocumentTransformer + ComposerBackend (both Clone+Copy, zero-sized)
//! - Entire recv→compose→send loop runs on blocking thread pool (single spawn_blocking, not per-page)
//! - Compose called directly: `composer.compose(&[page], &transformer)` — no spawn_blocking hop per page
//! - One page in, one composed payload out — no internal batching (SinkWorker handles buffering)
//! - N TransformWorkers share Channel1 receiver (MPMC fan-out) and Channel2 sender (MPMC fan-in)
//! - Default N = num_cpus - 1 (one core reserved for tokio async polling)
//!
//! 🦆 (the duck has evolved. it now does byte surgery.)
//!
//! ⚠️ The singularity will happen before we optimize this further.
//! But at least our bytes will be in bulk format when it does.

use super::Worker;
use crate::buffer_pool::PoolBuffer;
use crate::composers::{Composer, ComposerBackend};
use crate::transforms::DocumentTransformer;
use anyhow::{Context, Result};
use async_channel::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

/// 🧠 The TransformWorker: CPU-bound byte cruncher that converts raw source pages
/// into wire-format payloads (NDJSON bulk, JSON array, etc.)
///
/// 🧠 Knowledge graph:
/// - Receives raw PoolBuffer from Channel1 via `recv_blocking()` (blocking, no async overhead)
/// - Calls `composer.compose(&[page], &transformer)` directly (synchronous, no spawn_blocking hop)
/// - Sends composed PoolBuffer to Channel2 via `send_blocking()` (blocking, backpressure-aware)
/// - Entire loop runs on tokio's blocking thread pool — pure CPU, zero async context switches
/// - One page at a time — no batching, no buffering. That's SinkWorker's job.
/// - When Channel1 closes (source EOF), TransformWorker drains remaining pages,
///   then drops its Channel2 sender to propagate close downstream.
/// - Cancellation: synchronous `is_cancelled()` check at loop top (atomic load, zero overhead)
///
/// Like a factory worker on a dedicated assembly line — not waiting for a desk phone to ring,
/// just bolting parts together as fast as the conveyor belt delivers them. 🏭📡
#[derive(Debug)]
pub(crate) struct TransformWorker {
    /// 📥 Receiver for raw pages from source channel (Channel1)
    rx: Receiver<PoolBuffer>,
    /// 📤 Sender for composed payloads to sink channel (Channel2)
    tx: Sender<PoolBuffer>,
    /// 🔄 The transformer — knows how to convert between source and sink formats
    transformer: DocumentTransformer,
    /// 🏗️ The composer — assembles transformed items into wire-format payload
    composer: ComposerBackend,
    /// 🛑 The escape hatch — graceful shutdown signal
    cancellation_token: CancellationToken,
}

impl TransformWorker {
    /// 🏗️ Constructs a new TransformWorker — the CPU furnace between source and sink.
    ///
    /// Give it two channel endpoints (rx from source, tx to sink), a transformer
    /// (format conversion), a composer (wire format assembly), and a cancellation token.
    /// It will crunch bytes until the channel runs dry or the token fires. 🔥
    pub(crate) fn new(
        rx: Receiver<PoolBuffer>,
        tx: Sender<PoolBuffer>,
        transformer: DocumentTransformer,
        composer: ComposerBackend,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            rx,
            tx,
            transformer,
            composer,
            cancellation_token,
        }
    }
}

impl Worker for TransformWorker {
    /// 🧠 Starts the TransformWorker as a pure blocking thread — no async bouncing.
    ///
    /// 🧠 TRIBAL KNOWLEDGE: the previous implementation was `tokio::spawn(async { ... })` which
    /// awaited `compose_page()` which itself called `spawn_blocking()`. That's 2 context switches
    /// per page (async task → blocking thread → async task) plus a `vec![raw_page]` heap alloc
    /// per page. Like calling a taxi to drive you across the street.
    ///
    /// New architecture: the ENTIRE recv→compose→send loop runs on tokio's blocking thread pool
    /// via one `spawn_blocking()`. Channel ops use `recv_blocking()` / `send_blocking()` — these
    /// are blocking variants from `async_channel` designed for exactly this pattern. Compose is
    /// called directly (synchronous) with a stack slice `&[raw_page]` instead of `vec![raw_page]`.
    ///
    /// Eliminates per-page: 2 context switches + 1 Vec heap alloc + 1 async task yield.
    /// The blocking thread pool autoscales — tokio spawns more threads as needed. 🧵
    ///
    /// Cancellation: `is_cancelled()` is a synchronous atomic check at loop top. No select! needed.
    /// Channel close propagation: when source closes Channel1, `recv_blocking()` returns Err,
    /// we break, drop tx (Channel2 sender), which propagates close to SinkWorkers.
    ///
    /// "You don't need async to crunch bytes. You need a big thread and a bigger buffer." 🦆
    fn start(self) -> JoinHandle<Result<()>> {
        tokio::task::spawn_blocking(move || {
            debug!("🧠 TransformWorker started on blocking thread — pure CPU, zero async bouncing. Like a factory worker, not an office worker.");
            loop {
                // 🛑 Synchronous cancellation check — atomic load, zero overhead.
                // Checked at loop top so we don't start work on a page we'll never finish.
                if self.cancellation_token.is_cancelled() {
                    debug!(
                        "🛑 TransformWorker: cancellation received. \
                         Dropping tools mid-shift. The bytes will remain uncooked. 🍳"
                    );
                    break;
                }

                // 📥 Blocking receive from Channel1 (source → transform).
                // This blocks the current thread (NOT the async executor) until a page arrives.
                // If Channel1 closes (source EOF), recv_blocking() returns Err and we break.
                let raw_page = match self.rx.recv_blocking() {
                    Ok(page) => page,
                    Err(_) => {
                        // 🏁 Channel1 closed — source is done. No more raw pages.
                        // tx (Channel2 sender) drops when we exit, propagating close downstream.
                        debug!(
                            "🏁 TransformWorker: source channel closed. \
                             The well is dry. The CPU rests. Goodnight. 💤"
                        );
                        break;
                    }
                };

                let the_raw_page_bytes = raw_page.len();
                debug!(
                    "🧠 TransformWorker received {} byte raw page — time to crunch",
                    the_raw_page_bytes
                );

                // 🔥 Direct synchronous compose — no spawn_blocking hop, no vec! heap alloc.
                // 🧠 TRIBAL KNOWLEDGE: &[raw_page] is a stack slice (one-element array reference).
                // Previous code: vec![raw_page] → heap alloc 24 bytes + pointer + drop.
                // New code: &[raw_page] → zero alloc, lives on the stack. The allocator sends a thank-you card.
                let composed_payload = self
                    .composer
                    .compose(&[raw_page], &self.transformer)
                    .context(
                        "💀 TransformWorker compose failed — the bytes went in raw \
                         and came out as existential dread. Check the transformer \
                         and source data quality. The CPU tried its best. 🧠",
                    )?;

                debug!(
                    "✅ TransformWorker composed {} raw bytes → {} payload bytes",
                    the_raw_page_bytes,
                    composed_payload.len()
                );

                // 📤 Blocking send to Channel2 (transform → sink).
                // Blocks this thread if channel is full (backpressure propagation — good!).
                // Returns Err if all SinkWorkers dropped their receivers (unexpected).
                if self.tx.send_blocking(composed_payload).is_err() {
                    debug!("🛑 TransformWorker: sink channel closed unexpectedly — stopping");
                    break;
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composers::ComposerBackend;
    use crate::transforms::DocumentTransformer;
    use tokio_util::sync::CancellationToken;

    // 🧪 Helper: creates a passthrough transformer + ndjson composer for tests.
    // -- The simplest pipeline: bytes go in, bytes come out, nobody gets hurt. 🦆
    fn the_test_transformer_and_composer() -> (DocumentTransformer, ComposerBackend) {
        (
            DocumentTransformer::Passthrough(crate::transforms::passthrough::Passthrough),
            ComposerBackend::Ndjson(crate::composers::ndjson::NdjsonComposer),
        )
    }

    /// 🧪 The happy path: send a raw NDJSON page, receive a composed payload.
    /// Like a cooking show where nothing catches fire. Unrealistic but aspirational.
    #[tokio::test]
    async fn the_one_where_transform_worker_transforms_a_page() {
        let (tx_source, rx_source) = async_channel::bounded::<PoolBuffer>(10);
        let (tx_sink, rx_sink) = async_channel::bounded::<PoolBuffer>(10);
        let token = CancellationToken::new();
        let (transformer, composer) = the_test_transformer_and_composer();

        let worker = TransformWorker::new(
            rx_source,
            tx_sink,
            transformer,
            composer,
            token.clone(),
        );

        let handle = worker.start();

        // 📤 Send a raw page with 3 JSON docs
        let raw_page = crate::buffer_pool::rent_from_string(
            "{\"doc\":1}\n{\"doc\":2}\n{\"doc\":3}".to_string(),
        );
        tx_source.send(raw_page).await.unwrap();
        // 🏁 Close source channel to signal EOF
        tx_source.close();

        // 📥 Receive the composed payload
        let composed = rx_sink.recv().await.unwrap();
        let payload_str = composed.as_str().unwrap();
        // 🎯 Passthrough + NdjsonComposer: each line gets a newline, plus trailing newline
        assert!(
            payload_str.contains("{\"doc\":1}"),
            "Payload should contain first doc"
        );
        assert!(
            payload_str.contains("{\"doc\":3}"),
            "Payload should contain last doc"
        );

        // 🏁 Worker should exit cleanly after source channel closes
        handle.await.unwrap().unwrap();

        // 🎯 Sink channel should be closed after worker exits (tx_sink dropped)
        assert!(
            rx_sink.recv().await.is_err(),
            "Sink channel should be closed after worker exits"
        );
    }

    /// 🧪 Close the source channel immediately — worker should exit cleanly
    /// and propagate close to sink channel. Like a restaurant that closes
    /// before the sous chef even puts on an apron. 🍳
    #[tokio::test]
    async fn the_one_where_transform_worker_handles_channel_close() {
        let (tx_source, rx_source) = async_channel::bounded::<PoolBuffer>(10);
        let (tx_sink, rx_sink) = async_channel::bounded::<PoolBuffer>(10);
        let token = CancellationToken::new();
        let (transformer, composer) = the_test_transformer_and_composer();

        let worker = TransformWorker::new(
            rx_source,
            tx_sink,
            transformer,
            composer,
            token.clone(),
        );

        let handle = worker.start();

        // 🏁 Close immediately — no pages sent
        tx_source.close();

        // 🎯 Worker exits cleanly
        handle.await.unwrap().unwrap();

        // 🎯 Sink channel should be closed (tx_sink was the only sender, now dropped)
        assert!(
            rx_sink.recv().await.is_err(),
            "Sink channel should be closed when transform worker exits without sending"
        );
    }

    /// 🧪 Fire the cancellation token — worker should stop mid-loop.
    /// The bytes remain uncooked. The CPU rests. All is well. 🛑
    #[tokio::test]
    async fn the_one_where_transform_worker_respects_cancellation() {
        let (_tx_source, rx_source) = async_channel::bounded::<PoolBuffer>(10);
        let (tx_sink, rx_sink) = async_channel::bounded::<PoolBuffer>(10);
        let token = CancellationToken::new();
        let (transformer, composer) = the_test_transformer_and_composer();

        let worker = TransformWorker::new(
            rx_source,
            tx_sink,
            transformer,
            composer,
            token.clone(),
        );

        let handle = worker.start();

        // 🛑 Fire the cancellation token — worker should exit even though source is still open
        token.cancel();

        // 🎯 Worker exits cleanly via cancellation branch
        handle.await.unwrap().unwrap();

        // 🎯 Sink channel closed (tx_sink dropped on worker exit)
        assert!(
            rx_sink.recv().await.is_err(),
            "Sink channel should close after cancellation"
        );
    }

    /// 🧪 Spawn multiple transform workers, send M pages, verify all M come out.
    /// Like a kitchen brigade where everyone's chopping at once. 🔪🔪🔪
    #[tokio::test]
    async fn the_one_where_multiple_transform_workers_share_the_load() {
        let (tx_source, rx_source) = async_channel::bounded::<PoolBuffer>(100);
        let (tx_sink, rx_sink) = async_channel::bounded::<PoolBuffer>(100);
        let token = CancellationToken::new();
        let (transformer, composer) = the_test_transformer_and_composer();

        // 🧠 Spawn 4 transform workers sharing the same channels
        let mut handles = Vec::new();
        for _ in 0..4 {
            let worker = TransformWorker::new(
                rx_source.clone(),
                tx_sink.clone(),
                transformer.clone(),
                composer.clone(),
                token.clone(),
            );
            handles.push(worker.start());
        }
        // 🗑️ Drop the extra clones so channel closes propagate correctly
        drop(rx_source);
        drop(tx_sink);

        // 📤 Send 20 raw pages
        let the_page_count = 20;
        for i in 0..the_page_count {
            let raw_page = crate::buffer_pool::rent_from_string(
                format!("{{\"doc\":{}}}", i),
            );
            tx_source.send(raw_page).await.unwrap();
        }
        // 🏁 Close source channel
        tx_source.close();

        // 📥 Collect all composed payloads
        let mut received_count = 0;
        while let Ok(_composed) = rx_sink.recv().await {
            received_count += 1;
        }

        // 🎯 All 20 pages should have been transformed and sent
        assert_eq!(
            received_count, the_page_count,
            "All pages should be transformed — got {received_count}, expected {the_page_count}"
        );

        // 🎯 All workers should exit cleanly
        for handle in handles {
            handle.await.unwrap().unwrap();
        }
    }
}
