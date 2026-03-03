// ai
//! 🚰🕳️💀 The Sink trait — the final destination. The end of the line. The last stop.
//!
//! 🎬 COLD OPEN — INT. ELASTICSEARCH CLUSTER — 3:47 AM
//!
//! *The payload arrived at the bulk endpoint. It was 12MB of NDJSON, carefully composed,
//! lovingly buffered, artisanally transformed. The sink looked at it. Looked at the cluster.
//! Looked back at the payload. "I just do I/O," it whispered. And POST'd.*
//!
//! *The cluster returned 200. The sink said nothing. It never does.* 🦆
use anyhow::Result;
use async_trait::async_trait;

use crate::backends::{elasticsearch, file, in_mem, opensearch};

/// 🕳️ A sink that drains pre-rendered payloads — pure I/O, zero logic.
///
/// The yin to the source's yang. The drain at the bottom of the pipeline tub.
/// Sinks are ONLY an abstraction for how to drain the payload — HTTP POST to /_bulk,
/// write to file, stash in memory. They do not buffer. They do not transform.
/// They receive the full rendered payload and drain it. Like a postal worker who
/// delivers the mail without reading it. (Unlike your actual postal worker, Kevin.)
///
/// # Contract 📜
/// - `drain` accepts a fully rendered payload string and writes/sends it. That's it.
/// - `close` flushes, finalizes, and bids the data a fond farewell. MUST be called.
///   Skipping `close` is a bug. It is also considered rude.
/// - Buffering, transforming, and binary collecting happen in the SinkWorker, NOT here.
///
/// # Knowledge Graph 🧠
/// - Pattern: trait → concrete impls (FileSink, InMemorySink, ElasticsearchSink) → SinkBackend enum
/// - SinkWorker does: transform → buffer → binary collect → call sink.drain(payload)
/// - Sink does: I/O. Just I/O. HTTP POST, file write, memory push. Nothing else.
/// - Ancient proverb: "He who puts business logic in the Sink, debugs in production."
#[async_trait]
pub trait Sink: std::fmt::Debug {
    /// 🚰 Drain a fully rendered payload to the destination. I/O only. No questions asked.
    async fn drain(&mut self, payload: String) -> Result<()>;
    /// 🗑️ Flush, finalize, and release. Call this. Always. No exceptions. Not even on Fridays.
    async fn close(&mut self) -> Result<()>;
}

/// 🎭 The many faces of a Sink — a polymorphic casting call for data destinations.
///
/// Mirrors `SourceBackend` on the other end of the pipeline. Whoever designed this
/// was clearly a fan of symmetry. Or they ran out of ideas. Hard to tell.
///
/// The enum dispatches `send` and `close` to the inner concrete type,
/// keeping the supervisor blissfully ignorant of where data actually lands.
/// Ignorance is a feature. It's called "abstraction." We put it in AGENTS.md.
#[derive(Debug)]
pub(crate) enum SinkBackend {
    InMemory(in_mem::InMemorySink),
    File(file::FileSink),
    Elasticsearch(elasticsearch::ElasticsearchSink),
    /// 🔍 OpenSearch sink — the ES fork's bulk API, same NDJSON, different license
    OpenSearch(opensearch::OpenSearchSink),
}

#[async_trait]
impl Sink for SinkBackend {
    async fn drain(&mut self, payload: String) -> Result<()> {
        // -- 🚰 Four drains, one payload. The enum knows which drain. The payload doesn't care.
        match self {
            SinkBackend::InMemory(sink) => sink.drain(payload).await,
            SinkBackend::File(sink) => sink.drain(payload).await,
            SinkBackend::Elasticsearch(sink) => sink.drain(payload).await,
            SinkBackend::OpenSearch(sink) => sink.drain(payload).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        // -- 🎬 "That's a wrap!" — the director, at the end of every pipeline run
        match self {
            SinkBackend::InMemory(sink) => sink.close().await,
            SinkBackend::File(sink) => sink.close().await,
            SinkBackend::Elasticsearch(sink) => sink.close().await,
            SinkBackend::OpenSearch(sink) => sink.close().await,
        }
    }
}
