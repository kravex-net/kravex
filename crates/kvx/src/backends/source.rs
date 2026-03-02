use anyhow::Result;
use async_trait::async_trait;

use crate::backends::{elasticsearch, file, in_mem, s3_rally};

/// 🚰 A source that produces one raw page per call — maximally ignorant of content format.
///
/// Implement this trait and you too can be the origin of someone else's data problems.
/// Guaranteed to dispense only the finest organic, free-range, artisanal bytes.
///
/// # Contract 📜
/// - `next_page` returns `Option<String>` — one raw page of data, uninterpreted.
/// - `None` = EOF. The well is dry. The golden retriever goes home. 🐕
/// - The source does NOT parse, split, or understand its content. It's a faucet, not a chef.
/// - The Composer downstream handles format understanding via the Transformer.
/// - The borrow checker demands `&mut self` because sources have state. And feelings. Mostly state.
///
/// # Knowledge Graph 🧠
/// - Pattern: trait → concrete impls (FileSource, InMemorySource, ElasticsearchSource) → SourceBackend enum
/// - Source returns raw pages → channel(String) → SinkWorker buffers → Composer transforms+assembles
/// - Source is a data faucet 🚿 — it pours, the pipeline catches
/// - **Zero-copy enabled**: Source doesn't split docs, Composer borrows from buffered pages via Cow
#[async_trait]
pub(crate) trait Source: std::fmt::Debug {
    /// 📄 Fetch the next raw page of data.
    ///
    /// Returns `Ok(Some(page))` while data flows — one page per call, content uninterpreted.
    /// Returns `Ok(None)` when the tap runs dry. EOF. Fin. The end. 🏁
    /// Returns `Err(...)` when something has gone sideways, sidelong, or fully upside-down.
    async fn next_page(&mut self) -> Result<Option<String>>;
}

/// 🎭 The many faces of a Source — a polymorphic casting call for data origins.
///
/// Each variant wraps a concrete source implementation. The enum itself dispatches
/// via `impl Source for SourceBackend`, so callers never need to know (or care)
/// whether they're reading from RAM, disk, or a cluster of overworked Elasticsearch nodes.
///
/// Think of it as a universal remote. Except it only controls data ingestion. And it's async.
/// And there is no warranty. Ancient proverb: "He who hardcodes the backend, migrates only once."
#[derive(Debug)]
pub(crate) enum SourceBackend {
    InMemory(in_mem::InMemorySource),
    File(file::FileSource),
    Elasticsearch(elasticsearch::ElasticsearchSource),
    /// 🪣 S3 Rally source — benchmark data straight from the cloud, no layover
    S3Rally(s3_rally::S3RallySource),
}

#[async_trait]
impl Source for SourceBackend {
    async fn next_page(&mut self) -> Result<Option<String>> {
        match self {
            SourceBackend::InMemory(i) => i.next_page().await,
            SourceBackend::File(f) => f.next_page().await,
            SourceBackend::Elasticsearch(es) => es.next_page().await,
            SourceBackend::S3Rally(s3) => s3.next_page().await,
        }
    }
}
