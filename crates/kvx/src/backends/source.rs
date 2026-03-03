// ai
//! 🚰📄🔄 The Source trait — where every data migration begins its journey.
//!
//! 🎬 COLD OPEN — INT. A VAST INDEX — DOCUMENTS STRETCH TO THE HORIZON
//!
//! *Billions of documents sat in storage. They'd been there for years. Nobody visited.
//! Nobody queried. Then one day, a `next_page()` call came. And one by one, page by page,
//! the documents marched out — through the trait, through the channel, into the unknown.*
//!
//! *"Where are we going?" asked document 47,391.*
//! *"Somewhere better," said the SourceWorker. "Probably."*
//!
//! 🦆 The duck was the first document to volunteer. It had nothing to lose.
use anyhow::Result;
use async_trait::async_trait;

use crate::backends::{elasticsearch, file, in_mem, opensearch, s3_rally};

/// 🚰 A source that produces one raw page per call — maximally ignorant of content format.
///
/// Implement this trait and you too can be the origin of someone else's data problems.
/// Guaranteed to dispense only the finest organic, free-range, artisanal bytes.
///
/// # Contract 📜
/// - `pump(doc_count_hint)` returns `Option<String>` — one raw page of data, uninterpreted.
/// - `doc_count_hint` tells the source how many docs are desired — sources that support it
///   adjust their batch size accordingly. Those that don't, ignore it gracefully.
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
/// - `pump` replaces the old `next_page` + `set_page_size_hint` two-step — one call, one hint, one page.
#[async_trait]
pub trait Source: std::fmt::Debug {
    /// 🚰 Pump the next raw page of data, with a doc count hint for batch sizing.
    ///
    /// `doc_count_hint` is the controller's recommended batch size (number of documents).
    /// Sources that support dynamic sizing (File, S3Rally) use it. Others ignore it.
    /// Returns `Ok(Some(page))` while data flows — one page per call, content uninterpreted.
    /// Returns `Ok(None)` when the tap runs dry. EOF. Fin. The end. 🏁
    /// Returns `Err(...)` when something has gone sideways, sidelong, or fully upside-down.
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<String>>;
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
    /// 🔍 OpenSearch source — PIT + search_after pagination, the ES fork that dared to be free
    OpenSearch(opensearch::OpenSearchSource),
    /// 🪣 S3 Rally source — benchmark data straight from the cloud, no layover
    S3Rally(s3_rally::S3RallySource),
}

#[async_trait]
impl Source for SourceBackend {
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<String>> {
        // -- 🚰 The enum match: five backends enter, one page leaves. Thunderdome rules.
        match self {
            SourceBackend::InMemory(i) => i.pump(doc_count_hint).await,
            SourceBackend::File(f) => f.pump(doc_count_hint).await,
            SourceBackend::Elasticsearch(es) => es.pump(doc_count_hint).await,
            SourceBackend::OpenSearch(os) => os.pump(doc_count_hint).await,
            SourceBackend::S3Rally(s3) => s3.pump(doc_count_hint).await,
        }
    }
}
