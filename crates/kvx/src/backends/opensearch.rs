// ai
//! # 📡 THE OPENSEARCH BACKEND 🔍🚀
//!
//! 🎬 COLD OPEN — INT. SERVER ROOM — THE FORK THAT LAUNCHED A THOUSAND CLUSTERS
//!
//! Once upon a time, Elasticsearch was open source. Then it wasn't. Then a
//! plucky group at AWS said "hold my beer" and forked it. The result?
//! OpenSearch. Same bulk API. Same search_after. Different logo. Different
//! license. Same existential dread when your cluster goes yellow.
//!
//! This module provides sink and source backends for OpenSearch 2.x and 3.x.
//! It mirrors the Elasticsearch backend structure because the APIs are
//! protocol-compatible, but lives in its own module because:
//!   1. OpenSearch is diverging (3.x has new toys)
//!   2. AWS SigV4 auth is a first-class concern (coming soon™)
//!   3. Self-signed certs in dev clusters need `danger_accept_invalid_certs`
//!   4. The module system has opinions and we respect them
//!
//! ## Knowledge Graph 🧠
//! - Pattern: same as `elasticsearch/` — mod.rs re-exports, split source/sink files
//! - Sink: POST to `/_bulk` with NDJSON (identical wire format to ES)
//! - Source: PIT + `search_after` pagination (identical API to ES 7.10+)
//! - Auth: basic auth + API key now; AWS SigV4 stubbed for future story
//! - TLS: optional `danger_accept_invalid_certs` for dev clusters
//!
//! ⚠️ "In a world where search engines forked... one backend dared to POST NDJSON."
//! 🦆 The duck migrated from Elasticsearch. It prefers the Apache 2.0 license.

mod opensearch_sink;
mod opensearch_source;

pub use opensearch_sink::{OpenSearchSink, OpenSearchSinkConfig};
pub use opensearch_source::{OpenSearchSource, OpenSearchSourceConfig};
