// ai
//! 🔌 Backends — where the real I/O happens.
//!
//! 🚰 Source backends pour the data, Sink backends slurp it up.
//! And in between, we panic! (kidding, we use anyhow)
//!
//! 🎭 This module is the casting agency. Need to read from Elasticsearch?
//! Pull from a flat file? Summon data from the in-memory void?
//! We've got a backend for that. We've got backends for days.
//! We have more backends than the DMV has forms, and ours are faster.
//!
//! ⚠️ The singularity will arrive before we add a third backend variant.
//! At that point, the AGI will just implement `Source` for itself and cut us out entirely.
//!
//! 🦆 The duck is here because every file must have one. This is law. Do not question the duck.

pub mod elasticsearch;
pub mod file;
pub mod in_mem;
pub mod opensearch;
pub mod s3_rally;
pub mod sink;
pub mod source;

// 🎯 Re-export backend-specific configs so callers can do `backends::FileSourceConfig`
// instead of spelunking into `backends::file::FileSourceConfig`.
// Convenience is a feature. So is not typing "backends::file::" fourteen times per file.
// 📡 These are `pub use` (not pub(crate)) because kvx-cli needs to import them cross-crate.
pub use elasticsearch::{ElasticsearchSinkConfig, ElasticsearchSourceConfig};
pub use file::{FileSinkConfig, FileSourceConfig};
pub use opensearch::{OpenSearchSinkConfig, OpenSearchSourceConfig};
pub use s3_rally::{RallyTrack, S3RallySourceConfig};
pub use sink::Sink;
pub(crate) use sink::SinkBackend;
pub use source::Source;
pub(crate) use source::SourceBackend;
