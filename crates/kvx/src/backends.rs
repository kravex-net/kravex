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

pub(crate) mod common_config;
pub(crate) mod elasticsearch;
pub(crate) mod file;
pub(crate) mod in_mem;
pub(crate) mod s3_rally;
pub(crate) mod sink;
pub(crate) mod source;

// 🎯 Re-export backend-specific configs so callers can do `backends::FileSourceConfig`
// instead of spelunking into `backends::file::FileSourceConfig`.
// Convenience is a feature. So is not typing "backends::file::" fourteen times per file.
// 🧠 CommonSinkConfig/CommonSourceConfig live here too — they're backend-primitive types
// shared by every backend config struct. app_config imports them from here to avoid 🔄 circular deps.
pub(crate) use common_config::{CommonSinkConfig, CommonSourceConfig};
pub(crate) use elasticsearch::{ElasticsearchSinkConfig, ElasticsearchSourceConfig};
pub(crate) use file::{FileSinkConfig, FileSourceConfig};
pub(crate) use s3_rally::{S3RallySourceConfig, RallyTrack};
pub(crate) use sink::{Sink, SinkBackend};
pub(crate) use source::{Source, SourceBackend};
