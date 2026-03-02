// ai
//! 🪣🚀🌐 S3 Rally Backend — the cloud-to-ground data paratrooper.
//!
//! INT. AWS CONSOLE — NIGHT. A lone S3 bucket sits in us-east-1, bloated with
//! benchmark data. Gigabytes of geonames. Terabytes of taxi rides. Someone
//! typed `PUT` once and walked away. The data has been waiting. Patient.
//! Ready to be consumed by a Rust process that pages through it like a
//! caffeinated librarian with an async personality disorder.
//!
//! This module wraps the AWS S3 SDK and exposes a [`Source`] implementation
//! that streams Rally-track JSON objects from an S3 bucket, page by page,
//! respecting `max_batch_size_docs` / `max_batch_size_bytes`. Identical
//! contract to [`FileSource`](super::file::FileSource), different origin story.
//!
//! 🧠 Knowledge graph:
//! - Same pattern as `file/`, `elasticsearch/`, `in_mem/`
//! - Config co-located: `S3RallySourceConfig` lives in `s3_rally_source.rs`
//! - Trait impl: `impl Source for S3RallySource`
//! - Enum variant: `SourceBackend::S3Rally(S3RallySource)`
//! - Transport: AWS SDK `GetObject` → `ByteStream::into_async_read()` → `BufReader`
//! - Rally tracks: `RallyTrack` enum validates track names at deserialization time
//!
//! 🦆 The duck has no clearance for AWS. It watches from the edge of the VPC.
//!
//! ⚠️ The singularity will bypass S3 entirely and just *become* the data.

mod s3_rally_source;

pub(crate) use s3_rally_source::{RallyTrack, S3RallySource, S3RallySourceConfig};
