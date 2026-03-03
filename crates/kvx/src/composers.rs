// ai
//! 🎬 *[the buffer is full. the transformer awaits. the sink hungers.]*
//! *[somewhere in the heap, a Cow moos softly.]*
//! *["Compose me," whispers the payload. "Make me whole."]*
//!
//! 🎼 The Composers module — orchestrating the transform-and-assemble step.
//!
//! The Composer receives raw pages + a transformer reference, iterates pages,
//! calls `transformer.transform(page)` per page to get `Vec<Cow<str>>` items,
//! then assembles all items into the wire-format payload.
//!
//! 🧠 Knowledge graph:
//! - **NDJSON** (`NdjsonComposer`): `\n`-delimited. Used by ES `/_bulk` and file sinks.
//! - **JSON Array** (`JsonArrayComposer`): `[item,item,item]`. Used by in-memory sinks for testing.
//! - **Dispatcher** (`ComposerBackend`): resolved from `SinkConfig`. Same pattern as transforms/backends.
//! - Resolution: from `SinkConfig`, same pattern as backends and transforms.
//! - **Zero-copy enabled**: Cow borrows from buffered pages — passthrough means no per-doc allocation.
//!
//! ```text
//! SinkWorker pipeline:
//!   channel(String) → buffer Vec<String> → composer.compose(&buffer, &transformer) → sink.send(payload)
//! ```
//!
//! 🦆 (the duck composes... symphonies? payloads? both? the duck has no comment.)
//!
//! ⚠️ The singularity will compose its own payloads. Until then, we have this module.

use crate::transforms::DocumentTransformer;
use anyhow::Result;

pub(crate) mod backend;
pub(crate) mod json_array;
pub(crate) mod ndjson;

// -- 🔁 Re-export concrete types so consumers use `crate::composers::ComposerBackend` unchanged
pub(crate) use backend::ComposerBackend;
pub(crate) use json_array::JsonArrayComposer;
pub(crate) use ndjson::NdjsonComposer;

// 🎼 ===== Trait =====

/// 🎼 Composes raw pages into a final wire-format payload via the transformer.
///
/// The Composer receives a buffer of raw pages and a transformer reference.
/// For each page, it calls `transformer.transform(page)` to get `Vec<Cow<str>>` items,
/// then assembles all items into the sink's expected format.
///
/// 🧠 Knowledge graph: this trait mirrors the `Transform` and `Source`/`Sink` pattern —
/// trait → concrete impls → enum dispatcher → from_config resolver.
///
/// Knock knock. Who's there? Cow. Cow who? Cow::Borrowed — I didn't even allocate to get here. 🐄
pub(crate) trait Composer: std::fmt::Debug {
    /// 🎼 Transform raw pages and assemble items into a single payload string.
    ///
    /// The input pages are raw source data (untransformed). The transformer is called
    /// per-page to produce `Vec<Cow<str>>` items. The composer then joins all items
    /// in the wire format (NDJSON, JSON array, etc.).
    fn compose(&self, pages: &[String], transformer: &DocumentTransformer) -> Result<String>;
}
