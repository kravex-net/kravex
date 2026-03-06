// ai
//! 🎬 *[the buffer is full. the caster awaits. the sink hungers.]*
//! *[somewhere in the heap, a String moos softly.]*
//! *["Join me," whispers the payload. "Make me whole."]*
//!
//! 🎼 The Manifolds module — orchestrating the cast-and-join step.
//!
//! The Manifold receives raw feeds + a caster reference, iterates feeds,
//! calls `caster.cast(feed)` per feed to get the transformed String,
//! then joins all results into the wire-format payload.
//!
//! 🧠 Knowledge graph:
//! - **NDJSON** (`NdjsonManifold`): `\n`-delimited. Used by ES `/_bulk` and file sinks.
//! - **JSON Array** (`JsonArrayManifold`): `[item,item,item]`. Used by in-memory sinks for testing.
//! - **Dispatcher** (`ManifoldBackend`): resolved from `SinkConfig`. Same pattern as casts/backends.
//! - Resolution: from `SinkConfig`, same pattern as backends and casts.
//!
//! ```text
//! Joiner pipeline:
//!   ch1(Feed) → buffer Vec<String> → manifold.join(&buffer, &caster) → ch2(Payload) → Drainer → sink.drain()
//! ```
//!
//! 🦆 (the duck joins... symphonies? payloads? both? the duck has no comment.)
//!
//! ⚠️ The singularity will join its own payloads. Until then, we have this module.

use crate::casts::{DocumentCaster, Caster};
use anyhow::Result;

pub mod backend;
pub mod json_array;
pub mod ndjson;

// -- 🔁 Re-export concrete types so consumers use `crate::manifolds::ManifoldBackend` unchanged
pub use backend::ManifoldBackend;
pub use json_array::JsonArrayManifold;
pub use ndjson::NdjsonManifold;

// ===== Trait =====

/// 🎼 Joins raw feeds into a final wire-format payload via the caster.
///
/// The Manifold receives a buffer of raw feeds and a caster reference.
/// For each feed, it calls `caster.cast(feed)` to get the transformed String,
/// then joins all results into the sink's expected format.
///
/// 🧠 Knowledge graph: this trait mirrors the `Caster` and `Source`/`Sink` pattern —
/// trait → concrete impls → enum dispatcher → from_config resolver.
///
/// Knock knock. Who's there? String. String who? String::with_capacity — I came prepared. 🎯
pub trait Manifold: std::fmt::Debug {
    /// 🎼 Cast raw feeds and join results into a single payload string.
    ///
    /// The input feeds are raw source data (un-cast). The caster is called
    /// per-feed to produce a transformed String. The manifold then joins all results
    /// in the wire format (NDJSON, JSON array, etc.).
    fn join(&self, feeds: &[String], caster: &DocumentCaster) -> Result<String>;
}
