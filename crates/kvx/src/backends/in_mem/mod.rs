//! # Previously, on Kravex...
//!
//! 🎬 The data was trapped. Stranded between two search engines like a traveler
//! stuck in a connecting airport with no WiFi and a dead phone. Someone had to
//! move it. Someone had to be brave. Someone had to write a backend so simple
//! it lives entirely in RAM, gone the moment you blink.
//!
//! That someone was this module.
//!
//! `in_mem` provides an in-memory [`Source`] and [`Sink`] for testing and local
//! development. The [`InMemorySource`] emits exactly one batch of hardcoded docs
//! and then, like my motivation on a Friday afternoon, yields nothing further.
//! The [`InMemorySink`] collects received batches behind an `Arc<Mutex<...>>`
//! so callers can inspect what arrived — great for assertions, great for trust
//! issues, great for both.
//!
//! 🦆
//!
//! ⚠️ This is NOT for production. This is for tests. If you're deploying this
//! to prod, please also deploy a therapist.
//!
//! ✅ No network calls. No disk I/O. No heartbeat. No mortgage on the line.
//! Just vibes and heap memory.

mod in_mem_sink;
mod in_mem_source;

pub use in_mem_sink::InMemorySink;
pub use in_mem_source::InMemorySource;
