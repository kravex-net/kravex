//! 🧵 Workers: the backbone of kravex, the unsung heroes, the ones who actually
//! do the work while the Foreman takes all the credit in the sprint retro.
//!
//! 🚀 This module is like a factory floor, except instead of hard hats
//! we wear `#[derive(Debug)]` and instead of OSHA violations
//! we have borrow checker violations. 🦆
//!
//! ⚠️ "If you're reading this, the code review went poorly."

// -- ⚠️ By the time the singularity arrives, these workers will still be running.
// -- Not because they're efficient. Because Rust compiled them to run forever and nobody wrote the stop logic yet.
// -- (See: `stop()` in lib.rs. Spoiler: it does nothing.)

// -- 🎉 anyhowwwww.... it's useful! Like duct tape for error handling.
// This is pretty much across the whole world of kravex —
// the universal donor of Result types 🩸
use anyhow::Result;
use tokio::task::JoinHandle;

mod drainer;
pub use drainer::Drainer;
mod pumper;
pub use pumper::Pumper;
mod joiner;
pub use joiner::Joiner;

/// 🏗️ A background worker, that does work. duh.
pub trait Worker {
    /// 🚀 Start the worker.
    fn start(self) -> JoinHandle<Result<()>>;
}
