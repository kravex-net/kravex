// ai
//! 🧵 Workers: the backbone of kravex, the unsung heroes, the ones who actually
//! do the work while the Supervisor takes all the credit in the sprint retro.
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

pub(crate) mod sink_worker;
pub(crate) use sink_worker::SinkWorker;
pub(crate) mod source_worker;
pub(crate) use source_worker::SourceWorker;
pub(crate) mod transform_worker;
pub(crate) use transform_worker::TransformWorker;

/// 🏗️ A background worker — the async task that actually moves data while the Supervisor
/// takes credit in the sprint retro. Implement this trait to join the labor force.
pub(crate) trait Worker {
    /// 🚀 Consume self and spawn a tokio task that runs until completion or dramatic failure.
    /// Returns a JoinHandle so the Supervisor can await results and pretend it helped. 🎭
    fn start(self) -> JoinHandle<Result<()>>;
}
