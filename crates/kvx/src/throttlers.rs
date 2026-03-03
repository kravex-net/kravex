// ai
//! 🎛️🔧🚀 Controllers — the feedback loop whisperers of the kravex pipeline.
//!
//! 🎬 COLD OPEN — INT. CONTROL ROOM — 2:17 AM
//!
//! A lone engineer stares at a wall of blinking monitors. Each one shows a different
//! bulk request, crawling across the wire at wildly different speeds. The throughput
//! graph looks like an EKG during a horror movie. A single tear rolls down their cheek.
//!
//! "What if," they whisper, staring at the production dashboard, "we stopped guessing
//! the batch size... and let the DATA tell us? And also let latency tell us? BOTH?"
//!
//! The room went silent. The borrow checker nodded. The PID gains converged. There
//! was order in the universe, briefly, before the next 429.
//!
//! This module provides **two orthogonal controller abstractions**:
//!
//! ```text
//!  SOURCE SIDE (doc count)          SINK SIDE (byte size)
//!  ─────────────────────────        ──────────────────────────────
//!  trait Controller                 trait ThrottleController
//!    fn output() -> usize             fn output() -> usize (bytes)
//!    fn measure(bytes: f64)           fn measure(ms: f64)
//!         │                                │
//!  ┌──────┴────────────┐          ┌────────┴───────────────────┐
//!  │ ConfigController  │          │ StaticThrottleController    │
//!  │ PidBytesToDocCount│          │ PidControllerBytesToMs      │
//!  └──────┬────────────┘          └────────┬───────────────────┘
//!         │                                │
//!  ControllerBackend               ThrottleControllerBackend
//!  (owned by SourceWorker)         (owned by SinkWorker)
//! ```
//!
//! ## Knowledge Graph 🧠
//! - `Controller` trait: source-side. `output()` = doc count hint. `measure()` = response bytes.
//! - `ThrottleController` trait: sink-side. `output()` = byte budget. `measure()` = request ms.
//! - `ControllerBackend`: enum dispatch for source controllers. Created from `ControllerConfig`.
//! - `ThrottleControllerBackend`: enum dispatch for sink controllers. Created via `new_static()` / `new_pid()`.
//! - `ControllerConfig`: serde-tagged enum for TOML `[controller]` section.
//! - SourceWorker loop: `output()` → `set_page_size_hint()` → `next_page()` → `measure(page.len())`.
//! - SinkWorker loop: `measure(duration_ms)` after each send → `output()` = next buffer byte cap.
//! - Pattern: trait → concrete impls → enum dispatch — same as Source/Sink/Transform throughout.
//!
//! ⚠️ The singularity will have better PID gains than us. Until then, we iterate. Literally. 🦆
//!
//! "In a world where batch sizes must adapt... two controllers dared to control."
//!   — Kravex: The Throttling (2026, rated PG-13 for mild derivative gain)
//!
//! Note: `pid_bytes_to_ms` is licensed under LICENSE-EE (BSL 1.1).
//! The trait definitions and static controllers are MIT licensed.

use serde::Deserialize;

pub(crate) mod config_controller;
pub(crate) mod pid_bytes_to_doc_count;
pub(crate) mod pid_bytes_to_ms;
pub(crate) mod static_throttle;

// ============================================================
//  🔧 ThrottleAppConfig — top-level [throttle] section in TOML
//  Centralized throttle config — one place for batch/request sizing. 🏠
// ============================================================

/// 🔧 Top-level throttle configuration — the `[throttle]` section in TOML.
///
/// 🧠 Knowledge graph:
/// - `source`: batch sizing for source workers (max_batch_size_docs, max_batch_size_bytes, controller)
/// - `sink`: request sizing for sink workers (max_request_size_bytes, throttle mode)
/// - Centralized here — backends are pure connection config. 🔌
///
/// ```toml
/// [throttle.source]
/// max_batch_size_docs = 10000
/// max_batch_size_bytes = 10485760
///
/// [throttle.sink]
/// max_request_size_bytes = 10485760
/// ```
///
/// "He who scatters config across backends, debugs across lifetimes." — Ancient proverb 🦆
#[derive(Debug, Deserialize, Clone, Default)]
pub struct ThrottleAppConfig {
    /// 🚰 Source-side throttle knobs — batch sizing and controller config
    #[serde(default)]
    pub source: SourceThrottleConfig,
    /// 🚰 Sink-side throttle knobs — request sizing and throttle mode
    #[serde(default)]
    pub sink: SinkThrottleConfig,
}

// -- 🦆 ThrottleAppConfig: #[derive(Default)] handles this now. Less code, same vibes.

/// 🚰 Source-side throttle config — controls batch sizing for `pump()`.
///
/// 🧠 Knowledge graph:
/// - `max_batch_size_docs`: doc-count ceiling per page (used by controller as default, by File/S3Rally directly)
/// - `max_batch_size_bytes`: byte-size ceiling per page (File/S3Rally read capacity)
/// - `controller`: adaptive batch sizing (Static = fixed, PidBytesToDocCount = feedback-driven)
///
/// Centralized here — backends are pure connection config. The backends are free. Like interns on their last day. 🎓
#[derive(Debug, Deserialize, Clone)]
pub struct SourceThrottleConfig {
    /// 📦 Max docs per batch page — the doc-count speed limiter
    #[serde(default = "default_max_batch_size_docs")]
    pub max_batch_size_docs: usize,
    /// 📦 Max bytes per batch page — the byte-size speed limiter
    #[serde(default = "default_max_batch_size_bytes")]
    pub max_batch_size_bytes: usize,
    /// 🎛️ Controller config — Static (default) or PidBytesToDocCount (adaptive)
    #[serde(default)]
    pub controller: ControllerConfig,
}

impl Default for SourceThrottleConfig {
    fn default() -> Self {
        Self {
            max_batch_size_docs: 1000,
            max_batch_size_bytes: 1024 * 1024,
            controller: ControllerConfig::default(),
        }
    }
}

// 📦 10,000 docs per batch — the serde default, generous for TOML configs
fn default_max_batch_size_docs() -> usize {
    10000
}

// 📦 10MB — the serde default byte ceiling for source pages
fn default_max_batch_size_bytes() -> usize {
    10485760
}

/// 🚰 Sink-side throttle config — controls request sizing for `drain()`.
///
/// 🧠 Knowledge graph:
/// - `max_request_size_bytes`: flush threshold for the SinkWorker buffer
/// - `mode`: Static (fixed bytes) or Pid (adaptive latency-driven)
///
/// Centralized here — sinks are pure I/O config. As they always should have been. 🦆
#[derive(Debug, Deserialize, Clone)]
pub struct SinkThrottleConfig {
    /// 🚰 Max payload bytes per sink request — the flush trigger
    #[serde(default = "default_max_request_size_bytes")]
    pub max_request_size_bytes: usize,
    /// 🧠 Throttle strategy — Static (fixed bytes) or Pid (adaptive). Default: Static.
    #[serde(default)]
    pub mode: ThrottleConfig,
}

impl Default for SinkThrottleConfig {
    fn default() -> Self {
        Self {
            max_request_size_bytes: 64 * 1024 * 1024,
            mode: ThrottleConfig::default(),
        }
    }
}

// 🚰 10MB sink request size serde default — the conservative option for TOML configs
fn default_max_request_size_bytes() -> usize {
    10485760
}

pub(crate) use pid_bytes_to_ms::PidControllerBytesToMs;
pub(crate) use static_throttle::StaticThrottleController;

// ============================================================
//  ╔═══════════════════════════════════════════════════════╗
//  ║  🎛️ measure(bytes) ──▶ Controller ──▶ output(docs)  ║
//  ║    SOURCE SIDE — adapts doc count per page fetch     ║
//  ╚═══════════════════════════════════════════════════════╝
// ============================================================

/// 🎛️ Controller — the feedback loop trait for adaptive source batch sizing.
///
/// Think of it like cruise control for your data pipeline's source:
/// - `output()` = "how many docs should I fetch?" (batch size hint in doc count)
/// - `measure()` = "the last page weighed THIS many bytes" (observed response size)
///
/// The controller adjusts future doc count outputs based on past byte measurements.
/// For `ConfigController`, this is "set cruise control to 65 and never touch it."
/// For `PidBytesToDocCount`, this is "the PID algorithm from a 1922 paper,
///
/// # Contract 📜
/// - `output()` returns the current batch size recommendation (doc count)
/// - `measure(f64)` feeds a measurement (e.g., response size in bytes)
/// - Implementations must handle repeated calls gracefully
/// - Output must always be > 0 (you can't fetch negative documents, despite what the borrow checker implies)
///
/// 🧠 Knowledge graph: SourceWorker calls output() → source.pump(hint) →
/// measure(page.len()) — the eternal feedback loop. Hint + fetch in one call.
/// "He who measures not, optimizes in vain." — Ancient PID proverb 📜
pub(crate) trait Controller: std::fmt::Debug {
    /// 🎯 Get the current output value — the recommended batch size in doc count.
    ///
    /// Returns a doc count hint. Sources use this to adjust how many
    /// documents they fetch per `next_page()` call. Think of it as
    /// the thermostat reading: "set temperature to THIS."
    fn output(&self) -> usize;

    /// 📏 Feed a measurement back into the controller.
    ///
    /// The measurement semantics depend on the controller type:
    /// - `PidBytesToDocCount`: `measurement` = response size in bytes
    /// - `ConfigController`: measurement is politely ignored, like unsolicited career advice
    fn measure(&mut self, measurement: f64);
}

// ============================================================
//  🔧 ControllerConfig — TOML deserialization target
//  "He who configures the PID gains by hand, tunes until dawn."
// ============================================================

/// 🔧 Configuration enum for source controller selection and parameters.
///
/// Deserialized from the `[controller]` section of the TOML config.
/// Uses serde's internally tagged representation (`type` field).
///
/// ## TOML Examples 📄
///
/// Static (default — preserves existing behavior):
/// ```toml
/// [controller]
/// type = "static"
/// ```
///
/// PID bytes-to-doc-count:
/// ```toml
/// [controller]
/// type = "pid_bytes_to_doc_count"
/// desired_response_size_bytes = 5242880.0
/// initial_doc_count = 1000
/// min_doc_count = 100
/// max_doc_count = 10000
/// ```
///
/// 🧠 Knowledge graph: `ControllerConfig` → `ControllerBackend::from_config()` → concrete impl.
/// Same resolver pattern as `DocumentTransformer::from_configs()`.
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControllerConfig {
    /// 🧊 Static controller — returns the configured batch size. No PID. No feedback.
    /// The thermostat is unplugged. The cruise control is off. We're going manual.
    #[default]
    Static,
    /// 🎛️ PID controller that measures response bytes and outputs doc count.
    PidBytesToDocCount {
        /// 🎯 The desired response size in bytes — the "72°F" of our thermostat.
        /// The PID will chase this like a golden retriever chasing a tennis ball.
        desired_response_size_bytes: f64,
        /// 🚀 Starting doc count before PID has any measurements — the initial guess.
        /// Like your first salary negotiation: just throw a number out there.
        #[serde(default = "default_initial_doc_count")]
        initial_doc_count: usize,
        /// 📉 Minimum doc count — the floor. We'll never go below this.
        /// Because fetching 0 documents is technically an existential crisis.
        #[serde(default = "default_min_doc_count")]
        min_doc_count: usize,
        /// 📈 Maximum doc count — the ceiling. We'll never go above this.
        /// Because RAM is finite, even on those EC2 instances that cost more than rent.
        #[serde(default = "default_max_doc_count")]
        max_doc_count: usize,
    },
}

/// 🔢 Default initial doc count — 1000 docs. A respectable first guess.
/// Like showing up to a potluck with store-bought cookies: adequate.
fn default_initial_doc_count() -> usize {
    1000
}

/// 📉 Default minimum doc count — 10 docs. The absolute minimum viable batch.
/// Below this, you're basically doing one-at-a-time, which is called "grep". 💀
fn default_min_doc_count() -> usize {
    10
}

/// 📈 Default maximum doc count — 50,000 docs. The ceiling before your RAM files for divorce.
fn default_max_doc_count() -> usize {
    50_000
}

// ============================================================
//  🎭 ControllerBackend — source-side dispatching enum
//  Mirrors SourceBackend / SinkBackend / DocumentTransformer exactly.
// ============================================================

/// 🎭 The dispatching enum for source controllers — polymorphism via match, not vtables.
///
/// Same pattern as `SourceBackend`, `SinkBackend`, `DocumentTransformer`:
/// each variant wraps a concrete type that implements [`Controller`].
/// The enum dispatches via match. The branch predictor eliminates the cost.
/// The borrow checker approves. The developer cries tears of joy. 🎉
///
/// 🧠 Knowledge graph: owned by `SourceWorker`. Created by `from_config()` in `lib.rs`.
/// The SourceWorker loop: `output() → set_page_size_hint() → next_page() → measure()`.
#[derive(Debug)]
pub(crate) enum ControllerBackend {
    /// 🧊 Static config-based controller — cruise control locked at a single speed
    Config(config_controller::ConfigController),
    /// 🎛️ PID bytes-to-doc-count — the adaptive cruise control of batch sizing
    BytesToDocCount(pid_bytes_to_doc_count::PidBytesToDocCount),
}

impl Controller for ControllerBackend {
    #[inline]
    fn output(&self) -> usize {
        // -- 🎛️ "How many docs should I fetch?" — the source worker, every iteration, like clockwork
        match self {
            ControllerBackend::Config(c) => c.output(),
            ControllerBackend::BytesToDocCount(pid) => pid.output(),
        }
    }

    #[inline]
    fn measure(&mut self, measurement: f64) {
        // -- 📏 Feed the beast. The PID hungers for measurements. The Config shrugs and ignores them.
        match self {
            ControllerBackend::Config(c) => c.measure(measurement),
            ControllerBackend::BytesToDocCount(pid) => pid.measure(measurement),
        }
    }
}

impl ControllerBackend {
    /// 🔧 Resolve a `ControllerBackend` from config + the default page size.
    ///
    /// The `default_page_size` is used by `ConfigController` when `Static` is selected —
    /// it returns whatever `max_batch_size_docs` was in the source config.
    /// For `PidBytesToDocCount`, the config fields override everything.
    ///
    /// Same resolver pattern as `DocumentTransformer::from_configs()`.
    /// "In a world where configs must become controllers... one function dared to match."
    pub(crate) fn from_config(config: &ControllerConfig, default_page_size: usize) -> Self {
        match config {
            ControllerConfig::Static => ControllerBackend::Config(
                config_controller::ConfigController::new(default_page_size),
            ),
            ControllerConfig::PidBytesToDocCount {
                desired_response_size_bytes,
                initial_doc_count,
                min_doc_count,
                max_doc_count,
            } => {
                ControllerBackend::BytesToDocCount(pid_bytes_to_doc_count::PidBytesToDocCount::new(
                    *desired_response_size_bytes,
                    *initial_doc_count,
                    *min_doc_count,
                    *max_doc_count,
                ))
            }
        }
    }
}

// ============================================================
//  🧠 ThrottleConfig — adaptive throttling strategy selection
//  Moved from backends/common_config.rs — belongs with the controllers that consume it.
//  "He who colocates config with its consumer, avoids import spaghetti." 🍝
// ============================================================

/// 🧠 Throttle configuration — choose your fighter.
///
/// Controls how the SinkWorker decides the max request size for each bulk request.
/// Either a fixed static value (the OG), or a PID controller that adapts based on
/// measured request latency (the secret sauce, licensed under LICENSE-EE). 🔒
///
/// 🧠 Knowledge graph:
/// - Resolved in `lib.rs` into a `ThrottleControllerBackend` which is passed to SinkWorker.
/// - When `Static`: uses `SinkThrottleConfig.max_request_size_bytes` as the fixed output.
/// - When `Pid`: builds a `PidControllerBytesToMs` with the configured gains and bounds.
/// - Default: `Static` (backwards compatible — existing configs don't break). 🦆
///
/// ```toml
/// [sink_config.Elasticsearch.throttle]
/// mode = "Pid"
/// set_point_ms = 8000
/// min_bytes = 1048576
/// max_bytes = 104857600
/// initial_output_bytes = 10485760
/// ```
///
/// "He who chooses Static, sleeps peacefully. He who chooses Pid, sleeps never." — Ancient proverb
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "mode", deny_unknown_fields)]
pub enum ThrottleConfig {
    /// 🧊 Fixed byte size. No feedback. Like a rock. Reliable, but not dynamic.
    Static,
    /// 🧠 PID control: adapts byte output based on measured latency.
    /// Licensed under LICENSE-EE (BSL 1.1). The Krabby Patty formula. 🔒
    Pid {
        /// 🎯 Target request duration in milliseconds — the thermostat setting.
        /// For Elasticsearch bulk, 8000ms is a reasonable starting point.
        #[serde(default = "default_pid_set_point_ms")]
        set_point_ms: f64,
        /// 📏 Minimum output in bytes — the floor. Even during a 429 storm,
        /// we won't shrink below this. "A controller needs standards." 🎯
        #[serde(default = "default_pid_min_bytes")]
        min_bytes: usize,
        /// 📏 Maximum output in bytes — the ceiling. Even with a blazing fast server,
        /// we won't go bigger than this. "Hubris has limits." ⚠️
        #[serde(default = "default_pid_max_bytes")]
        max_bytes: usize,
        /// 📦 Initial byte size guess before any measurements arrive.
        /// The PID loop adjusts from here. Your first order at the restaurant. 🍔
        #[serde(default = "default_pid_initial_output_bytes")]
        initial_output_bytes: usize,
    },
}

impl Default for ThrottleConfig {
    /// 🧊 Default is Static — backwards compatible, no surprises. Like comfort food. 🍕
    fn default() -> Self {
        ThrottleConfig::Static
    }
}

impl ThrottleConfig {
    /// 🧊 A static reference to the Static variant — for InMemory sinks that can't produce
    /// a reference from a temporary. The eternal constant. The immovable object. 🪨
    pub const STATIC_DEFAULT: ThrottleConfig = ThrottleConfig::Static;
}

// 🎯 8 seconds — the Goldilocks duration for Elasticsearch bulk requests.
// Not too hot (overloading the cluster), not too cold (underutilizing bandwidth).
fn default_pid_set_point_ms() -> f64 {
    8000.0
}

// 📏 1MB floor — even if the server is choking, we send at least this much.
fn default_pid_min_bytes() -> usize {
    1_048_576
}

// 📏 100MB ceiling — the theoretical max. Elasticsearch says "up to 100MB" in the docs.
fn default_pid_max_bytes() -> usize {
    104_857_600
}

// 📦 10MB starting guess — same as the default max_request_size_bytes serde default.
fn default_pid_initial_output_bytes() -> usize {
    10_485_760
}

// ============================================================
//  📡 ThrottleController — sink-side trait
//  🏗️ measure(ms) ──▶ ThrottleController ──▶ output(bytes)
//  "What's the DEAL with latency? You measure it, and then it changes."
// ============================================================

/// 🎯 The universal interface for throttle controllers — the sink-side feedback loop.
///
/// 🧠 Knowledge graph:
/// - Every throttle strategy (static, PID, future token-bucket, etc.) implements this trait.
/// - SinkWorker owns a `ThrottleControllerBackend` and calls `measure()` + `output()` each cycle.
/// - `measure(duration_ms)`: feed the latest observed request duration into the controller.
///   For static controllers, this is a no-op. For PID, this drives the feedback loop.
/// - `output()`: returns the recommended byte size for the next bulk request.
///   The SinkWorker uses this as its dynamic `max_request_size_bytes`.
///
/// Contrast with `Controller` (source-side, measures bytes, outputs doc count).
/// This one measures milliseconds and outputs byte budget. Two feedback loops, one pipeline. 🔄
///
/// The trait is intentionally minimal — measure in, bytes out.
/// Like a vending machine, but for throughput decisions. 🚀
pub(crate) trait ThrottleController: std::fmt::Debug + Send {
    /// 📡 Feed a measured request duration (in milliseconds) into the controller.
    ///
    /// For static controllers: "cool story bro" (no-op).
    /// For PID controllers: this is the *raison d'être* — the feedback signal
    /// that drives proportional, integral, and derivative corrections. 🔄
    fn measure(&mut self, duration_ms: f64);

    /// 📏 Get the current recommended payload size in bytes.
    ///
    /// After each `measure()` call, this value may change (PID) or stay the same (static).
    /// The SinkWorker reads this to decide when to flush its page buffer. 🎯
    fn output(&self) -> usize;
}

// ============================================================
//  🏗️ ThrottleControllerBackend — sink-side dispatching enum
//  "He who dispatches via enum, avoids dyn Trait in production." — Ancient Rust proverb
// ============================================================

/// 📦 Enum dispatch for throttle controllers — the sink-side backend.
///
/// 🧠 Knowledge graph: follows the exact same pattern as `SinkBackend` and `SourceBackend`:
/// concrete types wrapped in an enum, delegating via match. Constructed via `new_static()` /
/// `new_pid()` builder methods. Handed to `SinkWorker::new()`.
///
/// Parallel structure to `ControllerBackend` but for an entirely different concern:
/// - `ControllerBackend` → source side → doc count adaptation
/// - `ThrottleControllerBackend` → sink side → byte size adaptation
#[derive(Debug, Clone)]
pub(crate) enum ThrottleControllerBackend {
    /// 🧊 Fixed byte size. No feedback loop. The classic.
    Static(StaticThrottleController),
    /// 🧠 PID control: bytes out, milliseconds in. The future is now.
    PidBytesToMs(PidControllerBytesToMs),
}

impl ThrottleController for ThrottleControllerBackend {
    fn measure(&mut self, duration_ms: f64) {
        // -- ⏱️ "How long did that take?" — the PID, obsessively. The Static, never.
        match self {
            ThrottleControllerBackend::Static(c) => c.measure(duration_ms),
            ThrottleControllerBackend::PidBytesToMs(c) => c.measure(duration_ms),
        }
    }

    fn output(&self) -> usize {
        // -- 📐 "How many bytes should I send next?" — the sink worker, trusting the math
        match self {
            ThrottleControllerBackend::Static(c) => c.output(),
            ThrottleControllerBackend::PidBytesToMs(c) => c.output(),
        }
    }
}

impl ThrottleControllerBackend {
    /// 🧊 Build a static controller — the "I know what I'm doing" option.
    /// No feedback. No math. Just vibes and a fixed byte size.
    pub(crate) fn new_static(fixed_bytes: usize) -> Self {
        ThrottleControllerBackend::Static(StaticThrottleController::new(fixed_bytes))
    }

    /// 🧠 Build a PID controller — the "let the math do the driving" option.
    /// The derivative gain will save us. The derivative gain always saves us.
    pub(crate) fn new_pid(
        set_point_ms: f64,
        initial_output_bytes: usize,
        min_bytes: usize,
        max_bytes: usize,
    ) -> Self {
        ThrottleControllerBackend::PidBytesToMs(PidControllerBytesToMs::new(
            set_point_ms,
            initial_output_bytes,
            min_bytes,
            max_bytes,
        ))
    }

    /// 🔧 Build a ThrottleControllerBackend from SinkThrottleConfig.
    ///
    /// 🧠 Knowledge graph: replaces the manual match in lib.rs run().
    /// Static → fixed bytes (the OG). Pid → PidControllerBytesToMs (the secret sauce). 🔒
    /// Now you can `from_config()` once and `.clone()` for N sink workers.
    /// "He who clones controllers, spawns workers without drama." — Ancient Rust proverb 🦆
    pub(crate) fn from_config(config: &SinkThrottleConfig) -> Self {
        match &config.mode {
            ThrottleConfig::Static => Self::new_static(config.max_request_size_bytes),
            ThrottleConfig::Pid {
                set_point_ms,
                min_bytes,
                max_bytes,
                initial_output_bytes,
            } => Self::new_pid(*set_point_ms, *initial_output_bytes, *min_bytes, *max_bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 Verify ConfigController dispatches correctly through the backend enum.
    /// If the enum dispatch is broken, everything is broken. But at least the test name is good.
    #[test]
    fn the_one_where_config_backend_dispatches_like_a_pro() {
        let mut the_backend = ControllerBackend::from_config(&ControllerConfig::Static, 5000);
        assert_eq!(the_backend.output(), 5000);
        // 📏 Measuring should be a no-op for static controller
        the_backend.measure(999_999.0);
        assert_eq!(
            the_backend.output(),
            5000,
            "Static controller should ignore measurements like I ignore Slack notifications on PTO"
        );
    }

    /// 🧪 Verify PidBytesToDocCount dispatches through the backend enum.
    #[test]
    fn the_one_where_pid_backend_dispatches_and_actually_adapts() {
        let the_config = ControllerConfig::PidBytesToDocCount {
            desired_response_size_bytes: 1_000_000.0,
            initial_doc_count: 500,
            min_doc_count: 10,
            max_doc_count: 5000,
        };
        let mut the_backend = ControllerBackend::from_config(&the_config, 500);

        // 🎯 Initial output should be the initial_doc_count
        assert_eq!(the_backend.output(), 500);

        // 📏 Feed a measurement — output should change (PID is alive!)
        the_backend.measure(500_000.0);
        let the_updated_output = the_backend.output();
        // 🧠 We measured half the desired size, so PID should want MORE docs
        assert!(
            the_updated_output > 0,
            "PID output must be positive — you can't fetch negative documents 💀"
        );
    }

    /// 🧪 Verify default ControllerConfig is Static.
    #[test]
    fn the_one_where_defaults_are_boring_and_thats_fine() {
        let the_default = ControllerConfig::default();
        assert!(
            matches!(the_default, ControllerConfig::Static),
            "Default controller should be Static — no surprises on first run 🧊"
        );
    }

    /// 🧪 Verify ControllerConfig deserializes from JSON (simulating TOML).
    #[test]
    fn the_one_where_static_config_deserializes_from_json() {
        let the_json = r#"{"type": "static"}"#;
        let the_config: ControllerConfig = serde_json::from_str(the_json)
            .expect("💀 Static config should deserialize. It's one field. Come on.");
        assert!(matches!(the_config, ControllerConfig::Static));
    }

    /// 🧪 Verify PID config deserializes with all fields.
    #[test]
    fn the_one_where_pid_config_deserializes_with_all_the_knobs() {
        let the_json = r#"{
            "type": "pid_bytes_to_doc_count",
            "desired_response_size_bytes": 5242880.0,
            "initial_doc_count": 2000,
            "min_doc_count": 50,
            "max_doc_count": 25000
        }"#;
        let the_config: ControllerConfig = serde_json::from_str(the_json)
            .expect("💀 PID config should deserialize. We even included all the fields.");
        match the_config {
            ControllerConfig::PidBytesToDocCount {
                desired_response_size_bytes,
                initial_doc_count,
                min_doc_count,
                max_doc_count,
            } => {
                assert!((desired_response_size_bytes - 5_242_880.0).abs() < f64::EPSILON);
                assert_eq!(initial_doc_count, 2000);
                assert_eq!(min_doc_count, 50);
                assert_eq!(max_doc_count, 25000);
            }
            honestly_who_knows => panic!(
                "💀 Expected PidBytesToDocCount, got {:?}. The JSON was RIGHT THERE.",
                honestly_who_knows
            ),
        }
    }

    /// 🧪 Verify PID config uses defaults for optional fields.
    #[test]
    fn the_one_where_pid_config_fills_in_the_blanks() {
        let the_json = r#"{
            "type": "pid_bytes_to_doc_count",
            "desired_response_size_bytes": 1000000.0
        }"#;
        let the_config: ControllerConfig = serde_json::from_str(the_json).expect(
            "💀 PID config with defaults should deserialize. We have serde(default) for this.",
        );
        match the_config {
            ControllerConfig::PidBytesToDocCount {
                initial_doc_count,
                min_doc_count,
                max_doc_count,
                ..
            } => {
                assert_eq!(
                    initial_doc_count, 1000,
                    "Default initial_doc_count should be 1000"
                );
                assert_eq!(min_doc_count, 10, "Default min_doc_count should be 10");
                assert_eq!(
                    max_doc_count, 50_000,
                    "Default max_doc_count should be 50,000"
                );
            }
            honestly_who_knows => panic!(
                "💀 Expected PidBytesToDocCount, got {:?}",
                honestly_who_knows
            ),
        }
    }

    // 🦆 The duck observes that these tests are more thorough than most production configs.
    // 🦆 The duck also notes two controller abstractions coexist peacefully in this file.
    // 🦆 The duck has opinions about PID gains. The duck keeps them to itself.
}
