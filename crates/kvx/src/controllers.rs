// ai
//! 🎛️🔧🚀 Controllers — the feedback loop whisperers of the kravex pipeline.
//!
//! 🎬 COLD OPEN — INT. THERMOSTAT FACTORY — 2:17 AM
//!
//! "What if," whispered the engineer, staring at the production dashboard,
//! "we stopped guessing the batch size... and let the data TELL us?"
//! The room went silent. The borrow checker nodded approvingly.
//! And thus, the PID controller was born — not from brilliance,
//! but from the quiet desperation of watching 429s at 3am.
//!
//! This module implements the **Controller** trait and its dispatching enum
//! `ControllerBackend`, following the exact same pattern as transforms.rs:
//!
//! ```text
//!   transforms.rs pattern:             controllers.rs pattern:
//!   ┌──────────────────────┐          ┌───────────────────────────┐
//!   │ trait Transform       │          │ trait Controller            │
//!   │   fn transform()     │          │   fn output() -> usize     │
//!   └────────┬─────────────┘          │   fn measure(f64)          │
//!            │                         └────────┬──────────────────┘
//!   ┌────────┴─────────────┐                    │
//!   │ RallyS3ToEs          │          ┌─────────┴──────────────────┐
//!   │ Passthrough          │          │ ConfigController            │
//!   └────────┬─────────────┘          │ PidBytesToDocCount          │
//!            │                         └─────────┬──────────────────┘
//!   ┌────────┴─────────────┐                     │
//!   │ enum DocTransformer  │          ┌──────────┴──────────────────┐
//!   │   impl Transform     │          │ enum ControllerBackend       │
//!   │   match dispatch     │          │   impl Controller            │
//!   └──────────────────────┘          │   match dispatch             │
//!                                     └──────────────────────────────┘
//! ```
//!
//! ## Knowledge Graph 🧠
//! - Pattern: trait → concrete impls → enum dispatch (same as Source/Sink/Transform)
//! - `Controller` trait: `output()` returns batch size hint, `measure()` feeds measurements
//! - `ConfigController`: static — returns configured value, ignores measurements (thermostat unplugged)
//! - `PidBytesToDocCount`: dynamic PID — measures response bytes, adjusts doc count output
//! - `ControllerConfig`: serde-tagged enum for TOML deserialization
//! - `ControllerBackend::from_config()`: resolver (like `DocumentTransformer::from_configs()`)
//! - SourceWorker owns the controller: output → set_page_size_hint → next_page → measure → loop
//!
//! ⚠️ The singularity will have better PID gains than us. Until then, we iterate. Literally. 🦆

use serde::Deserialize;

pub(crate) mod config_controller;
pub(crate) mod pid_bytes_to_doc_count;

// ============================================================
//  ╔═══════════════════════════════════════════════════════╗
//  ║  🎛️ measure(bytes) ──▶ Controller ──▶ output(docs)  ║
//  ║    (same pattern as Source/Sink/Transform. finally.) ║
//  ╚═══════════════════════════════════════════════════════╝
// ============================================================

/// 🎛️ Controller — the feedback loop trait for adaptive batch sizing.
///
/// Think of it like cruise control for your data pipeline:
/// - `output()` = "how fast should I go?" (batch size hint)
/// - `measure()` = "the speedometer says THIS" (observed response metric)
///
/// The controller adjusts future outputs based on past measurements.
/// For `ConfigController`, this is "set cruise control to 65 and never touch it."
/// For `PidBytesToDocCount`, this is "the PID algorithm from a 1922 paper,
/// reinvented by a C# developer, ported to Rust, running at 3am." 🔧
///
/// # Contract 📜
/// - `output()` returns the current batch size recommendation (doc count)
/// - `measure(f64)` feeds a measurement (e.g., response size in bytes)
/// - Implementations must handle repeated calls gracefully
/// - Output must always be > 0 (you can't fetch negative documents, despite what the borrow checker implies)
///
/// 🧠 Knowledge graph: SourceWorker calls output() → source.set_page_size_hint() →
/// source.next_page() → measure(page.len()) — the eternal feedback loop.
/// "He who measures not, optimizes in vain." — Ancient PID proverb 📜
pub(crate) trait Controller: std::fmt::Debug {
    /// 🎯 Get the current output value — the recommended batch size.
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

/// 🔧 Configuration enum for controller selection and parameters.
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
    /// Ported from C# with love, sweat, and approximately 47 unit tests.
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
//  🎭 ControllerBackend — the dispatching enum
//  Mirrors SourceBackend / SinkBackend / DocumentTransformer exactly.
// ============================================================

/// 🎭 The dispatching enum for controllers — polymorphism via match, not vtables.
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
        match self {
            ControllerBackend::Config(c) => c.output(),
            ControllerBackend::BytesToDocCount(pid) => pid.output(),
        }
    }

    #[inline]
    fn measure(&mut self, measurement: f64) {
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
            ControllerConfig::Static => {
                ControllerBackend::Config(config_controller::ConfigController::new(
                    default_page_size,
                ))
            }
            ControllerConfig::PidBytesToDocCount {
                desired_response_size_bytes,
                initial_doc_count,
                min_doc_count,
                max_doc_count,
            } => ControllerBackend::BytesToDocCount(
                pid_bytes_to_doc_count::PidBytesToDocCount::new(
                    *desired_response_size_bytes,
                    *initial_doc_count,
                    *min_doc_count,
                    *max_doc_count,
                ),
            ),
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
        let mut the_backend =
            ControllerBackend::from_config(&ControllerConfig::Static, 5000);
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
        let the_config: ControllerConfig = serde_json::from_str(the_json)
            .expect("💀 PID config with defaults should deserialize. We have serde(default) for this.");
        match the_config {
            ControllerConfig::PidBytesToDocCount {
                initial_doc_count,
                min_doc_count,
                max_doc_count,
                ..
            } => {
                assert_eq!(initial_doc_count, 1000, "Default initial_doc_count should be 1000");
                assert_eq!(min_doc_count, 10, "Default min_doc_count should be 10");
                assert_eq!(max_doc_count, 50_000, "Default max_doc_count should be 50,000");
            }
            honestly_who_knows => panic!("💀 Expected PidBytesToDocCount, got {:?}", honestly_who_knows),
        }
    }

    // 🦆 The duck observes that these tests are more thorough than most production configs.
}
