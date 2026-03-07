// ai
//! 🎬 *[INT. CONTROL TOWER — DAWN BREAKS]*
//! *[Below, the pipeline roars. Data flows like a river. But rivers flood.]*
//! *[The regulators stir. They exist for this. To hold the line.]*
//! *["Not too fast," they murmur. "Not too slow. Just right."]*  🔧📡🦆
//!
//! 📦 Regulators — the throttle control layer between pipeline velocity and cluster health.
//!
//! 🧠 Knowledge graph:
//! ```text
//! PressureGauge (tokio, reads _nodes/stats/os every Ns)
//!   → Regulator.regulate(cpu_reading, dt_ms) → new flow rate (bytes)
//!     → FlowKnob: Arc<AtomicUsize> (effective max_request_size_bytes)
//!       → Joiner reads flow knob on every flush check
//! ```
//!
//! - `Regulate` trait: `fn regulate(&mut self, reading: f64, dt_ms: f64) -> f64`
//! - `Regulators` enum: dispatches to CpuPressure (PID) or ByteValue (static)
//! - `RegulatorConfig`: serde struct for TOML `[regulator]` section
//! - `PressureGauge`: background tokio task that reads node stats and adjusts FlowKnob
//!
//! ⚠️ The singularity will self-regulate. We're just practicing.

pub mod config;
pub mod cpu_pressure;
pub mod pressure_gauge;
pub mod static_regulator;

use std::time::Duration;

pub use config::CpuRegulatorConfig;
pub use config::StaticRegulatorConfig;
pub use config::LatencyRegulatorConfig;
pub use cpu_pressure::CpuPressure;
pub use pressure_gauge::{FlowKnob, SinkAuth, spawn_pressure_gauge};
pub use static_regulator::ByteValue;

use crate::GaugeReading;

// ============================================================
// 🎛️ Regulate trait — the contract for all regulators
// ============================================================

/// 🔄 The Regulate trait — it needs to regulate itself hehehe.
///
/// Takes a pressure reading and time delta, returns an adjusted output.
/// Like a thermostat, but for bytes. And the house is on fire. 🔥
pub trait Regulate {
    /// 🔄 Feed a reading, get an adjusted output.
    /// - `reading`: the measured value (CPU %, latency ms, whatever)
    /// - `since_last_checked_ms`: time since last call in milliseconds
    /// - Returns: new output value (bytes, typically)
    fn regulate(&mut self, reading: GaugeReading, since_last_checked_ms: Duration) -> f64;
}

// ============================================================
// 🎭 Regulators enum — the dispatcher
// ============================================================

/// 🎭 Regulators — enum dispatcher for concrete regulator implementations.
///
/// Like a union of thermostats: one does PID, one returns a constant.
/// Knock knock. *Who's there?* Match arm. *Match arm wh—*
/// `Regulators::Static(v) => v.regulate(r, dt)` 🚪
#[derive(Debug, Clone)]
pub enum Regulators {
    /// 📏 Fixed value — no regulation, just vibes
    Static(ByteValue),
    /// 🎛️ PID-controlled CPU pressure regulation — the real deal
    CpuPressure(CpuPressure),
}

impl Regulators {
    /// 🏗️ Create a Regulators instance from config.
    /// Always creates a CpuPressure PID controller — if you wanted static,
    /// you wouldn't have a `[regulator]` section in your config. 🧠
    ///
    /// 📏 `sink_max_request_size_bytes` is the hard ceiling from the sink config.
    /// The PID uses this as its max output — no disagreement between regulator and sink.
    /// Single source of truth for the ceiling. The floor comes from RegulatorConfig. 🎚️
    pub fn from_config(config: &CpuRegulatorConfig, sink_max_request_size_bytes: usize) -> Self {
        Regulators::CpuPressure(CpuPressure::new(
            config.target_cpu,
            config.min_request_size_bytes as f64,
            sink_max_request_size_bytes as f64,
            config.initial_output_bytes as f64,
        ))
    }
}

impl Regulate for Regulators {
    fn regulate(&mut self, reading: GaugeReading, since_last_checked_ms: Duration) -> f64 {
        match self {
            Regulators::Static(the_byte_value) => the_byte_value.regulate(reading, since_last_checked_ms),
            Regulators::CpuPressure(the_pid) => the_pid.regulate(reading, since_last_checked_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 The one where the enum dispatches to the right regulator.
    /// Pattern matching: the least dramatic form of decision-making in Rust. 🎭
    #[test]
    fn the_one_where_enum_dispatch_actually_dispatches() {
        // 📏 Static variant — should return fixed value
        let mut the_static = Regulators::Static(ByteValue::new(42.0));
        assert_eq!(the_static.regulate(GaugeReading::CpuValue(999), Duration::from_millis(1000)), 42.0, "🎯 Static should return 42 regardless");

        // 🎛️ CpuPressure variant — should return something different from initial after regulation
        let mut the_pid = Regulators::CpuPressure(CpuPressure::new(75.0, 100.0, 1_000_000.0, 500_000.0));
        let the_first_output = the_pid.regulate(GaugeReading::CpuValue(50), Duration::from_millis(3000));
        assert!(the_first_output > 0.0, "🎯 PID should return a positive value — got {}", the_first_output);
    }

    /// 🧪 The one where from_config creates a PID controller.
    /// Because if you wrote a [regulator] section, you meant business. 🦆
    #[test]
    fn the_one_where_from_config_creates_pid() {
        let the_config = CpuRegulatorConfig {
            target_cpu: 80.0,
            poll_interval_secs: 5,
            min_request_size_bytes: 65_536,
            initial_output_bytes: 2_097_152,
        };

        // 📏 Sink max passed in separately — single source of truth for the ceiling 🎚️
        let mut the_regulator = Regulators::from_config(&the_config, 33_554_432);
        let the_output = the_regulator.regulate(GaugeReading::CpuValue(60), Duration::from_millis(5000));
        assert!(the_output > 0.0, "🎯 from_config regulator should produce positive output");
    }

    /// 🧪 The one where RegulatorConfig deserializes from TOML.
    /// If this fails, serde and figment are having a disagreement. Mediation required. 🧑‍⚖️
    #[test]
    fn the_one_where_regulator_config_deserializes() {
        let the_toml = r#"
            target_cpu = 72.5
            poll_interval_secs = 5
            min_request_size_bytes = 65536
            initial_output_bytes = 2097152
        "#;

        let the_config: CpuRegulatorConfig = toml::from_str(the_toml)
            .expect("💀 RegulatorConfig should deserialize from TOML — serde had one job");

        assert!((the_config.target_cpu - 72.5).abs() < f64::EPSILON, "🎯 target_cpu should be 72.5");
        assert_eq!(the_config.poll_interval_secs, 5);
        assert_eq!(the_config.min_request_size_bytes, 65_536);
        assert_eq!(the_config.initial_output_bytes, 2_097_152);
    }

    /// 🧪 The one where RegulatorConfig defaults are sane.
    /// Defaults: the safety net for developers who forget to write config. 🪢
    #[test]
    fn the_one_where_defaults_are_not_insane() {
        let the_toml = "";
        let the_config: CpuRegulatorConfig = toml::from_str(the_toml)
            .expect("💀 Empty TOML should use defaults — that's literally the point");

        assert!((the_config.target_cpu - 75.0).abs() < f64::EPSILON, "🎯 Default target CPU is 75%");
        assert_eq!(the_config.poll_interval_secs, 3, "🎯 Default poll interval is 3s");
        assert_eq!(the_config.min_request_size_bytes, 128 * 1024, "🎯 Default min is 128 KiB");
        assert_eq!(the_config.initial_output_bytes, 4 * 1024 * 1024, "🎯 Default initial is 4 MiB");
    }
}
