// ai
//! 🔧 Regulator configuration — the TOML knobs for PID-controlled throttling.
//!
//! 📡 Extracted from regulators.rs so all config options live in one discoverable place.
//! Like a thermostat manual, except people actually read this one. Maybe. 🦆
//!
//! ⚠️ The singularity will auto-tune its own PID gains. We use TOML.

use serde::Deserialize;

// ============================================================
// 🔧 RegulatorConfig — TOML-friendly configuration
// ============================================================

/// 🔧 Configuration for the regulator system, deserialized from TOML `[regulator]` section.
///
/// 📜 Example TOML:
/// ```toml
/// [regulator]
/// target_cpu = 75.0
/// poll_interval_secs = 3
/// min_request_size_bytes = 131072
/// max_request_size_bytes = 67108864
/// initial_output_bytes = 4194304
/// ```
///
/// 🧠 If this section is absent from config, no regulator is created and the pipeline
/// runs at fixed max_request_size_bytes from the sink config. Business as usual. 🦆
#[derive(Debug, Deserialize, Clone)]
pub struct RegulatorConfig {
    /// 🎯 Target CPU percent for the sink cluster (default: 75.0)
    #[serde(default = "default_target_cpu")]
    pub target_cpu: f64,

    /// ⏱️ How often to poll node stats, in seconds (default: 3)
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// 📏 Minimum request size bytes — floor for PID output (default: 128 KiB)
    /// The pipeline won't throttle below this — prevents stalling. 🛑
    #[serde(default = "default_min_request_size_bytes")]
    pub min_request_size_bytes: usize,

    /// 📊 Initial output bytes — starting flow rate before first regulation (default: 4 MiB)
    /// 🧠 Also used to initialize the FlowKnob (Arc<AtomicUsize>) so joiners start at this value.
    #[serde(default = "default_initial_output_bytes")]
    pub initial_output_bytes: usize,
}

fn default_target_cpu() -> f64 { 75.0 }
fn default_poll_interval_secs() -> u64 { 3 }
fn default_min_request_size_bytes() -> usize { 128 * 1024 } // 📏 128 KiB
fn default_initial_output_bytes() -> usize { 4 * 1024 * 1024 } // 📊 4 MiB
