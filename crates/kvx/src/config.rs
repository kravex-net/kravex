//! 🔧 App Configuration — the sacred TOML-to-struct pipeline.
//!
//! 📡 "Config not found: We looked everywhere. Under the couch. Behind the fridge.
//! In the junk drawer. Nothing." — every developer at 3am 🦆
//!
//! 🏗️ Powered by Figment, because manually parsing env vars is a form of
//! self-harm that even the borrow checker wouldn't approve of.

use anyhow::Context;
use crate::regulators::CpuRegulatorConfig;
use crate::workers::DrainerConfig;
use crate::workers::FlowMasterConfig;
use serde::Deserialize;
// -- 🔧 To load the configuration, so I don't have to manually parse
// -- environment variables or files. Bleh. Like doing taxes but for bytes.
pub use crate::backends::{SinkConfig, SourceConfig};
use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use std::path::Path;
// -- 🚀 tracing::info — because println! in production is a cry for help.
// -- "I used to use println! for debugging... but then I got help." — anonymous dev, 2 kids, 1 wife, 1 mortgage
use tracing::info;

// ============================================================
// 🔧 RuntimeConfig — the knobs we admit in public
// ============================================================

/// ⚙️ Runtime configuration — how fast do we go, how many workers do we spawn?
///
/// 🧠 Knowledge graph: former resident of `supervisors/config.rs`. Evicted to `app_config`
/// because it's application-level config, not supervisor internals. The supervisor is a
/// *consumer* of this config, not its *owner*. Separation of concerns, baby. 🏗️
///
/// 🎯 Defaults: queue capacity 10, sink parallelism 1 — conservative enough to not
/// immediately explode on first run, ambitious enough to migrate actual data. 🦆
#[derive(Debug, Deserialize, Clone)]
pub struct RuntimeConfig {
    /// 📬 Bounded channel capacity for ch1 (pumper → joiners) — raw feeds in transit 🚚
    #[serde(default = "default_pumper_to_joiner_capacity", alias = "channel_size", alias = "queue_capacity")]
    pub pumper_to_joiner_capacity: usize,
    /// 📬 Bounded channel capacity for ch2 (joiners → drainers) — assembled payloads in transit 🚛
    /// Separate from pumper_to_joiner_capacity because payloads are larger than raw feeds —
    /// think of ch1 as the loading dock and ch2 as the dispatch bay 🏗️
    // The byte size of this effectively becomes source max bytes * this capacity
    #[serde(default = "default_joiner_to_drainer_capacity", alias = "payload_channel_capacity")]
    pub joiner_to_drainer_capacity: usize,
    /// 🧵 How many sink workers run in parallel — more lanes, more throughput, more debugging
    #[serde(default = "default_sink_parallelism", alias = "num_sink_workers")]
    pub sink_parallelism: usize,
    /// 🧵 How many joiner threads to spawn for CPU-bound casting+joining work.
    /// Defaults to (cpu_count - 1, minimum 1) because we're generous enough to leave
    /// one core for the OS, the async runtime, and whatever else wants to live. 🦆
    #[serde(default = "default_joiner_parallelism", alias = "num_joiner_workers")]
    pub joiner_parallelism: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            pumper_to_joiner_capacity: default_pumper_to_joiner_capacity(),
            joiner_to_drainer_capacity: default_joiner_to_drainer_capacity(),
            sink_parallelism: default_sink_parallelism(),
            joiner_parallelism: default_joiner_parallelism(),
        }
    }
}

// 🔢 10: chosen by rolling a d20, getting a 10, and calling it "load tested".
// -- The queue holds batches, not feelings, though both can become backpressure if ignored. 🦆
fn default_pumper_to_joiner_capacity() -> usize {
    default_parallelism()
}

// 🧵 One sink lane by default: fewer moving parts, fewer ways to invent folklore during debugging.
// -- Ancient proverb: he who spawns eight writers before breakfast, debugs until dinner.
fn default_sink_parallelism() -> usize {
    default_parallelism() * 3
}

// 📬 Payload channel (ch2, joiners → drainers): same default as ch1. Assembled payloads are
// chunkier than raw feeds, so a smaller buffer is fine. Like a VIP line at the club — fewer
// people, more velvet rope per capita. 🦆
fn default_joiner_to_drainer_capacity() -> usize {
    default_parallelism()
}

fn default_parallelism() -> usize {
    default_joiner_parallelism() * 4
}

// 🧵 Joiner threads: cpu_count - 1, because leaving one core for the OS and tokio is the
// polite thing to do. Like leaving one slice of pizza for the next person.
// Nobody does it, but we pretend we would. Minimum 1 because 0 joiners means 0 progress
// and that's called a government agency.
fn default_joiner_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(1)
}

/// 📦 The AppConfig: one struct to rule them all, one struct to find them,
/// one struct to bring them all, and in the Figment bind them.
///
/// 🎯 Contains everything the app needs to know about itself,
/// which is more self-awareness than most apps achieve in their lifetime.
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// 📡 How shall the source workers behave? Configurable, unlike my children.
    pub source_config: SourceConfig,
    pub sink_config: SinkConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    /// 🔬 Optional regulator config — if present, spawns a PID-controlled pressure gauge
    /// that dynamically adjusts payload size based on sink cluster CPU pressure.
    /// If absent, pipeline runs at fixed max_request_size_bytes. Business as usual. 🎚️
    pub regulator: Option<CpuRegulatorConfig>,
    /// 🔄 Drainer retry config — exponential backoff knobs for when the sink says "not now".
    /// Defaults to 3 retries, 1s initial, 2x multiplier, 30s cap. Optional section in TOML. 🦆
    #[serde(default)]
    pub drainer: DrainerConfig,
    #[serde(default)]
    pub flow_master: FlowMasterConfig
}

/// 🚀 Load the config — from a file, from env vars, or from the sheer power of hoping.
///
/// 🔧 Merges environment variables (KVX_*) with an optional TOML file.
/// Notice: no `.only(...)` restriction — ALL KVX_ vars are fair game now.
/// We don't gatekeep env vars here. This is a safe space. 🦆
///
/// 📐 DESIGN NOTE (no cap, this is tribal knowledge):
///   - If `config_file_name` is None  → env vars only. No file. No assumptions. No pizza defaults.
///   - If `config_file_name` is Some  → env vars + TOML file, merged. TOML wins on conflicts.
///   Previously kravex always fell back to "config.toml" — like assuming everyone wants pineapple
///   on their pizza. We fixed that. ethos showed us the light.
///
/// 💀 Returns an error if config is unparseable. Which it will be. Check the error message though —
/// it's contextual, informative, and written with love. Or despair. Hard to tell at 3am.
pub fn load_config(config_file_name: Option<&Path>) -> anyhow::Result<AppConfig> {
    // -- 🚀 Log what we're loading — because silent failures are the villain origin story
    // -- of every 3am incident. "The config loaded fine." — famous last words.
    info!(
        "🔧 Loading configuration: {:#?}",
        config_file_name.unwrap_or(&Path::new(""))
    );

    // -- 🏗️ Start with env vars as the base layer — like a good sourdough starter.
    // -- ALL KVX_* vars accepted. No ID required. No velvet rope. Everyone's invited.
    let config = Figment::new().merge(Env::prefixed("KVX_"));

    // -- 🎯 Conditionally layer in TOML only if a file was actually provided.
    // -- No file? No problem. We trust the env. Like a golden retriever trusts everyone.
    // -- Ancient proverb: "He who defaults to config.toml uninvited, deploys to production alone."
    let config = match config_file_name {
        Some(file_name) => config.merge(Toml::file(file_name)),
        None => config,
    };

    // 💬 Build a context message that will actually TELL you what went wrong.
    // -- None of that "error: error" energy. This isn't a Kafka novel. (The author, not the queue.)
    let context_msg = match config_file_name {
        Some(path) => format!(
            "💀 Failed to parse configuration from file '{}' and environment variables (KVX_*). \
             The file exists in our hearts, but apparently not on disk.",
            path.display()
        ),
        None => "💀 Failed to parse configuration from environment variables (KVX_*). \
                 No file was provided — this one's all on the environment. Classic."
            .to_string(),
    };

    // -- ✅ or 💀, there is no try — actually there is, it's called `?`
    // -- TODO: win the lottery, retire, delete this crate
    config.extract().context(context_msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// 🧪 Write test TOML to a guaranteed-unique temp file via the `tempfile` crate.
    /// 🧠 Previously used nanosecond timestamps which collided when tests ran in parallel —
    /// two tests at the same nanosecond would write to the same file, and one would read
    /// the other's TOML content. Like two people writing different grocery lists on the
    /// same fridge whiteboard at the same time. Now each test gets its own file. 🧊🦆
    fn write_test_config(contents: &str) -> tempfile::TempPath {
        let the_temp_file = tempfile::Builder::new()
            .prefix("kvx_app_config_")
            .suffix(".toml")
            .tempfile()
            .expect("💀 Failed to create temp file. The OS said 'I'm full, try again never'.");

        fs::write(the_temp_file.path(), contents)
            .expect("💀 Failed to write test config. The filesystem said 'new phone who dis'.");

        // 📂 Return TempPath — file auto-deletes when TempPath drops. No manual cleanup needed.
        // Like a self-cleaning oven, but for config files. 🧹
        the_temp_file.into_temp_path()
    }

    #[test]
    fn the_one_where_runtime_knobs_move_into_their_own_apartment() {
        let config_path = write_test_config(
            r#"
            [runtime]
            pumper_to_joiner_capacity = 8
            sink_parallelism = 3

            [source_config.File]
            file_name = "input.json"

            [sink_config.File]
            file_name = "output.json"
            max_request_size_bytes = 123456
            "#,
        );

        let app_config = load_config(Some(&config_path)).expect(
            "💀 Runtime config should parse. The schema drift goblin does not get this win.",
        );

        assert_eq!(app_config.runtime.pumper_to_joiner_capacity, 8);
        assert_eq!(app_config.runtime.sink_parallelism, 3);
        match app_config.sink_config {
            SinkConfig::File(file_config) => {
                assert_eq!(file_config.common_config.max_request_size_bytes, 123456);
            }
            honestly_who_knows => panic!(
                "💀 Expected File sink config in the test, but serde took us to {:?}. Plot twist energy.",
                honestly_who_knows
            ),
        }

        // 🧹 TempPath auto-deletes on drop — no manual cleanup needed
    }

    #[test]
    fn the_one_where_runtime_defaults_show_up_uninvited_but_helpful() {
        let config_path = write_test_config(
            r#"
            [source_config.File]
            file_name = "input.json"

            [sink_config.File]
            file_name = "output.json"
            "#,
        );

        let app_config: AppConfig = Figment::new()
            .merge(Toml::file(&config_path))
            .extract()
            .expect("💀 Default runtime config should exist. Serde left us on read otherwise.");

        assert_eq!(app_config.runtime.pumper_to_joiner_capacity, RuntimeConfig::default().pumper_to_joiner_capacity);
        assert_eq!(app_config.runtime.sink_parallelism, RuntimeConfig::default().sink_parallelism);

        // 🧹 TempPath auto-deletes on drop — no manual cleanup needed
    }

    #[test]
    fn the_one_where_runtime_accepts_its_former_stage_names() {
        let config_path = write_test_config(
            r#"
            [runtime]
            channel_size = 12
            num_sink_workers = 4

            [source_config.File]
            file_name = "input.json"

            [sink_config.File]
            file_name = "output.json"
            "#,
        );

        let app_config = load_config(Some(&config_path))
            .expect("💀 Runtime aliases should parse. The witness protection paperwork was valid.");

        assert_eq!(app_config.runtime.pumper_to_joiner_capacity, 12);
        assert_eq!(app_config.runtime.sink_parallelism, 4);

        // 🧹 TempPath auto-deletes on drop — no manual cleanup needed
    }
}
