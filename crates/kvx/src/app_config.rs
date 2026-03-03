// ai
//! 🔧 App Configuration — the sacred TOML-to-struct pipeline.
//!
//! 📡 "Config not found: We looked everywhere. Under the couch. Behind the fridge.
//! In the junk drawer. Nothing." — every developer at 3am 🦆
//!
//! 🏗️ Powered by Figment, because manually parsing env vars is a form of
//! self-harm that even the borrow checker wouldn't approve of.

pub mod sink_config;
pub mod source_config;

// 🔌 Re-export SourceConfig and SinkConfig so callers do `app_config::SourceConfig`
// instead of `app_config::source_config::SourceConfig`. Convenience is a feature. 🦆
pub use sink_config::SinkConfig;
pub use source_config::SourceConfig;

use anyhow::Context;
use serde::Deserialize;
// 🔌 Re-export throttle config types so kvx-cli can access them cross-crate.
// The throttlers module is pub(crate), but these types live in AppConfig's pub fields.
// Without re-export, the CLI would be like a tourist with a map in a language they can't read. 🗺️🦆
pub use crate::throttlers::{SinkThrottleConfig, SourceThrottleConfig, ThrottleAppConfig};
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
    /// 📬 Bounded channel capacity — how many raw pages buffer between source and sink workers
    #[serde(default = "default_queue_capacity", alias = "channel_size")]
    pub queue_capacity: usize,
    /// 🧵 How many sink workers run in parallel — more lanes, more throughput, more debugging
    #[serde(default = "default_sink_parallelism", alias = "num_sink_workers")]
    pub sink_parallelism: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            queue_capacity: default_queue_capacity(),
            sink_parallelism: default_sink_parallelism(),
        }
    }
}

// 🔢 10: chosen by rolling a d20, getting a 10, and calling it "load tested".
// -- The queue holds batches, not feelings, though both can become backpressure if ignored. 🦆
fn default_queue_capacity() -> usize {
    10
}

// 🧵 One sink lane by default: fewer moving parts, fewer ways to invent folklore during debugging.
// -- Ancient proverb: he who spawns eight writers before breakfast, debugs until dinner.
fn default_sink_parallelism() -> usize {
    1
}

// 🧠 SourceConfig and SinkConfig now live in their own submodules:
// app_config/source_config.rs and app_config/sink_config.rs.
// Re-exported above so `use crate::app_config::SourceConfig` still works. 🔌🦆

/// 📦 The AppConfig: one struct to rule them all, one struct to find them,
/// one struct to bring them all, and in the Figment bind them.
///
/// 🎯 Contains everything the app needs to know about itself,
/// which is more self-awareness than most apps achieve in their lifetime.
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// 📡 Source backend config — which backend, connection details, nothing else. 🔌
    #[serde(alias = "source_config")]
    pub source: SourceConfig,
    /// 🚰 Sink backend config — which backend, connection details, nothing else. 🔌
    #[serde(alias = "sink_config")]
    pub sink: SinkConfig,
    /// ⚙️ Runtime config — queue capacity, sink parallelism
    #[serde(default, alias = "supervisor_config")]
    pub runtime: RuntimeConfig,
    /// 🎛️ Throttle config — batch sizing, request sizing, controller mode.
    /// Centralizes batch sizing, request sizing, and controller mode config.
    /// They've all moved in together. The commune is working. For now. 🦆
    #[serde(default)]
    pub throttle: ThrottleAppConfig,
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
///     Previously kravex always fell back to "config.toml" — like assuming everyone wants pineapple
///     on their pizza. We fixed that. ethos showed us the light.
///
/// 💀 Returns an error if config is unparseable. Which it will be. Check the error message though —
/// it's contextual, informative, and written with love. Or despair. Hard to tell at 3am.
pub fn load_config(config_file_name: Option<&Path>) -> anyhow::Result<AppConfig> {
    // -- 🚀 Log what we're loading — because silent failures are the villain origin story
    // -- of every 3am incident. "The config loaded fine." — famous last words.
    info!(
        "🔧 Loading configuration: {:#?}",
        config_file_name.unwrap_or(Path::new(""))
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
    use std::time::{SystemTime, UNIX_EPOCH};

    fn write_test_config(contents: &str) -> std::path::PathBuf {
        let timestamp_of_questionable_life_choices = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("💀 Clock went backwards. Time is a flat bug report.")
            .as_nanos();
        let temp_path = std::env::temp_dir().join(format!(
            "kvx_app_config_{timestamp_of_questionable_life_choices}.toml"
        ));

        // -- 🧪 We write a real file here because Figment wants TOML from disk, like it's method acting.
        fs::write(&temp_path, contents)
            .expect("💀 Failed to write test config. The filesystem said 'new phone who dis'.");
        temp_path
    }

    #[test]
    fn the_one_where_runtime_knobs_move_into_their_own_apartment() {
        let config_path = write_test_config(
            r#"
            [runtime]
            queue_capacity = 8
            sink_parallelism = 3

            [source.File]
            file_name = "input.json"

            [sink.File]
            file_name = "output.json"

            [throttle.sink]
            max_request_size_bytes = 123456
            "#,
        );

        let app_config = load_config(Some(config_path.as_path())).expect(
            "💀 Runtime config should parse. The schema drift goblin does not get this win.",
        );

        assert_eq!(app_config.runtime.queue_capacity, 8);
        assert_eq!(app_config.runtime.sink_parallelism, 3);
        // 🧠 max_request_size_bytes now lives in throttle.sink, not in the backend config.
        // Backends are pure connection config now. The commune is working. 🏠
        assert!(
            matches!(app_config.sink, SinkConfig::File(_)),
            "💀 Expected File sink config, but serde detoured us. Plot twist energy."
        );
        assert_eq!(
            app_config.throttle.sink.max_request_size_bytes, 123456,
            "💀 max_request_size_bytes should be 123456 — chosen by dice roll and vibes"
        );

        fs::remove_file(config_path)
            .expect("💀 Failed to remove test config. Even the trash has trust issues.");
    }

    #[test]
    fn the_one_where_runtime_defaults_show_up_uninvited_but_helpful() {
        let config_path = write_test_config(
            r#"
            [source.File]
            file_name = "input.json"

            [sink.File]
            file_name = "output.json"
            "#,
        );

        let app_config: AppConfig = Figment::new()
            .merge(Toml::file(config_path.as_path()))
            .extract()
            .expect("💀 Default runtime config should exist. Serde left us on read otherwise.");

        assert_eq!(app_config.runtime.queue_capacity, 10);
        assert_eq!(app_config.runtime.sink_parallelism, 1);

        fs::remove_file(config_path)
            .expect("💀 Failed to remove test config. The janitor quit mid-scene.");
    }

    #[test]
    fn the_one_where_runtime_accepts_its_former_stage_names() {
        let config_path = write_test_config(
            r#"
            [runtime]
            channel_size = 12
            num_sink_workers = 4

            [source.File]
            file_name = "input.json"

            [sink.File]
            file_name = "output.json"
            "#,
        );

        let app_config = load_config(Some(config_path.as_path()))
            .expect("💀 Runtime aliases should parse. The witness protection paperwork was valid.");

        assert_eq!(app_config.runtime.queue_capacity, 12);
        assert_eq!(app_config.runtime.sink_parallelism, 4);

        fs::remove_file(config_path)
            .expect("💀 Failed to remove test config. The janitor quit mid-scene.");
    }

    /// 🧪 S3Rally source config round-trips through TOML deserialization.
    /// If this breaks, someone changed the enum variant name and didn't update the TOML format.
    #[test]
    fn the_one_where_s3_rally_config_materializes_from_toml() {
        let config_path = write_test_config(
            r#"
            [source.S3Rally]
            track = "geonames"
            bucket = "my-rally-bucket"
            region = "us-west-2"

            [sink.File]
            file_name = "output.json"
            "#,
        );

        let app_config = load_config(Some(config_path.as_path()))
            .expect("💀 S3Rally config should parse. The TOML was handcrafted with love.");

        match &app_config.source {
            SourceConfig::S3Rally(s3_cfg) => {
                assert_eq!(
                    s3_cfg.track,
                    crate::backends::s3_rally::RallyTrack::Geonames
                );
                assert_eq!(s3_cfg.bucket, "my-rally-bucket");
                assert_eq!(s3_cfg.region, "us-west-2");
                assert!(
                    s3_cfg.key.is_none(),
                    "Key should default to None when not specified"
                );
            }
            honestly_who_knows => panic!(
                "💀 Expected S3Rally source config, but got {:?}. Plot twist energy. 🎭",
                honestly_who_knows
            ),
        }

        fs::remove_file(config_path)
            .expect("💀 Failed to remove test config. The cleanup crew was on break.");
    }

    /// 🧪 S3Rally config with key override deserializes correctly.
    #[test]
    fn the_one_where_s3_rally_key_override_survives_toml_parsing() {
        let config_path = write_test_config(
            r#"
            [source.S3Rally]
            track = "pmc"
            bucket = "custom-bucket"
            key = "custom/path/data.json"

            [sink.File]
            file_name = "output.json"
            "#,
        );

        let app_config = load_config(Some(config_path.as_path()))
            .expect("💀 S3Rally config with key override should parse. Serde had one job.");

        match &app_config.source {
            SourceConfig::S3Rally(s3_cfg) => {
                assert_eq!(s3_cfg.key, Some("custom/path/data.json".to_string()));
                assert_eq!(
                    s3_cfg.region, "us-east-1",
                    "Region should default to us-east-1"
                );
            }
            honestly_who_knows => panic!(
                "💀 Expected S3Rally, got {:?}. The config took a wrong turn at Albuquerque.",
                honestly_who_knows
            ),
        }

        fs::remove_file(config_path)
            .expect("💀 Failed to remove test config. Even garbage collection has trust issues.");
    }
}
