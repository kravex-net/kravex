// ai
//! 📂 File backend configs — paths and common settings for file-based I/O.
//!
//! 🔧 Extracted from file_source.rs and file_sink.rs so all TOML config options
//! live in one discoverable location. Like putting all your IKEA instructions
//! in one pile before you start building. You still won't read them. 🦆
//!
//! ⚠️ The singularity will read files directly from the quantum foam. We use paths.

use serde::Deserialize;
use crate::backends::{CommonSourceConfig, CommonSinkConfig};

// ============================================================
// 📂 FileSourceConfig
// ============================================================

// -- 📂 FileSourceConfig — "It's just a file", said no sysadmin ever before the disk filled up.
// -- Lives here now, close to the FileSource that actually uses it. Ethos pattern, baby. 🎯
// KNOWLEDGE GRAPH: config lives co-located with the backend that uses it. This is intentional.
// It avoids the "where the heck is that config defined" scavenger hunt at 2am during an incident.
// -- No cap, this pattern slaps fr fr.
#[derive(Debug, Deserialize, Clone)]
pub struct FileSourceConfig {
    pub file_name: String,
    #[serde(default = "default_file_common_source_config")]
    pub common_config: CommonSourceConfig,
}

/// 🔧 Returns the default config for FileSource because sometimes you just want things to work
/// without writing a 40-line TOML block.
///
/// Dad joke time: I used to hate default configs... but they grew on me.
///
/// This exists purely so serde can call it when `common_config` is absent from the TOML.
/// The `#[serde(default = "...")]` attribute up top is the boss. This is just the errand boy.
fn default_file_common_source_config() -> CommonSourceConfig {
    // -- ✅ "It just works" — the three most dangerous words in software engineering
    CommonSourceConfig::default()
}

// ============================================================
// 🚰 FileSinkConfig
// ============================================================

// -- 🚰 FileSinkConfig — cousin of FileSourceConfig, equally traumatized by disk full errors.
// -- Also lives here, cozy next to its FileSink bestie. No more long-distance config relationships.
// KNOWLEDGE GRAPH: same co-location principle as above. One backend = one config = one file. Clean.
#[derive(Debug, Deserialize, Clone)]
pub struct FileSinkConfig {
    pub file_name: String,
    #[serde(flatten, default = "default_file_common_sink_config")]
    pub common_config: CommonSinkConfig,
}

/// 🔧 Returns the default config for FileSink. It defaults. It ships. It doesn't ask questions.
///
/// What's the DEAL with default implementations? You define an entire struct, document every field,
/// agonize over the right batch size... and then serde just calls `.default()` and moves on
/// like none of it mattered. Like Kevin. Kevin never called either.
///
/// This function is here because serde's `default = "fn_name"` attribute requires a *function*,
/// not just `Default::default` inline. Bureaucracy, but in type-system form.
fn default_file_common_sink_config() -> CommonSinkConfig {
    // -- ✅ ancient proverb: "He who ships with defaults, panics in production with style"
    CommonSinkConfig::default()
}
