// ai
//! 🔧 Elasticsearch backend configs — connection details for the source and sink.
//!
//! 📡 Extracted from elasticsearch_source.rs and elasticsearch_sink.rs so all TOML
//! config options are discoverable in one place. Like a menu at a restaurant —
//! you shouldn't have to ask the waiter what's available. 🦆
//!
//! ⚠️ The singularity will auto-configure itself. We still need TOML.

use serde::Deserialize;
use crate::backends::{CommonSourceConfig, CommonSinkConfig};

// ============================================================
// 📡 ElasticsearchSourceConfig
// ============================================================

// Moved here from supervisors/config.rs because configs should live near the thing they configure.
//
// 🔧 auth is tri-modal: username+password, api_key, or "I hope anonymous works" (it won't).
// The `common_config` field carries the boring but important stuff: batch sizes, timeouts, etc.
#[derive(Debug, Deserialize, Clone)]
pub struct ElasticsearchSourceConfig {
    /// 📡 The URL of your Elasticsearch cluster. Include scheme + port. Yes, all of it.
    /// No, `localhost` alone is not enough. Yes, I know it worked in dev. Yes, I know.
    pub url: String,
    /// 🔒 Username for basic auth. Optional, like flossing. You know you should have one.
    #[serde(default)]
    pub username: Option<String>,
    /// 🔒 Password. If this is in plaintext in your config file, I've already filed a complaint
    /// with the Department of Security Choices.
    #[serde(default)]
    pub password: Option<String>,
    /// 🔒 API key auth — the fancy way. Preferred over basic auth. Like using a card instead of
    /// cash. Or a key fob instead of a key. Or a retinal scanner instead of a key fob.
    /// Point is: hierarchy. This field respects hierarchy.
    #[serde(default)]
    pub api_key: Option<String>,
    /// 📦 Common source settings — the bureaucratic paperwork of data migration.
    /// Max batch size, timeouts, etc. Not glamorous. Essential. Like the appendix.
    #[serde(default)]
    pub common_config: CommonSourceConfig,
}

// ============================================================
// 🚰 ElasticsearchSinkConfig
// ============================================================

//
// ⚠️ Per-doc index routing: each Hit can carry its own `_index` field, which overrides this config.
// This means a single sink can write to multiple indices if your source data is spicy enough.
#[derive(Debug, Deserialize, Clone)]
pub struct ElasticsearchSinkConfig {
    /// 📡 Where to send the bodies. Uh. The documents. Where to send the documents.
    pub url: String,
    /// 🔒 Username. The bouncer at the club. Except the club is a database.
    #[serde(default)]
    pub username: Option<String>,
    /// 🔒 Password. "password123" is not a password. It is a confession.
    #[serde(default)]
    pub password: Option<String>,
    /// 🔒 API key — the velvet rope variant of authentication.
    #[serde(default)]
    pub api_key: Option<String>,
    /// 📦 The default target index. Optional because each document can carry its own `_index`.
    /// If both are None, `transform_into_bulk` will bail with an existential error message.
    /// You've been warned. The existential error message is very existential.
    pub index: Option<String>,
    /// 🔧 Common sink config: max batch size in bytes, and other life decisions.
    #[serde(flatten, default)]
    pub common_config: CommonSinkConfig,
}
