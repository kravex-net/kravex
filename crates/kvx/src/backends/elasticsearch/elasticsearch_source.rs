use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;

use crate::backends::Source;
use crate::progress::ProgressMetrics;
use crate::backends::CommonSourceConfig;

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

/// 📦 The source side of the Elasticsearch backend.
///
/// This struct holds a config and a progress tracker, and currently does approximately
/// nothing useful in production because `next_batch` returns empty. 🐛
/// It is, however, a *very* well-intentioned nothing. The vibes are all correct.
/// The scaffolding is artisan-grade. The potential is immense. The implementation is... pending.
///
/// No cap, this will slap once scroll/search_after lands. We believe in it. We believe in you.
pub struct ElasticsearchSource {
    #[allow(dead_code)]
    // -- 🔧 config kept for when next_batch finally stops ghosting us and actually scrolls.
    // -- Marked dead_code because rustc has opinions and no chill.
    config: ElasticsearchSourceConfig,
    // 📊 progress tracker — total_size is 0 because elasticsearch doesn't tell us upfront.
    // -- it's fine. we're fine. we'll show what we can. no percent, no ETA. just vibes.
    // TODO: implement _count query on init so we can actually show progress like adults
    progress: ProgressMetrics,
}

impl std::fmt::Debug for ElasticsearchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 🔧 We carefully omit `progress` here because indicatif::ProgressBar does not implement
        // -- Debug. It's a whole thing. Don't ask. Actually do ask — it's a good story about
        // -- why we can't have nice derive macros sometimes. Short version: channels. Long version:
        // -- also channels, but with more feelings.
        f.debug_struct("ElasticsearchSource")
            .field("config", &self.config)
            .finish() // 🚀 progress omitted — it's in there, trust us, no cap
    }
}

#[async_trait]
impl Source for ElasticsearchSource {
    /// 📡 Returns the next raw page from Elasticsearch.
    ///
    /// Currently returns `None` faster than you can say "scroll API."
    /// It's aspirational. It's a placeholder with excellent posture.
    /// The borrow checker is fully satisfied. The product manager is not.
    /// "He who stubs with None, deploys with hope." — Ancient scroll API proverb 📜
    async fn next_page(&mut self) -> Result<Option<String>> {
        // TODO: Implement search_after — the glow-up we deserve. 🚀
        Ok(None)
    }
}

impl ElasticsearchSource {
    /// 🚀 Constructs a new `ElasticsearchSource`.
    ///
    /// Currently: allocates a ProgressMetrics with `total_size = 0` because we have
    /// no idea how many docs are waiting for us — Elasticsearch does not greet us at the
    /// door with a number. It's mysterious like that. Enigmatic. A little rude, honestly.
    ///
    /// "How much data?" "Yes." — Elasticsearch, every time.
    ///
    /// ⚠️ Future improvement: fire a `_count` query here so we can show a real ETA
    /// instead of an existential void on the progress bar.
    pub async fn new(config: ElasticsearchSourceConfig) -> Result<Self> {
        // 📡 total_size = 0: unknown until we scroll through everything.
        // -- Classic elasticsearch — "how much data is there?" — "yes"
        // -- It's fine. We'll count as we go. Like eating chips and not checking how many are left.
        let progress = ProgressMetrics::new(config.url.clone(), 0);
        Ok(Self { config, progress })
    }
}
