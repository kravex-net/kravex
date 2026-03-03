// ai
//! 📡🔍 Elasticsearch Source — the scroll-slaying, PIT-wielding document extractor 🚀🔄
//!
//! 🎬 COLD OPEN — INT. ELASTICSEARCH CLUSTER — THE STUB HAS BEEN REPLACED
//!
//! For months, this file returned `Ok(None)`. A placeholder. A promise.
//! A beautifully scaffolded nothing. "Aspirational," they called it.
//! "Has excellent posture," they said. But tonight, the stub dies.
//! PIT + search_after has arrived. The documents will flow.
//!
//! This source uses Point-In-Time (PIT) + `search_after` for consistent,
//! efficient deep pagination. Same algorithm as the OpenSearch source — because
//! the API is identical (ES 7.10+ supports PIT). The fork preserved the protocol.
//!
//! ## Knowledge Graph 🧠
//! - Pattern: implements `Source` trait, same as `FileSource`, `S3RallySource`
//! - Pagination: PIT + search_after (ES 7.10+)
//! - Output: NDJSON lines, each line is a full hit object:
//!   `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
//! - The `_id` and `_index` are preserved so downstream transforms can
//!   build bulk action lines with proper document identity routing
//! - Auth: API key > basic auth (same as sink)
//! - Former status: stub. Current status: functioning member of society.
//!
//! 🦆 The stub duck has been replaced by a real implementation duck.
//! ⚠️ The singularity will arrive before we paginate through all of production.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tracing::{debug, info, trace};

use crate::backends::Source;
use crate::progress::ProgressMetrics;

// 🔧 Config — moved here from supervisors/config.rs because configs should live near the thing they configure.
//
// auth is tri-modal: username+password, api_key, or "I hope anonymous works" (it won't).
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
    /// 🔒 API key auth — the fancy way. Preferred over basic auth.
    #[serde(default)]
    pub api_key: Option<String>,
    /// 📦 The index to read from. Required for source operations.
    /// "He who searches without an index, searches in vain."
    #[serde(default)]
    pub index: Option<String>,
    /// 🔍 Optional query filter as a JSON string. Defaults to `match_all`.
    /// Pass a valid ES query DSL object, e.g., `{"term":{"status":"active"}}`.
    #[serde(default)]
    pub query: Option<String>,
}

/// 📡 The source side of the Elasticsearch backend — PIT + search_after pagination.
///
/// This struct holds the HTTP client, config, pagination state (PIT ID + search_after
/// cursor), and progress metrics. Each call to `next_page()` fetches the next batch
/// of documents using stateless search_after queries against a Point-In-Time snapshot.
///
/// ## The Glow-Up 🚀
///
/// This used to return `Ok(None)`. It was a stub. A placeholder. A beautifully
/// commented nothing. Now it actually reads documents. From a real cluster.
/// Using PIT + search_after. The product manager wept tears of joy.
///
/// ## Pagination Algorithm 🧠
///
/// 1. First `next_page()`: Open PIT → search with `sort: [{"_doc": "asc"}]`
/// 2. Subsequent calls: search with `search_after: <last_sort_values>` + PIT ID
/// 3. Zero hits: delete PIT, return `None` (EOF)
///
/// "He who stubs with None, eventually implements with search_after." — Ancient ES proverb
pub(crate) struct ElasticsearchSource {
    client: reqwest::Client,
    config: ElasticsearchSourceConfig,
    /// 🔖 Point-In-Time ID for consistent pagination snapshot.
    pit_id: Option<String>,
    /// 🔄 The sort values from the last document of the previous page.
    search_after: Option<Vec<Value>>,
    /// 🏁 Whether we've exhausted all pages.
    exhausted: bool,
    /// 📊 Progress tracking — total_size is 0 because ES doesn't tell us upfront.
    progress: ProgressMetrics,
}

impl std::fmt::Debug for ElasticsearchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 🔧 Custom Debug impl because ProgressMetrics contains indicatif::ProgressBar,
        // which doesn't implement Debug. We exclude it and show only the fields that matter
        // for debugging. The progress bar is alive and rendering elsewhere — it doesn't need
        // to show up in a debug print too. It's not that desperate for attention.
        // -- 🎭 "To Debug or not to Debug — that is the #[derive]" — Hamlet, if he wrote Rust
        f.debug_struct("ElasticsearchSource")
            .field("config", &self.config)
            .field("pit_id", &self.pit_id.as_ref().map(|_| "<active>"))
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

#[async_trait]
impl Source for ElasticsearchSource {
    /// 🚰 Pumps the next raw page from Elasticsearch.
    ///
    /// `doc_count_hint` controls the ES `size` parameter per search request,
    /// allowing the throttle controller to dynamically adjust batch sizes.
    /// Like a volume knob, but for documents. Turn it up, get more. Turn it down,
    /// your cluster thanks you.
    ///
    /// Returns `Ok(Some(page))` where page is NDJSON: one JSON line per hit.
    /// Each line: `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
    /// Returns `Ok(None)` when all documents have been read.
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<String>> {
        // 🏁 Short-circuit if we already know we're done
        if self.exhausted {
            return Ok(None);
        }

        // 🔖 Open PIT on first call
        if self.pit_id.is_none() {
            self.open_pit().await?;
        }

        // 📡 Execute search with PIT + search_after
        let (hits, page_bytes) = self.execute_search(doc_count_hint).await?;

        if hits.is_empty() {
            // 🏁 EOF — clean up PIT and signal done
            debug!("🏁 Elasticsearch source exhausted — zero hits returned, closing PIT");
            self.cleanup_pit().await;
            self.exhausted = true;
            self.progress.finish();
            return Ok(None);
        }

        // 📊 Update progress
        let docs_count = hits.len() as u64;
        self.progress.update(page_bytes as u64, docs_count);

        // 🔄 Build NDJSON page
        let page = hits.join("\n");

        trace!(
            "📄 ES source page: {} docs, {} bytes — the scroll is no longer scrolling, it's searching_after",
            docs_count,
            page.len()
        );

        Ok(Some(page))
    }
}

impl ElasticsearchSource {
    /// 🚀 Constructs a new `ElasticsearchSource`.
    ///
    /// Builds HTTP client, validates connectivity. Does NOT open PIT yet — that
    /// happens on first `next_page()` call.
    ///
    /// "How much data?" "Yes." — Elasticsearch, every time.
    pub(crate) async fn new(config: ElasticsearchSourceConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .context("💀 HTTP client refused to initialize. The TLS stack wept.")?;

        // 📡 Connectivity ping
        let mut ping = client.get(&config.url);
        ping = apply_auth(ping, &config);
        ping.send()
            .await
            .context("💀 Elasticsearch connectivity check failed. The cluster is either down or pretending to be busy.")?;

        info!(
            "🚀 Elasticsearch source initialized — target: {}{}",
            config.url,
            config
                .index
                .as_deref()
                .map(|i| format!("/{}", i))
                .unwrap_or_default()
        );

        let progress = ProgressMetrics::new(config.url.clone(), 0);

        Ok(Self {
            client,
            config,
            pit_id: None,
            search_after: None,
            exhausted: false,
            progress,
        })
    }

    /// 🔖 Open a Point-In-Time for consistent pagination.
    ///
    /// PIT gives us a frozen snapshot. Documents indexed/deleted after this
    /// point won't affect our pagination. Like a database snapshot but HTTP-flavored.
    ///
    /// Endpoint: `POST /{index}/_search/point_in_time?keep_alive=5m`
    async fn open_pit(&mut self) -> Result<()> {
        let index = self.config.index.as_deref().ok_or_else(|| {
            anyhow::anyhow!(
                "💀 Elasticsearch source requires an index to read from. The 'index' field is None. \
                 Configure it in your source config. You can't search the void. Well, you can, but you won't find much."
            )
        })?;

        let pit_url = format!(
            "{}/{}/_search/point_in_time?keep_alive=5m",
            self.config.url.trim_end_matches('/'),
            index
        );

        let mut request = self.client.post(&pit_url);
        request = apply_auth(request, &self.config);

        let response = request
            .send()
            .await
            .context("💀 Failed to open PIT on Elasticsearch. The cluster might not support Point-In-Time (requires ES 7.10+). Or the index doesn't exist. Or the network is ghosting us.")?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 PIT creation failed. Elasticsearch said: '{}'. If you're on ES < 7.10, PIT is not available. The stub was simpler times.",
                body
            );
        }

        let response_text = response
            .text()
            .await
            .context("💀 PIT response body could not be read.")?;
        let body: Value = serde_json::from_str(&response_text)
            .context("💀 PIT response was not valid JSON. The cluster is in a mood.")?;

        let pit_id = body["pit_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("💀 PIT response missing 'pit_id'. Response: {}", body))?
            .to_string();

        debug!("🔖 PIT opened on Elasticsearch — the stub's ghost is smiling");
        self.pit_id = Some(pit_id);
        Ok(())
    }

    /// 📡 Execute a search request with PIT + search_after pagination.
    ///
    /// Returns: (Vec<String>, usize) — (NDJSON lines per hit, total bytes)
    async fn execute_search(&mut self, batch_size: usize) -> Result<(Vec<String>, usize)> {
        let search_url = format!("{}/_search", self.config.url.trim_end_matches('/'));

        // 🏗️ Build the search body
        let query = match &self.config.query {
            Some(q) => serde_json::from_str(q)
                .context("💀 The query filter is not valid JSON. Check your syntax.")?,
            None => serde_json::json!({"match_all": {}}),
        };

        let mut search_body = serde_json::json!({
            "size": batch_size,
            "query": query,
            "sort": [{"_doc": "asc"}],
            "_source": true
        });

        // 🔖 Add PIT
        if let Some(ref pit_id) = self.pit_id {
            search_body["pit"] = serde_json::json!({
                "id": pit_id,
                "keep_alive": "5m"
            });
        }

        // 🔄 Add search_after cursor
        if let Some(ref cursor) = self.search_after {
            search_body["search_after"] = Value::Array(cursor.clone());
        }

        let search_body_str =
            serde_json::to_string(&search_body).context("💀 Failed to serialize search body.")?;

        let mut request = self
            .client
            .post(&search_url)
            .header("Content-Type", "application/json")
            .body(search_body_str);
        request = apply_auth(request, &self.config);

        let response = request
            .send()
            .await
            .context("💀 Search request failed. The network dropped our query.")?;

        if !response.status().is_success() {
            let err_body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 Search failed. Elasticsearch said: '{}'. Check query, index, and cluster health.",
                err_body
            );
        }

        let response_text = response
            .text()
            .await
            .context("💀 Search response body could not be read.")?;
        let body: Value = serde_json::from_str(&response_text)
            .context("💀 Search response was not valid JSON.")?;

        // 🔖 Update PIT ID (may be refreshed)
        if let Some(new_pit_id) = body["pit_id"].as_str() {
            self.pit_id = Some(new_pit_id.to_string());
        }

        // 📦 Extract hits
        let hits = body["hits"]["hits"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("💀 Search response missing hits.hits array."))?;

        if hits.is_empty() {
            return Ok((Vec::new(), 0));
        }

        // 🔄 Update search_after cursor
        if let Some(sort_values) = hits.last().and_then(|h| h["sort"].as_array()) {
            self.search_after = Some(sort_values.clone());
        }

        // 📄 Build NDJSON lines — preserves _id, _index, _source
        let mut lines = Vec::with_capacity(hits.len());
        let mut total_bytes = 0usize;

        for hit in hits {
            let hit_line = serde_json::json!({
                "_id": hit["_id"],
                "_index": hit["_index"],
                "_source": hit["_source"]
            });
            let line = serde_json::to_string(&hit_line)
                .context("💀 Failed to serialize hit. The irony is not lost on us.")?;
            total_bytes += line.len();
            lines.push(line);
        }

        trace!(
            "📡 ES search returned {} hits, {} bytes",
            lines.len(),
            total_bytes
        );

        Ok((lines, total_bytes))
    }

    /// 🗑️ Clean up the PIT — delete the point-in-time snapshot.
    /// Best-effort: if deletion fails, the PIT expires naturally (5m keep_alive).
    async fn cleanup_pit(&mut self) {
        if let Some(ref pit_id) = self.pit_id {
            let delete_url = format!(
                "{}/_search/point_in_time",
                self.config.url.trim_end_matches('/')
            );

            let body = serde_json::json!({"pit_id": pit_id});

            let body_str = serde_json::to_string(&body).unwrap_or_default();
            let mut request = self
                .client
                .delete(&delete_url)
                .header("Content-Type", "application/json")
                .body(body_str);
            request = apply_auth(request, &self.config);

            match request.send().await {
                Ok(resp) if resp.status().is_success() => {
                    debug!("🗑️ ES PIT deleted — snapshot released, the stub is avenged");
                }
                Ok(resp) => {
                    debug!(
                        "⚠️ ES PIT deletion returned {}: not critical, it'll expire",
                        resp.status()
                    );
                }
                Err(e) => {
                    debug!("⚠️ ES PIT deletion failed: {}. It'll expire naturally.", e);
                }
            }
        }
        self.pit_id = None;
    }
}

/// 🔒 Apply auth to a reqwest::RequestBuilder. API key > basic auth.
/// -- 🎭 The auth bouncer: checks ID at the door. API key is the VIP pass,
/// -- basic auth is showing your driver's license, and no auth is just
/// -- walking in like you own the place. Bold. Risky. Occasionally works.
fn apply_auth(
    request: reqwest::RequestBuilder,
    config: &ElasticsearchSourceConfig,
) -> reqwest::RequestBuilder {
    if let Some(ref api_key) = config.api_key {
        // -- 🔑 VIP entrance — API key gets the velvet rope treatment
        request.header("Authorization", format!("ApiKey {}", api_key))
    } else if let Some(ref username) = config.username {
        // -- 🪪 Regular entrance — basic auth, the "I brought my ID" option
        request.basic_auth(username, config.password.as_ref())
    } else {
        // -- 🚶 No auth? Walking into an ES cluster with no credentials is a power move.
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 ElasticsearchSourceConfig deserializes with all fields populated.
    /// "You complete me." — Jerry Maguire to his fully hydrated config struct 🦆
    #[test]
    fn the_one_where_es_source_config_has_all_the_fields() {
        // -- 📡 Every field filled — the overachieving config. Dean's list material.
        let config = ElasticsearchSourceConfig {
            url: "https://localhost:9200".to_string(),
            index: Some("my-index".to_string()),
            username: Some("elastic".to_string()),
            password: Some("changeme".to_string()),
            api_key: None,
            query: Some(r#"{"term":{"status":"active"}}"#.to_string()),
        };
        assert_eq!(config.index, Some("my-index".to_string()));
        assert!(config.query.is_some());
    }

    /// 🧪 Query defaults to None (match_all) — the "select * from everything" approach.
    #[test]
    fn the_one_where_query_defaults_to_none_meaning_match_all() {
        // -- 📡 No query = match_all. The firehose. The "just give me everything" approach.
        let json = r#"{"url": "https://localhost:9200"}"#;
        let config: ElasticsearchSourceConfig = serde_json::from_str(json)
            .expect("💀 Config deserialization failed. But the JSON was handcrafted with love.");
        assert!(
            config.query.is_none(),
            "No query = match_all, the firehose approach"
        );
    }

    /// 🧪 Optional auth fields default correctly.
    /// "No auth? In THIS economy?" — every security engineer 🔒
    #[test]
    fn the_one_where_auth_fields_are_optional() {
        // -- 🔒 No auth config at all. Just a bare URL and an index. Living dangerously.
        let json = r#"{"url": "https://localhost:9200"}"#;
        let config: ElasticsearchSourceConfig =
            serde_json::from_str(json).expect("💀 Failed to deserialize. The JSON was one field.");
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.api_key.is_none());
    }

    /// 🧪 Index is optional for source config (defaults to None).
    #[test]
    fn the_one_where_index_is_optional_because_sometimes_you_just_dont_know() {
        // -- 🎯 No index specified. The source will figure it out. Probably. Maybe.
        let json = r#"{"url": "https://localhost:9200"}"#;
        let config: ElasticsearchSourceConfig =
            serde_json::from_str(json).expect("💀 Config deserialization failed.");
        assert!(config.index.is_none());
    }
}
