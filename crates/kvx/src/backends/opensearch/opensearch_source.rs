// ai
//! 🔍📡 OpenSearch Source — the faucet that pours documents from your cluster 🚀🔄
//!
//! 🎬 COLD OPEN — INT. OPENSEARCH CLUSTER — THE DOCUMENTS ARE RESTLESS
//!
//! Somewhere, deep in a 12-shard index, 47 million documents sit in storage.
//! They've been there since the last migration. Or the one before that. Nobody
//! remembers. Nobody asks. But tonight, someone configures a source. Someone
//! runs kravex. And one by one, page by page, those documents will march out
//! through the search_after API, through a Point-In-Time snapshot, through the
//! network, and into whatever sink awaits them on the other side.
//!
//! This was the first search-source implementation in kravex (Elasticsearch
//! followed the same pattern). Uses PIT (Point-In-Time) + `search_after` for consistent, efficient
//! deep pagination. No scroll API. No server-side cursors. Just stateless
//! requests and a sort field that remembers where we left off.
//!
//! ## Knowledge Graph 🧠
//! - Pattern: implements `Source` trait, same as `FileSource`, `S3RallySource`
//! - Pagination: PIT + search_after (OpenSearch 2.4+, ES 7.10+)
//! - Fallback: scroll API for OpenSearch < 2.4 (TODO: future enhancement)
//! - Output: NDJSON lines, each line is a full hit object:
//!   `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
//! - The `_id` and `_index` are preserved so downstream transforms can
//!   build bulk action lines with proper document identity routing
//! - Auth: API key > basic auth (same as sink). SigV4 stubbed for future.
//! - TLS: optional `danger_accept_invalid_certs`
//!
//! ⚠️ The singularity will arrive before we paginate through the entire internet.
//! But we'll have a progress bar for it. That counts for something. 🦆

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tracing::{debug, info, trace};

use crate::backends::Source;
use crate::progress::ProgressMetrics;

// 🔧 OpenSearch source config — what index, what cluster, how to authenticate, how big are pages.
//
// The `index` field is REQUIRED for source (unlike sink where per-doc routing exists).
// You must know WHERE the documents live to read them. This is not a philosophical question.
// It is a practical one. The API requires it. So do we.
#[derive(Debug, Deserialize, Clone)]
pub struct OpenSearchSourceConfig {
    /// 📡 The URL of your OpenSearch cluster. Include scheme + port.
    /// `https://localhost:9200` is the classic. Don't forget the scheme.
    pub url: String,
    /// 📦 The index to read from. Required. Non-negotiable.
    /// "He who searches without an index, searches in vain." — Ancient query proverb
    pub index: String,
    /// 🔒 Username for basic auth. Optional but recommended unless you like open doors.
    #[serde(default)]
    pub username: Option<String>,
    /// 🔒 Password. Keep it out of version control. I'm watching you.
    #[serde(default)]
    pub password: Option<String>,
    /// 🔒 API key — takes priority over basic auth. The premium lane.
    #[serde(default)]
    pub api_key: Option<String>,
    /// 🔓 Accept invalid TLS certificates. For dev clusters with self-signed certs.
    /// Production usage will void your security warranty. Such as it is.
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
    // 🔮 TODO (next story): AWS SigV4 authentication for Amazon OpenSearch Service.
    // When this lands, add: pub aws_sigv4_region: Option<String>
    // Use aws-sigv4 to sign each request. Credentials from aws-config.
    /// 🔍 Optional query filter as a JSON string. Defaults to `match_all`.
    /// Pass a valid OpenSearch query DSL object, e.g., `{"term":{"status":"active"}}`.
    /// If None, we read ALL documents. Every single one. No discrimination.
    #[serde(default)]
    pub query: Option<String>,
}

/// 🔍 The source side of the OpenSearch backend — PIT + search_after pagination.
///
/// This struct holds the HTTP client, config, pagination state (PIT ID + search_after
/// cursor), and progress metrics. Each call to `next_page()` fetches the next batch
/// of documents using stateless search_after queries against a Point-In-Time snapshot.
///
/// ## Pagination Algorithm 🧠
///
/// 1. First `next_page()`: Open PIT → search with `sort: [{"_doc": "asc"}]`
/// 2. Subsequent calls: search with `search_after: <last_sort_values>` + PIT ID
/// 3. Zero hits: delete PIT, return `None` (EOF)
///
/// ## Why PIT + search_after over scroll API
///
/// - OpenSearch docs recommend it for deep pagination
/// - Scroll API is being deprecated in newer OpenSearch versions
/// - PIT provides a consistent snapshot without holding search context on every shard
/// - search_after is stateless on the server side (no server-side cursor to expire)
///
/// "He who scrolls, holds resources on every shard. He who searches_after, travels light."
pub struct OpenSearchSource {
    client: reqwest::Client,
    config: OpenSearchSourceConfig,
    /// 🔖 Point-In-Time ID for consistent pagination snapshot.
    /// Opened on first `next_page()`, deleted when exhausted or on drop.
    pit_id: Option<String>,
    /// 🔄 The sort values from the last document of the previous page.
    /// Used as the cursor for search_after. None on first request.
    search_after: Option<Vec<Value>>,
    /// 🏁 Whether we've exhausted all pages (zero hits returned).
    exhausted: bool,
    /// 📊 Progress tracking — total_size is 0 initially (we don't know upfront).
    progress: ProgressMetrics,
}

impl std::fmt::Debug for OpenSearchSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 🔧 Custom Debug because ProgressBar (inside ProgressMetrics) doesn't implement Debug.
        // This is the same workaround as ElasticsearchSource. The duck approves.
        f.debug_struct("OpenSearchSource")
            .field("config", &self.config)
            .field("pit_id", &self.pit_id.as_ref().map(|_| "<active>"))
            .field("exhausted", &self.exhausted)
            .finish() // 🚀 progress omitted — it's in there, trust us
    }
}

#[async_trait]
impl Source for OpenSearchSource {
    /// 📄 Fetch the next raw page of documents from OpenSearch.
    ///
    /// Returns `Ok(Some(page))` where page is NDJSON: one JSON line per hit.
    /// Each line: `{"_id":"abc","_index":"src-idx","_source":{"field":"val"}}`
    ///
    /// Returns `Ok(None)` when all documents have been read (EOF). The PIT is
    /// cleaned up automatically. The progress bar says "done." The engineer exhales.
    ///
    /// ## First call behavior
    /// Opens a PIT, executes the first search, stores the cursor.
    ///
    /// ## Subsequent calls
    /// Executes search_after with the stored cursor, advances the cursor.
    ///
    /// ## EOF detection
    /// When the search returns zero hits, we're done. PIT gets deleted.
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<String>> {
        // 🏁 Short-circuit if we already know we're done
        if self.exhausted {
            return Ok(None);
        }

        // 🔖 Open PIT on first call — the consistent snapshot begins here
        if self.pit_id.is_none() {
            self.open_pit().await?;
        }

        // 📡 Execute search with PIT + search_after
        let (hits, page_bytes) = self.execute_search(doc_count_hint).await?;

        if hits.is_empty() {
            // 🏁 EOF — no more documents. Clean up the PIT. Wave goodbye.
            debug!("🏁 OpenSearch source exhausted — zero hits returned, closing PIT");
            self.cleanup_pit().await;
            self.exhausted = true;
            self.progress.finish();
            return Ok(None);
        }

        // 📊 Update progress with this batch
        let docs_count = hits.len() as u64;
        self.progress.update(page_bytes as u64, docs_count);

        // 🔄 Build the NDJSON page from hits
        let page = hits.join("\n");

        trace!(
            "📄 OpenSearch source page: {} docs, {} bytes — the faucet flows",
            docs_count,
            page.len()
        );

        Ok(Some(page))
    }
}

impl OpenSearchSource {
    /// 🚀 Constructs a new `OpenSearchSource`.
    ///
    /// Validates connectivity by pinging the cluster root URL.
    /// Does NOT open the PIT yet — that happens on the first `next_page()` call.
    ///
    /// 🧠 total_size = 0 because OpenSearch doesn't tell us upfront how many docs exist.
    /// We could fire a `_count` query here, but that's a future enhancement.
    /// For now, progress shows raw counts without percentages. Like a road trip
    /// without a GPS ETA. You'll get there. Eventually. Probably.
    pub(crate) async fn new(config: OpenSearchSourceConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60)) // 60s for search requests — they can be chunky
            .danger_accept_invalid_certs(config.danger_accept_invalid_certs)
            .build()
            .context("💀 HTTP client refused to initialize. The TLS stack is having a moment. Check your certs, check your OpenSSL, check your horoscope.")?;

        // 📡 Connectivity ping — make sure the cluster is alive before we commit to reading
        let mut ping = client.get(&config.url);
        ping = apply_auth(ping, &config);
        ping.send()
            .await
            .context("💀 OpenSearch connectivity check failed. The cluster is either down, unreachable, or pretending to be busy. Check the URL and network.")?;

        info!(
            "🚀 OpenSearch source initialized — target: {}/{}",
            config.url, config.index
        );

        // 📊 total_size = 0: unknown until we page through everything.
        // "How many documents?" — "Yes." — OpenSearch, always
        let progress = ProgressMetrics::new(format!("opensearch://{}", config.index), 0);

        Ok(Self {
            client,
            config,
            pit_id: None,
            search_after: None,
            exhausted: false,
            progress,
        })
    }

    /// 🔖 Open a Point-In-Time (PIT) for consistent pagination.
    ///
    /// PIT gives us a frozen snapshot of the index at this moment in time.
    /// Documents indexed/deleted after this point won't affect our pagination.
    /// It's like taking a photograph of the data — the original can change,
    /// but our photo stays the same.
    ///
    /// Endpoint: `POST /{index}/_search/point_in_time?keep_alive=5m`
    async fn open_pit(&mut self) -> Result<()> {
        let pit_url = format!(
            "{}/{}/_search/point_in_time?keep_alive=5m",
            self.config.url.trim_end_matches('/'),
            self.config.index
        );

        let mut request = self.client.post(&pit_url);
        request = apply_auth(request, &self.config);

        let response = request
            .send()
            .await
            .context("💀 Failed to open PIT on OpenSearch. The cluster might not support Point-In-Time (requires OpenSearch 2.4+). Or the index doesn't exist. Or the network is ghosting us. Classic.")?;

        if !response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 PIT creation failed. OpenSearch said: '{}'. If you're on OpenSearch < 2.4, PIT is not available. Consider upgrading, or filing a feature request with Father Time.",
                body
            );
        }

        let response_text = response.text().await.context(
            "💀 PIT response body could not be read. The cluster whispered and we couldn't hear.",
        )?;
        let body: Value = serde_json::from_str(&response_text)
            .context("💀 PIT response was not valid JSON. The cluster is speaking in tongues.")?;

        let pit_id = body["pit_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("💀 PIT response missing 'pit_id' field. The response was: {}. This is unusual.", body))?
            .to_string();

        debug!("🔖 PIT opened successfully — snapshot taken, time frozen, documents captured");
        self.pit_id = Some(pit_id);
        Ok(())
    }

    /// 📡 Execute a search request with PIT + search_after pagination.
    ///
    /// Returns: (Vec<String>, usize) — (NDJSON lines for each hit, total bytes in page)
    ///
    /// Each hit line: `{"_id":"abc","_index":"src-idx","_source":{...}}`
    /// The _id and _index are preserved so downstream transforms can build
    /// bulk action lines with proper document identity routing.
    async fn execute_search(&mut self, batch_size: usize) -> Result<(Vec<String>, usize)> {
        let search_url = format!("{}/_search", self.config.url.trim_end_matches('/'));

        // 🏗️ Build the search body
        let query = match &self.config.query {
            Some(q) => serde_json::from_str(q).context(
                "💀 The query filter is not valid JSON. You passed a query string that serde couldn't parse. Check your JSON syntax. We believe in you.",
            )?,
            None => serde_json::json!({"match_all": {}}),
        };

        let mut search_body = serde_json::json!({
            "size": batch_size,
            "query": query,
            "sort": [{"_doc": "asc"}],
            "_source": true
        });

        // 🔖 Add PIT to the search body
        if let Some(ref pit_id) = self.pit_id {
            search_body["pit"] = serde_json::json!({
                "id": pit_id,
                "keep_alive": "5m"
            });
        }

        // 🔄 Add search_after cursor if we have one (not first page)
        if let Some(ref cursor) = self.search_after {
            search_body["search_after"] = Value::Array(cursor.clone());
        }

        let search_body_str = serde_json::to_string(&search_body)
            .context("💀 Failed to serialize search body. The JSON gods are not pleased.")?;

        let mut request = self
            .client
            .post(&search_url)
            .header("Content-Type", "application/json")
            .body(search_body_str);
        request = apply_auth(request, &self.config);

        let response = request
            .send()
            .await
            .context("💀 Search request failed. The network dropped our carefully crafted query into the void. Check connectivity and try again.")?;

        if !response.status().is_success() {
            let err_body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 Search request failed. OpenSearch responded with: '{}'. Check your query, index name, and cluster health.",
                err_body
            );
        }

        let response_text = response
            .text()
            .await
            .context("💀 Search response body could not be read.")?;
        let body: Value = serde_json::from_str(&response_text)
            .context("💀 Search response was not valid JSON. The cluster is having a bad day.")?;

        // 🔖 Update PIT ID (it may be refreshed in the response)
        if let Some(new_pit_id) = body["pit_id"].as_str() {
            self.pit_id = Some(new_pit_id.to_string());
        }

        // 📦 Extract hits
        let hits = body["hits"]["hits"].as_array().ok_or_else(|| {
            anyhow::anyhow!(
                "💀 Search response missing hits.hits array. Response: {}",
                body
            )
        })?;

        if hits.is_empty() {
            return Ok((Vec::new(), 0));
        }

        // 🔄 Update search_after cursor from the last hit's sort values
        if let Some(sort_values) = hits.last().and_then(|h| h["sort"].as_array()) {
            self.search_after = Some(sort_values.clone());
        }

        // 📄 Build NDJSON lines — each line preserves _id, _index, _source
        let mut lines = Vec::with_capacity(hits.len());
        let mut total_bytes = 0usize;

        for hit in hits {
            // 🏗️ Build a compact hit object with just the fields we need
            let hit_line = serde_json::json!({
                "_id": hit["_id"],
                "_index": hit["_index"],
                "_source": hit["_source"]
            });
            let line = serde_json::to_string(&hit_line).context(
                "💀 Failed to serialize hit. The document came out of OpenSearch but couldn't go back into JSON. Ironic.",
            )?;
            total_bytes += line.len();
            lines.push(line);
        }

        trace!(
            "📡 Search returned {} hits, {} bytes — the well is not yet dry",
            lines.len(),
            total_bytes
        );

        Ok((lines, total_bytes))
    }

    /// 🗑️ Clean up the PIT — delete the point-in-time snapshot.
    ///
    /// Called when we exhaust all documents or encounter an unrecoverable error.
    /// Best-effort: if deletion fails, the PIT will expire naturally (5m keep_alive).
    /// We log but don't propagate the error — it's cleanup, not critical path.
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
                    debug!("🗑️ PIT deleted successfully — snapshot released, time unfrozen");
                }
                Ok(resp) => {
                    debug!(
                        "⚠️ PIT deletion returned {}: not critical, it'll expire in 5m anyway",
                        resp.status()
                    );
                }
                Err(e) => {
                    debug!(
                        "⚠️ PIT deletion failed: {}. Not critical — it'll expire naturally. Like a gym membership.",
                        e
                    );
                }
            }
        }
        self.pit_id = None;
    }
}

/// 🔒 Apply auth to a reqwest::RequestBuilder. API key > basic auth.
/// Same pattern as the sink — consistency is a feature, not an accident.
fn apply_auth(
    request: reqwest::RequestBuilder,
    config: &OpenSearchSourceConfig,
) -> reqwest::RequestBuilder {
    if let Some(ref api_key) = config.api_key {
        request.header("Authorization", format!("ApiKey {}", api_key))
    } else if let Some(ref username) = config.username {
        request.basic_auth(username, config.password.as_ref())
    } else {
        request
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 OpenSearchSourceConfig deserializes with all fields.
    #[test]
    fn the_one_where_source_config_has_all_the_fields() {
        let config = OpenSearchSourceConfig {
            url: "https://localhost:9200".to_string(),
            index: "my-index".to_string(),
            username: Some("admin".to_string()),
            password: Some("admin".to_string()),
            api_key: None,
            danger_accept_invalid_certs: true,
            query: Some(r#"{"term":{"status":"active"}}"#.to_string()),
        };
        assert_eq!(config.index, "my-index");
        assert!(config.danger_accept_invalid_certs);
        assert!(config.query.is_some());
    }

    /// 🧪 danger_accept_invalid_certs defaults to false.
    #[test]
    fn the_one_where_source_danger_defaults_to_false() {
        let json = r#"{"url": "https://localhost:9200", "index": "test"}"#;
        let config: OpenSearchSourceConfig = serde_json::from_str(json)
            .expect("💀 Config deserialization failed. The JSON was handcrafted with love.");
        assert!(!config.danger_accept_invalid_certs);
    }

    /// 🧪 Query defaults to None (match_all).
    #[test]
    fn the_one_where_query_defaults_to_none_meaning_match_all() {
        let json = r#"{"url": "https://localhost:9200", "index": "test"}"#;
        let config: OpenSearchSourceConfig =
            serde_json::from_str(json).expect("💀 Config deserialization failed.");
        assert!(
            config.query.is_none(),
            "No query = match_all, the firehose approach"
        );
    }
}
