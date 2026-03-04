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
use serde_json::value::RawValue;
use serde_json::Value;
use std::fmt::Write;
use std::time::Duration;
use tracing::{debug, info, trace};

use crate::backends::Source;
use crate::buffer_pool;
use crate::progress::ProgressMetrics;

// 🧠 Zero-copy deserialization structs for search responses.
// Same pattern as elasticsearch_source.rs — RawValue borrows from response_text,
// no intermediate Value tree. Eliminates N × json!() + to_string() per page.
// If you're reading this in both files: yes, we duplicated. No, we won't abstract.
// Two backends, two files, two copies. The borrow checker prefers explicit over clever.

/// 📦 Pre-computed search result — everything pump() needs, ready to go.
/// "Pre-computation: because the async executor has better things to do." — Ancient Tokio Proverb
pub(crate) struct ParsedSearchResult {
    /// 📄 Pre-joined NDJSON page — one line per hit, no trailing newline
    pub page: String,
    /// 📊 Total bytes across all hit lines
    pub page_bytes: usize,
    /// 📊 Number of documents in this page
    pub doc_count: usize,
    /// 🔖 Refreshed PIT ID (OS may rotate it)
    pub new_pit_id: Option<String>,
    /// 🔄 Sort values from the last hit — becomes the next search_after cursor
    pub new_search_after: Option<Vec<Value>>,
}

/// 📡 Typed search response — borrows strings via RawValue, zero-copy life.
#[derive(Deserialize)]
struct SearchResponse<'a> {
    #[serde(default)]
    pit_id: Option<&'a str>,
    hits: SearchResponseHits<'a>,
}

/// 📦 The hits wrapper — OpenSearch also nests hits inside hits. Tradition.
#[derive(Deserialize)]
struct SearchResponseHits<'a> {
    #[serde(borrow)]
    hits: Vec<SearchResponseHit<'a>>,
}

/// 📄 A single hit — _id and _source as RawValue (zero-copy JSON slices).
#[derive(Deserialize)]
struct SearchResponseHit<'a> {
    #[serde(borrow)]
    _id: Option<&'a RawValue>,
    _index: Option<&'a str>,
    #[serde(borrow)]
    _source: Option<&'a RawValue>,
    sort: Option<Vec<Value>>,
}

/// 🧵 Parse search response JSON — CPU work offloaded from tokio via spawn_blocking.
/// write!() directly into a pre-sized String page buffer. No Vec<String> + join.
/// "One buffer to rule them all, one write!() to fill them." — Lord of the Allocations
fn parse_search_response(response_text: &str) -> Result<Option<ParsedSearchResult>> {
    let response: SearchResponse<'_> = serde_json::from_str(response_text)
        .context("💀 Search response was not valid JSON. The cluster is having a bad day.")?;

    let hits = &response.hits.hits;

    if hits.is_empty() {
        return Ok(None);
    }

    // 🏗️ Pre-size: ~200 bytes per doc estimate
    let mut page = String::with_capacity(hits.len() * 200);
    let mut total_bytes = 0usize;

    for (i, hit) in hits.iter().enumerate() {
        if i > 0 {
            page.push('\n');
        }

        let the_mark_before_writing = page.len();

        // -- 🎬 "Write it all into one buffer. Like a confession, but faster." — Every parser ever
        page.push_str(r#"{"_id":"#);
        match hit._id {
            Some(raw_id) => page.push_str(raw_id.get()),
            None => page.push_str("null"),
        }
        page.push_str(r#","_index":"#);
        match hit._index {
            Some(idx) => write!(page, "\"{}\"", idx).unwrap(),
            None => page.push_str("null"),
        }
        page.push_str(r#","_source":"#);
        match &hit._source {
            Some(raw_src) => page.push_str(raw_src.get()),
            None => page.push_str("null"),
        }
        page.push('}');

        total_bytes += page.len() - the_mark_before_writing;
    }

    let new_search_after = hits.last().and_then(|h| h.sort.clone());
    let new_pit_id = response.pit_id.map(|s| s.to_string());

    Ok(Some(ParsedSearchResult {
        page,
        page_bytes: total_bytes,
        doc_count: hits.len(),
        new_pit_id,
        new_search_after,
    }))
}

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
    async fn pump(&mut self, doc_count_hint: usize) -> Result<Option<buffer_pool::PoolBuffer>> {
        // 🏁 Short-circuit if we already know we're done
        if self.exhausted {
            return Ok(None);
        }

        // 🔖 Open PIT on first call — the consistent snapshot begins here
        if self.pit_id.is_none() {
            self.open_pit().await?;
        }

        // 📡 Execute search + parse (HTTP async, JSON parsing on spawn_blocking)
        let parsed = self.execute_search(doc_count_hint).await?;

        match parsed {
            None => {
                // 🏁 EOF — no more documents. Clean up the PIT. Wave goodbye.
                debug!("🏁 OpenSearch source exhausted — zero hits returned, closing PIT");
                self.cleanup_pit().await;
                self.exhausted = true;
                self.progress.finish();
                Ok(None)
            }
            Some(result) => {
                // 📊 Update progress + pagination state
                self.progress
                    .update(result.page_bytes as u64, result.doc_count as u64);

                if let Some(pit_id) = result.new_pit_id {
                    self.pit_id = Some(pit_id);
                }
                self.search_after = result.new_search_after;

                trace!(
                    "📄 OpenSearch source page: {} docs, {} bytes — zero-copy parsed, the faucet flows faster",
                    result.doc_count,
                    result.page.len()
                );

                // 🏦 Adopt the pre-joined NDJSON page into the buffer pool.
                // rent_from_string takes ownership — zero copy, just bucket assignment.
                Ok(Some(buffer_pool::rent_from_string(result.page)))
            }
        }
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

    /// 📡 Execute search: HTTP stays async, JSON parsing offloaded to spawn_blocking.
    ///
    /// Phase split: (1) build request body, (2) send/receive HTTP, (3) parse on blocking thread.
    /// At batch_size=10K, the parse phase was burning 10K json!() + to_string() cycles inline
    /// on the tokio executor. Now it runs on a dedicated thread with zero-copy RawValue deserialization.
    ///
    /// Returns None when hits are empty (EOF signal for pump).
    async fn execute_search(
        &mut self,
        batch_size: usize,
    ) -> Result<Option<ParsedSearchResult>> {
        let search_url = format!("{}/_search", self.config.url.trim_end_matches('/'));

        // 🏗️ Build the search body — trivial CPU, stays on async thread
        let query = match &self.config.query {
            Some(q) => serde_json::from_str(q).context(
                "💀 The query filter is not valid JSON. Check your JSON syntax. We believe in you.",
            )?,
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

        let search_body_str = serde_json::to_string(&search_body)
            .context("💀 Failed to serialize search body. The JSON gods are not pleased.")?;

        // 📡 HTTP send + receive — async, where it belongs
        let mut request = self
            .client
            .post(&search_url)
            .header("Content-Type", "application/json")
            .body(search_body_str);
        request = apply_auth(request, &self.config);

        let response = request
            .send()
            .await
            .context("💀 Search request failed. The network dropped our query into the void.")?;

        if !response.status().is_success() {
            let err_body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 Search failed. OpenSearch responded with: '{}'. Check query, index, and cluster health.",
                err_body
            );
        }

        let response_text = response
            .text()
            .await
            .context("💀 Search response body could not be read.")?;

        // 🧵 Offload CPU-heavy JSON parsing to a blocking thread.
        // At 10K docs, this is 10K deserialize+write cycles — too much for tokio's event loop.
        let parsed = tokio::task::spawn_blocking(move || parse_search_response(&response_text))
            .await
            .context("💀 spawn_blocking panicked during search response parsing. The thread had trust issues.")??;

        Ok(parsed)
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

    /// 🧪 parse_search_response extracts pit_id, hits, search_after.
    /// "We parsed it. We parsed it all. And we didn't even need serde_json::Value."
    /// — Zero-Copy Confessions, Volume 1 🦆
    #[test]
    fn the_one_where_parse_search_response_extracts_all_fields() {
        let response_json = r#"{
            "pit_id": "os_pit_xyz789",
            "hits": {
                "total": {"value": 100, "relation": "eq"},
                "hits": [
                    {
                        "_id": "os-doc1",
                        "_index": "source-index",
                        "_source": {"city": "Portland", "temp": 72},
                        "sort": [10]
                    },
                    {
                        "_id": "os-doc2",
                        "_index": "source-index",
                        "_source": {"city": "Seattle", "temp": 55},
                        "sort": [20]
                    },
                    {
                        "_id": "os-doc3",
                        "_index": "source-index",
                        "_source": {"city": "Vancouver", "temp": 48},
                        "sort": [30]
                    }
                ]
            }
        }"#;

        let result = parse_search_response(response_json)
            .expect("💀 Parse failed")
            .expect("💀 Got None but hits were present");

        assert_eq!(result.doc_count, 3);
        assert_eq!(result.new_pit_id.as_deref(), Some("os_pit_xyz789"));
        assert!(result.new_search_after.is_some());
        // 🔄 search_after should be from the LAST hit's sort
        assert_eq!(result.new_search_after.unwrap()[0], 30);

        // 📄 3 NDJSON lines
        let lines: Vec<&str> = result.page.split('\n').collect();
        assert_eq!(lines.len(), 3);

        // 🔍 First line should contain Portland
        assert!(
            lines[0].contains("Portland"),
            "First hit should be Portland"
        );
        // 🔍 Last line should contain Vancouver
        assert!(
            lines[2].contains("Vancouver"),
            "Last hit should be Vancouver"
        );
    }

    /// 🧪 RawValue preserves nested _source verbatim — no parsing, no re-ordering.
    /// The _source travels through like a diplomatic pouch: unopened, untouched, unquestioned. 🏃
    #[test]
    fn the_one_where_raw_value_borrows_source_without_parsing() {
        let response_json = r#"{
            "hits": {
                "hits": [
                    {
                        "_id": "nested-doc",
                        "_index": "deep-index",
                        "_source": {"a": {"b": {"c": [true, false, null]}}},
                        "sort": [42]
                    }
                ]
            }
        }"#;

        let result = parse_search_response(response_json)
            .expect("💀 Parse failed")
            .expect("💀 Got None");

        let parsed: Value =
            serde_json::from_str(&result.page).expect("💀 Page is not valid JSON");
        assert_eq!(parsed["_source"]["a"]["b"]["c"][0], true);
        assert_eq!(parsed["_source"]["a"]["b"]["c"][2], Value::Null);
        assert!(result.new_pit_id.is_none());
    }

    /// 🧪 Empty hits → None. The well is dry. The faucet is off. Go home.
    #[test]
    fn the_one_where_empty_hits_returns_none() {
        let response_json = r#"{
            "pit_id": "still_alive_pit",
            "hits": {
                "total": {"value": 0, "relation": "eq"},
                "hits": []
            }
        }"#;

        let result = parse_search_response(response_json)
            .expect("💀 Parse failed on empty response");

        assert!(result.is_none(), "📭 Empty hits = None, not empty page");
    }
}
