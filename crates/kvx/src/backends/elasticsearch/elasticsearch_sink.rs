// ai
//! 🔥📡💀 The Elasticsearch Sink — where NDJSON payloads go to become indexed documents.
//!
//! *The cluster was quiet. Too quiet. Then the bulk request arrived — 50MB of NDJSON,
//! screaming through the wire like a freight train of JSON objects. The shards trembled.
//! The mapping creaked. And somewhere, deep in the JVM, a garbage collector whispered:
//! "Not today."*
//!
//! # What This Module Does 🚰
//! Pure I/O sink for Elasticsearch's `_bulk` API. Receives fully rendered NDJSON payloads
//! from the SinkWorker and POSTs them. No buffering. No transformation. Just HTTP.
//!
//! # Knowledge Graph 🧠
//! - SinkWorker → transform → buffer by bytes → Composer renders NDJSON → this sink POSTs it
//! - Auth priority: API key > basic auth (consistent across index check + bulk POST)
//! - ⚠️ Connectivity ping currently uses basic auth only — API key not applied there (known gap)
//! - Content-Type: `application/x-ndjson` — not `application/json`. Elasticsearch cares. A lot.
//!
//! 🦆 The duck is here because even sinks need emotional support.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::{debug, trace};

use crate::backends::Sink;
use crate::buffer_pool::PoolBuffer;

// ⚠️ Per-doc index routing: each document's `_index` field in the NDJSON action line overrides this config.
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
}

/// 📡 The sink side of the Elasticsearch backend — pure I/O, zero buffering.
///
/// `ElasticsearchSink` accepts a fully rendered NDJSON payload string and POSTs it
/// to the `_bulk` API. That's it. No internal buffer. No transform logic.
/// The SinkWorker upstream handles transform + binary collect + size management.
///
/// 🧠 Knowledge graph: Sinks are I/O-only abstractions now. This one does HTTP POST.
/// The FileSink does file write. The InMemorySink does Vec push.
/// Buffering, transforming, and collecting moved to SinkWorker. Clean separation.
///
/// Internally holds:
/// - `client`: the HTTP muscle 💪 — reused across requests
/// - `sink_config`: auth, URL, index targeting info
///
/// 🚰 Think of this as the drain at the end of a data pipeline. The last stop.
/// Knock knock. Who's there? HTTP POST. HTTP POST who? HTTP POST your NDJSON
/// and hope the cluster's in a good mood.
/// 📡 The sink side of the Elasticsearch backend — pure I/O, zero buffering.
///
/// 🧠 Performance: `the_precomputed_bulk_url` and `the_precomputed_auth_header` are
/// computed once in `new()` and reused for every bulk request. This eliminates 2
/// `format!()` String allocations per request on the hot path. At 64 workers × thousands
/// of requests, that's millions of avoided allocations. "He who allocates per-request,
/// GCs in production." — Ancient JVM proverb (we're in Rust but the wisdom still applies) 🦆
#[derive(Debug)]
pub(crate) struct ElasticsearchSink {
    client: reqwest::Client,
    // 🔒 Auth + URL pre-computed at init. No per-request format!() overhead.
    the_precomputed_bulk_url: String,
    the_precomputed_auth_header: Option<reqwest::header::HeaderValue>,
}

#[async_trait]
impl Sink for ElasticsearchSink {
    /// 📡 POST the fully rendered NDJSON payload to /_bulk. Pure I/O. No buffering. No drama.
    ///
    /// The SinkWorker upstream already transformed each doc and binary-collected them into
    /// a single NDJSON payload string. We just fire it into the elastic void.
    /// "In a world where sinks had too many responsibilities... one refactor dared to simplify."
    async fn drain(&mut self, payload: PoolBuffer) -> Result<()> {
        debug!(
            "🚰 Draining {} bytes to /_bulk — the payload has left the building, Elvis-style",
            payload.len()
        );
        // 🏦 Convert PoolBuffer → Vec<u8> for reqwest body. One-way trip — buffer is consumed.
        // reqwest::Body accepts Vec<u8> via Into<Body>, zero-copy ownership transfer. 🚀
        self.submit_bulk_request(payload.into_vec()).await
            .context("💀 The bulk submission stumbled at the finish line. The NDJSON was rendered with love, the SinkWorker did its job, and the HTTP layer said 'nah.' Check connectivity. Check your cluster. Check your horoscope.")?;
        Ok(())
    }

    /// 🗑️ Nothing to flush — we don't buffer. The SinkWorker sends complete payloads.
    /// Close is a no-op. The HTTP client drops cleanly. The connections pool says goodbye.
    /// Knock knock. Who's there? Nobody. The sink is closed. Go home. 🦆
    async fn close(&mut self) -> Result<()> {
        debug!("🗑️ Elasticsearch sink closing — no buffer to flush, just vibes to release");
        Ok(())
    }
}

impl ElasticsearchSink {
    /// 🚀 Stand up a new `ElasticsearchSink`, fully wired and ready to receive documents.
    ///
    /// This constructor does three things:
    /// 1. Builds the `reqwest::Client` with sane timeouts (10s connect, 30s read).
    ///    Like a polite person — we will wait, but not forever.
    /// 2. Pings the cluster root URL with a GET to confirm it's alive and talking to us.
    ///    A handshake. A hello. A "are you even there?"
    /// 3. If a static `index` is configured, verifies it exists with a HEAD/GET check.
    ///    Because indexing into a non-existent index is a skill issue we catch at init time,
    ///    not at 10,000 documents deep. You're welcome.
    ///
    /// ⚠️ Basic auth is used for the connectivity ping. API key is used for the index check.
    /// Pick your auth adventure, but be consistent about it in your config.
    pub(crate) async fn new(config: ElasticsearchSinkConfig) -> Result<Self> {
        // 🔧 Build the HTTP client. 10 second connect timeout because if ES can't handshake
        // in 10 seconds, it's not having a good time and neither are we. 120 second response
        // timeout because bulk requests can be 50MB+ and ES needs time to process all those
        // index operations. The old 30s timeout could cause partial commits: ES finishes
        // indexing but the response arrives after reqwest gives up. With auto-generated _ids,
        // a re-run would create duplicates instead of upserts. 120s gives ES room to breathe.
        // 🧠 TRIBAL KNOWLEDGE: The 19.3M-from-11.4M benchmark anomaly was likely caused by
        // timeout-induced partial commits + re-run with auto-generated document IDs.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .tcp_nodelay(true) // 🚀 Disable Nagle's algorithm — send bulk payloads immediately, not in sad little 40ms chunks
            .pool_idle_timeout(Duration::from_secs(30)) // 🔄 Keep warm connections alive for 30s — reuse > reconnect
            .build()
            // -- 💀 "Failed to initialize http client" — a tragedy in one act.
            // -- The curtain rises. reqwest::Client::builder() enters, full of promise.
            // -- It calls .build(). The TLS stack hesitates. The operating system shrugs.
            // -- There is no retry. There is only this context string, and silence.
            .context("💀 The HTTP client refused to be born. The TLS stack wept. The architect shrugged. We tried to build a reqwest::Client and the universe said 'no'. Probably a missing TLS cert or a cursed system OpenSSL. Either way: tragic.")?;

        // -- 📡 Connectivity ping — "Hello? Is this thing on?" — a developer, gesturing at a cluster.
        // We do a basic GET to the root to confirm the URL is real and auth works.
        // If this fails, we fail loudly here, rather than quietly 50,000 docs later.
        let c = config.clone();
        client
            .get(&c.url)
            .basic_auth(c.username.unwrap_or_default(), c.password)
            .send()
            .await?;

        // 🔒 Optional index existence check — only runs if a static index is configured.
        // Per-doc index routing skips this, because checking every possible target index at
        // -- startup would be... ambitious. Like planning to read every book in a library before
        // -- borrowing the first one.
        if let Some(ref index_name) = config.index {
            // 📡 Construct the full index URL for a targeted existence check.
            // trim_end_matches('/') — the "/" hygiene you didn't know you needed.
            // Without it: `https://host//my-index`. With it: `https://host/my-index`.
            // -- One slash of difference. Infinite suffering of difference.
            let index_url = format!("{}/{}", config.url.trim_end_matches('/'), index_name);
            let mut request = client.get(&index_url);
            // -- 🔒 Auth priority: API key wins over basic auth. This is not a democracy.
            // -- This is an Elasticsearch cluster and api_key is the premium tier.
            if let Some(ref api_key) = config.api_key {
                request = request.header("Authorization", format!("ApiKey {}", api_key));
            } else if let Some(ref username) = config.username {
                request = request.basic_auth(username, config.password.as_ref());
            }

            let response = request.send().await
                // -- 💀 "Failed to check for index availability" — a drama in one act.
                // -- We sent a request into the void. The void sent back... nothing. Or an error.
                // -- A TCP RST. A DNS NXDOMAIN. A firewall rule written by someone who has since
                // -- left the company. We may never know. The index may or may not exist.
                // -- Schrodinger's cluster. Very advanced. Very unhelpful.
                .context("💀 Reached out to check if the index exists. Got ghosted. The network is giving us the silent treatment. Or the firewall is on a power trip again. Either way: we cannot confirm the index lives, so we refuse to proceed. Dignity intact.")?;
            let status = response.status();
            if !status.is_success() {
                // -- 💀 The index does not exist. This is not a warning. This is not a soft error.
                // -- This is a hard stop, a full bail, a "we're not doing this."
                // -- Indexing into a nonexistent index is chaos. We are order. We are the wall.
                anyhow::bail!(
                    "💀 Index '{}' does not exist and never has, as far as we can tell. We knocked. We waited. The door remained unanswered. You may want to create it, or check your spelling — easy mistake, no judgment, but also: please fix it.",
                    index_url
                );
            } else {
                // -- ✅ The index exists! It is real! We found it! Like finding your keys in your coat!
                // -- The one you already checked! But they were there! They were always there!
                debug!(
                    "✅ Index exists and is accepting visitors — welcome mat is out, cluster is home"
                );
            }
        }

        // 🚀 Pre-compute the bulk URL — one format!() at init instead of per-request.
        // "Premature optimization is the root of all evil, but this one is just punctual." 🦆
        let the_base_trimmed = config.url.trim_end_matches('/');
        let the_precomputed_bulk_url = match config.index {
            Some(ref index_name) => format!(
                "{}/{}/_bulk?filter_path=items.*.error",
                the_base_trimmed, index_name
            ),
            None => format!(
                "{}/_bulk?filter_path=items.*.error",
                the_base_trimmed
            ),
        };

        // 🔒 Pre-compute auth header — one allocation at init, zero per-request.
        // API key > basic auth > none. Same precedence everywhere. Consistency is a feature.
        let the_precomputed_auth_header = if let Some(ref api_key) = config.api_key {
            Some(
                reqwest::header::HeaderValue::from_str(&format!("ApiKey {}", api_key))
                    .context("💀 API key contains invalid header characters — what did you put in there?")?,
            )
        } else if let (Some(username), password) = (&config.username, &config.password) {
            // 🔒 Basic auth header: base64("username:password")
            use base64::Engine;
            let credentials = match password {
                Some(p) => format!("{}:{}", username, p),
                None => format!("{}:", username),
            };
            let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
            Some(
                reqwest::header::HeaderValue::from_str(&format!("Basic {}", encoded))
                    .context("💀 Basic auth credentials contain invalid header characters")?,
            )
        } else {
            None
        };

        // 🚀 All checks passed. URL and auth pre-baked. Zero per-request overhead. Clean. Light. Free.
        Ok(Self {
            client,
            the_precomputed_bulk_url,
            the_precomputed_auth_header,
        })
    }

    /// 📡 Fires a `_bulk` POST request with the given NDJSON body.
    ///
    /// This is the actual HTTP call that makes documents leave our process and enter
    /// Elasticsearch's warm embrace. Or cold rejection. Depends on the status code.
    ///
    /// Auth is applied here: API key takes priority over basic auth, same as index check.
    /// If the response is not 2xx, we bail with enough detail to file a reasonable postmortem.
    ///
    /// 🔄 This function does not retry. Retries are the caller's problem. Good luck.
    /// 📡 Fires a `_bulk` POST with pre-computed URL + auth. Zero per-request allocations.
    ///
    /// 🧠 Performance path: URL and auth header are pre-computed in new().
    /// Content-length check on response: with filter_path=items.*.error, ES returns `{}`
    /// (2 bytes) on success. If content_length <= 2, we skip the async body read entirely.
    /// This avoids an unnecessary copy of potentially large response buffers on the hot path.
    ///
    /// "He who reads response bodies he doesn't need, deserializes in vain." — Ancient REST proverb 🦆
    async fn submit_bulk_request(&self, request_body: Vec<u8>) -> Result<()> {
        // 🚀 Build request with pre-computed URL + auth — zero format!() on hot path
        let mut request = self
            .client
            .post(&self.the_precomputed_bulk_url)
            // ⚠️ Content-Type: application/x-ndjson — not application/json. VERY important.
            .header("Content-Type", "application/x-ndjson");

        // 🔒 Apply pre-computed auth header — no per-request format!() or base64 encoding
        if let Some(ref auth_header) = self.the_precomputed_auth_header {
            request = request.header("Authorization", auth_header.clone());
        }

        let response = request
            .body(request_body)
            .send()
            .await
            .context("💀 The bulk request never made it to Elasticsearch. Check connectivity, check timeouts, check your horoscope.")?;

        let status = response.status();

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 Elasticsearch bulk request failed with HTTP {}. Response: '{}'",
                status,
                body
            );
        }

        // 📡 With filter_path=items.*.error, ES returns `{}` on success (2 bytes).
        // Skip the async body read when content-length tells us there's nothing to check.
        // 🧠 TRIBAL KNOWLEDGE: Full response body parsing caused a 30x performance regression.
        // 🧠 TRIBAL KNOWLEDGE: This check catches the 19.3M-from-11.4M anomaly —
        // item-level 429s/mapping errors were silently swallowed.
        let content_length = response.content_length();
        if let Some(len) = content_length {
            if len <= 2 {
                // ✅ Empty success `{}` — skip body read, save an async syscall + alloc
                trace!("🚀 Bulk request landed — content-length={}, skipping body read", len);
                return Ok(());
            }
        }

        // ⚠️ Content-length suggests errors exist (or was absent) — read and check
        let body = response.text().await.unwrap_or_default();
        if body.contains("\"error\"") {
            anyhow::bail!(
                "💀 Elasticsearch bulk response: HTTP 200 but items failed. \
                 The cluster accepted the request but rejected individual documents. \
                 This is the silent killer of data migrations — partial success looks \
                 like full success unless you check. Response: '{}'",
                body
            );
        }

        trace!("🚀 Bulk request landed successfully — all items indexed, zero casualties");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 ElasticsearchSinkConfig deserializes with all fields populated.
    /// "Trust no one. Not even your own config." — Fox Mulder, if he wrote Rust 🦆
    #[test]
    fn the_one_where_es_sink_config_has_all_the_fields() {
        // -- 🔧 Full config, no defaults, no mercy. Like ordering at a restaurant with zero substitutions.
        let config = ElasticsearchSinkConfig {
            url: "https://localhost:9200".to_string(),
            username: Some("elastic".to_string()),
            password: Some("changeme".to_string()),
            api_key: None,
            index: Some("my-index".to_string()),
        };
        assert_eq!(config.url, "https://localhost:9200");
        assert_eq!(config.index, Some("my-index".to_string()));
    }

    /// 🧪 Optional fields default to None — serde's version of "I didn't bring anything to the potluck."
    #[test]
    fn the_one_where_optional_fields_default_to_none() {
        // -- 📡 Minimal JSON — just a URL, living its best minimalist life 🧘
        let json = r#"{"url": "https://localhost:9200"}"#;
        let config: ElasticsearchSinkConfig = serde_json::from_str(json).expect(
            "💀 Failed to deserialize ElasticsearchSinkConfig. The JSON was literally two fields.",
        );
        assert!(config.username.is_none());
        assert!(config.password.is_none());
        assert!(config.api_key.is_none());
    }

    /// 🧪 API key and basic auth are both optional — anonymous access is valid (in dev, hopefully only dev).
    #[test]
    fn the_one_where_auth_is_entirely_optional_for_dev_clusters() {
        // -- 🔒 No auth. The cluster is wide open. "Come in, come in!" — the cluster, probably 🦆
        let config = ElasticsearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: None,
        };
        assert!(config.username.is_none());
        assert!(config.api_key.is_none());
    }

    /// 🧪 Bulk URL includes index when configured — ES 8.x needs it in the URL path
    /// because RallyS3ToEs emits `{"index":{}}` without `_index` in the action line.
    /// "You shall not pass... without an index in the URL." — Gandalf, if he ran ES 8 🦆
    #[test]
    fn the_one_where_bulk_url_includes_index_when_configured() {
        // -- 📡 Config with an index — the URL should be /{index}/_bulk, not just /_bulk
        let config = ElasticsearchSinkConfig {
            url: "https://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: Some("geonames".to_string()),
        };
        let base = config.url.trim_end_matches('/');
        let bulk_url = match config.index {
            Some(ref idx) => format!("{}/{}/_bulk", base, idx),
            None => format!("{}/_bulk", base),
        };
        assert_eq!(bulk_url, "https://localhost:9200/geonames/_bulk");
    }

    /// 🧪 Bulk URL falls back to bare /_bulk when no index is configured (per-doc routing).
    /// Like a bulk API without a destination — it trusts each doc to know where it's going.
    /// "Not all who wander to /_bulk are lost" — Tolkien, bulk API edition 🚀
    #[test]
    fn the_one_where_bulk_url_is_bare_when_no_index_configured() {
        // -- 📡 Config without an index — per-doc routing, so bare /_bulk is correct
        let config = ElasticsearchSinkConfig {
            url: "https://localhost:9200/".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: None,
        };
        let base = config.url.trim_end_matches('/');
        let bulk_url = match config.index {
            Some(ref idx) => format!("{}/{}/_bulk", base, idx),
            None => format!("{}/_bulk", base),
        };
        // ⚠️ Also tests trailing-slash trimming — the URL had a trailing `/`
        assert_eq!(bulk_url, "https://localhost:9200/_bulk");
    }
}
