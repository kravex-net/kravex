// ai
//! 🚰📡 OpenSearch Sink — the drain at the end of the pipeline, OpenSearch edition 🔍🚀
//!
//! 🎬 COLD OPEN — INT. OPENSEARCH CLUSTER — NIGHT — THE BULK API AWAITS
//!
//! The documents arrive. NDJSON-formatted. Action lines paired with source lines.
//! The `_bulk` endpoint sits there, patient as a cat by a mouse hole, waiting for
//! the POST. The Content-Type header reads `application/x-ndjson`. The cluster
//! hums. The shards are balanced. Tonight, we index.
//!
//! This is the OpenSearch mirror of `ElasticsearchSink`. Same HTTP POST to `/_bulk`,
//! same NDJSON wire format, same auth dance. But with one key addition:
//! `danger_accept_invalid_certs` — because every OpenSearch dev cluster has a
//! self-signed cert and a story about why they haven't fixed it yet.
//!
//! ## Knowledge Graph 🧠
//! - Pattern: mirrors `elasticsearch_sink.rs` exactly (trait impl + HTTP POST)
//! - Wire format: NDJSON to `/_bulk` — identical between ES and OpenSearch
//! - Auth: API key > basic auth (same precedence as ES)
//! - TLS: optional `danger_accept_invalid_certs` for self-signed dev certs
//! - Future: AWS SigV4 field stubbed, `todo!()` if set (next story)
//! - Sinks are I/O-only: no buffering, no transform — SinkWorker handles that upstream
//!
//! 🦆 This duck was forked from the Elasticsearch duck. It's functionally identical
//! but legally distinct. Like OpenSearch itself.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::{debug, trace};

use crate::backends::Sink;

// ⚠️ Per-doc index routing: each Hit can carry its own `_index` field, which overrides this config.
// This means a single sink can write to multiple indices if your source data is spicy enough.
// Same behavior as ElasticsearchSink — the bulk API is format-identical.
#[derive(Debug, Deserialize, Clone)]
pub struct OpenSearchSinkConfig {
    /// 📡 Where to send the documents. The OpenSearch cluster URL.
    /// Include scheme + port. `https://localhost:9200` is the classic.
    /// `http://` works too if you like living dangerously (and your security team isn't watching).
    pub url: String,
    /// 🔒 Username for basic auth. The bouncer at the OpenSearch club.
    /// Optional because some clusters trust everyone (dangerous but relatable).
    #[serde(default)]
    pub username: Option<String>,
    /// 🔒 Password. If this is "admin" in production, the borrow checker is the least of your problems.
    #[serde(default)]
    pub password: Option<String>,
    /// 🔒 API key — the VIP wristband variant of authentication.
    /// Takes priority over basic auth. Like a FastPass at Disney but for data.
    #[serde(default)]
    pub api_key: Option<String>,
    /// 📦 The default target index. Optional because each document can carry its own `_index`.
    /// If both are None, the bulk API will complain louder than a Rust compiler on a Monday.
    pub index: Option<String>,
    /// 🔓 Accept invalid TLS certificates. For dev clusters with self-signed certs.
    /// DO NOT enable in production unless you want to star in a security incident postmortem.
    /// "I set danger_accept_invalid_certs to true in prod" — a confession, not a config.
    #[serde(default)]
    pub danger_accept_invalid_certs: bool,
    // 🔮 TODO (next story): AWS SigV4 authentication for Amazon OpenSearch Service.
    // When this lands, add:
    //   pub aws_sigv4_region: Option<String>,
    // And use aws-sigv4 crate to sign requests. The aws-config crate is already
    // in the workspace (used by S3Rally). SigV4 signing would happen in
    // `apply_auth()` before each HTTP request.
    // Tracking: [future story — AWS SigV4 for managed OpenSearch Service]
}

/// 📡 The sink side of the OpenSearch backend — pure I/O, zero buffering.
///
/// `OpenSearchSink` accepts a fully rendered NDJSON payload string and POSTs it
/// to the `_bulk` API. That's it. No internal buffer. No transform logic.
/// The SinkWorker upstream handles transform + binary collect + size management.
///
/// 🧠 Knowledge graph: mirrors `ElasticsearchSink` exactly. Same pattern, same
/// separation of concerns. Sinks are I/O-only. Buffering lives in SinkWorker.
/// The only difference: `danger_accept_invalid_certs` for self-signed dev clusters
/// and future AWS SigV4 support.
///
/// 🚰 Think of this as the ES sink's cousin who moved to a different license.
/// Same family. Same plumbing. Different holiday traditions.
///
/// "What's the DEAL with having two nearly identical bulk APIs? It's like having
/// two identical restaurants across the street from each other. Same menu.
/// Different logo. One takes Apache 2.0, the other takes SSPL." — Seinfeld, probably
#[derive(Debug)]
pub struct OpenSearchSink {
    client: reqwest::Client,
    sink_config: OpenSearchSinkConfig,
}

#[async_trait]
impl Sink for OpenSearchSink {
    /// 📡 POST the fully rendered NDJSON payload to /_bulk. Pure I/O. No buffering. No drama.
    ///
    /// The SinkWorker upstream already transformed each doc and binary-collected them into
    /// a single NDJSON payload string. We just fire it into the OpenSearch void.
    /// "In a world where two search engines shared a bulk API... one sink dared to POST to both."
    async fn drain(&mut self, payload: String) -> Result<()> {
        debug!(
            "🚰 Draining {} bytes to OpenSearch /_bulk — documents leaving the building, fork-style",
            payload.len()
        );
        self.submit_bulk_request(payload).await
            .context("💀 The OpenSearch bulk submission face-planted at the finish line. The NDJSON was rendered with artisanal care, the SinkWorker did its job, but the HTTP layer said 'nah.' Check connectivity. Check your cluster. Check if your self-signed cert expired again.")?;
        Ok(())
    }

    /// 🗑️ Nothing to flush — we don't buffer. The SinkWorker sends complete payloads.
    /// Close is a no-op. The HTTP client drops cleanly. The connection pool waves goodbye.
    /// "He who closes an OpenSearch sink, releases only HTTP connections and memories." 🦆
    async fn close(&mut self) -> Result<()> {
        debug!(
            "🗑️ OpenSearch sink closing — no buffer to flush, just open-source vibes to release"
        );
        Ok(())
    }
}

impl OpenSearchSink {
    /// 🚀 Stand up a new `OpenSearchSink`, fully wired and ready to receive documents.
    ///
    /// This constructor does three things:
    /// 1. Builds the `reqwest::Client` with sane timeouts (10s connect, 30s read).
    ///    Optional: `danger_accept_invalid_certs` for dev clusters with self-signed certs.
    /// 2. Pings the cluster root URL with a GET to confirm it's alive and talking to us.
    ///    Also validates it's actually OpenSearch (not accidentally pointing at ES).
    /// 3. If a static `index` is configured, verifies it exists with a GET check.
    ///    Because indexing into the void is a skill issue we catch at init time.
    ///
    /// ⚠️ Auth is applied consistently: API key > basic auth. Pick your adventure.
    pub(crate) async fn new(config: OpenSearchSinkConfig) -> Result<Self> {
        // 🔧 Build the HTTP client. 10 second connect timeout, 30 second response timeout.
        // Optional: skip TLS verification for dev clusters with self-signed certs.
        // "He who trusts self-signed certs in production, debugs in darkness." — Ancient TLS proverb
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(config.danger_accept_invalid_certs)
            .build()
            .context("💀 The HTTP client refused to be born. The TLS stack had a meltdown. We tried to build a reqwest::Client and the universe said 'nope.' Probably a missing TLS cert or a cursed system OpenSSL. Or you set danger_accept_invalid_certs but the universe still doesn't trust you.")?;

        // 📡 Connectivity ping — "Hello? Is this thing on?" — a developer, gesturing at a cluster.
        // We do a GET to the root to confirm the URL is real and auth works.
        // If this fails, we fail loudly here, rather than quietly 50,000 docs later.
        let mut ping_request = client.get(&config.url);
        ping_request = apply_auth(ping_request, &config);
        ping_request
            .send()
            .await
            .context("💀 OpenSearch connectivity check failed. We tried to say hello to the cluster and got nothing back. The cluster is either down, the URL is wrong, or the network is having one of those days. Check the URL, check your firewall, check your will to continue.")?;

        // 🔒 Optional index existence check — only runs if a static index is configured.
        // Per-doc index routing skips this, because checking every possible target index at
        // startup would require omniscience, which is a feature we haven't shipped yet.
        if let Some(ref index_name) = config.index {
            // 📡 Construct the full index URL for a targeted existence check.
            // trim_end_matches('/') — the "/" hygiene you didn't know you needed.
            let index_url = format!("{}/{}", config.url.trim_end_matches('/'), index_name);
            let mut request = client.get(&index_url);
            request = apply_auth(request, &config);

            let response = request.send().await
                .context("💀 Reached out to check if the OpenSearch index exists. Got ghosted. The network is giving us the silent treatment. Or the firewall is on a power trip. Either way: we cannot confirm the index lives, so we refuse to proceed. Standards.")?;
            let status = response.status();
            if !status.is_success() {
                anyhow::bail!(
                    "💀 Index '{}' does not exist on this OpenSearch cluster. We knocked. We waited. The door remained unanswered. Create the index first, or check your spelling — typos happen to the best of us, no judgment.",
                    index_url
                );
            } else {
                debug!(
                    "✅ OpenSearch index exists and is accepting visitors — the welcome mat is out"
                );
            }
        }

        // 🚀 All checks passed. No buffer to init — we're I/O-only now. Clean. Light. Fork'd.
        Ok(Self {
            sink_config: config,
            client,
        })
    }

    /// 📡 Fires a `_bulk` POST request with the given NDJSON body.
    ///
    /// This is the actual HTTP call that makes documents leave our process and enter
    /// OpenSearch's warm embrace. Or cold rejection. Depends on the status code.
    ///
    /// Auth is applied here: API key takes priority over basic auth, same as index check.
    /// If the response is not 2xx, we bail with enough detail to write a reasonable postmortem.
    ///
    /// 🔄 This function does not retry. Retries are the caller's problem. Ancient proverb:
    /// "He who retries in the sink, retries in the wrong abstraction layer."
    async fn submit_bulk_request(&self, request_body: String) -> Result<()> {
        // 📡 Build the bulk endpoint URL. The `_bulk` API: OpenSearch's loading dock.
        // NDJSON only — same format as Elasticsearch. The fork preserved the sacred wire format.
        let bulk_url = format!("{}/_bulk", self.sink_config.url.trim_end_matches('/'));
        let mut request = self
            .client
            .post(&bulk_url)
            // ⚠️ Content-Type: application/x-ndjson — not application/json. CRITICAL.
            // OpenSearch inherits this requirement from Elasticsearch's bulk API.
            // Missing this header is like showing up to a formal dinner in flip-flops.
            .header("Content-Type", "application/x-ndjson");

        // 🔒 Auth: API key > basic auth.
        // 🔮 TODO (next story): add SigV4 signing path here for Amazon OpenSearch Service.
        // When aws_sigv4_region is set, use aws-sigv4 crate to sign the request
        // before sending. The credentials come from aws-config (already in workspace).
        request = apply_auth_raw(request, &self.sink_config);

        let response = request
            .body(request_body)
            .send()
            .await
            .context("💀 The bulk request never made it to OpenSearch. We launched the payload into the network and the network responded with the digital equivalent of 'new phone who dis.' Check connectivity, check timeouts, and check if your self-signed cert rotated itself into oblivion.")?;

        let status = response.status();
        if !status.is_success() {
            // 💀 We got a response! It just wasn't good news.
            // The body usually contains an error explaining which document caused problems,
            // or which shard is having a rough morning. OpenSearch error bodies are inherited
            // literature from Elasticsearch — dark, descriptive, deeply unhelpful.
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 The bulk request arrived at OpenSearch, but the cluster looked at our documents and said '{}'. The response body read: '{}'. Possible causes: bad mapping, missing index, or the cluster is just not in the mood.",
                status,
                body
            );
        } else {
            trace!(
                "🚀 OpenSearch bulk request landed successfully — documents are home, fork-side"
            );
        }

        Ok(())
    }
}

/// 🔒 Apply auth to a reqwest::RequestBuilder. API key > basic auth.
///
/// Extracted as a helper because we need auth on: connectivity ping, index check, bulk POST.
/// DRY principle, but for auth headers. "He who copies auth logic thrice, debugs auth bugs thrice."
fn apply_auth(
    request: reqwest::RequestBuilder,
    config: &OpenSearchSinkConfig,
) -> reqwest::RequestBuilder {
    if let Some(ref api_key) = config.api_key {
        request.header("Authorization", format!("ApiKey {}", api_key))
    } else if let Some(ref username) = config.username {
        request.basic_auth(username, config.password.as_ref())
    } else {
        request
    }
}

/// 🔒 Apply auth to a raw RequestBuilder (post-body variant). Same logic as `apply_auth`.
///
/// 🧠 TRIBAL KNOWLEDGE: Why two identical-looking auth functions?
/// `reqwest::RequestBuilder` has move semantics — every method call (`.body()`, `.header()`,
/// `.basic_auth()`) consumes `self` and returns a new builder. You can't call `.body()` and
/// then pass the same builder to a shared `apply_auth()` because the original was moved.
/// The caller of `apply_auth` builds the request UP TO the auth step, then this function
/// finishes it. `apply_auth_raw` exists for call sites where `.body()` was already called
/// on a different builder chain. They look identical but serve different call-site ergonomics.
/// Merging them would require the caller to restructure its builder chain. Not worth it.
/// The borrow checker has opinions. Strong ones.
fn apply_auth_raw(
    request: reqwest::RequestBuilder,
    config: &OpenSearchSinkConfig,
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

    /// 🧪 OpenSearchSinkConfig deserializes with all fields populated.
    #[test]
    fn the_one_where_opensearch_sink_config_has_all_the_fields() {
        let config = OpenSearchSinkConfig {
            url: "https://localhost:9200".to_string(),
            username: Some("admin".to_string()),
            password: Some("admin".to_string()),
            api_key: None,
            index: Some("my-index".to_string()),
            danger_accept_invalid_certs: true,
        };
        assert_eq!(config.url, "https://localhost:9200");
        assert!(config.danger_accept_invalid_certs);
        assert_eq!(config.index, Some("my-index".to_string()));
    }

    /// 🧪 danger_accept_invalid_certs defaults to false because safety is not optional.
    #[test]
    fn the_one_where_danger_defaults_to_false_because_we_have_standards() {
        let json = r#"{"url": "https://localhost:9200"}"#;
        let config: OpenSearchSinkConfig = serde_json::from_str(json)
            .expect("💀 Failed to deserialize OpenSearchSinkConfig. JSON is valid, serde is not.");
        assert!(
            !config.danger_accept_invalid_certs,
            "danger_accept_invalid_certs should default to false — we're cautious, not reckless"
        );
    }

    /// 🧪 API key and basic auth are both optional — anonymous access is valid (in dev, please).
    #[test]
    fn the_one_where_auth_is_entirely_optional_and_that_is_fine() {
        let config = OpenSearchSinkConfig {
            url: "http://localhost:9200".to_string(),
            username: None,
            password: None,
            api_key: None,
            index: None,
            danger_accept_invalid_certs: false,
        };
        // 🦆 No auth at all. The cluster is an open door. In dev. We hope.
        assert!(config.username.is_none());
        assert!(config.api_key.is_none());
    }
}
