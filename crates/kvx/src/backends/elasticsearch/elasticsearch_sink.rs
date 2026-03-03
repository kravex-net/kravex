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
#[derive(Debug)]
pub(crate) struct ElasticsearchSink {
    client: reqwest::Client,
    sink_config: ElasticsearchSinkConfig,
}

#[async_trait]
impl Sink for ElasticsearchSink {
    /// 📡 POST the fully rendered NDJSON payload to /_bulk. Pure I/O. No buffering. No drama.
    ///
    /// The SinkWorker upstream already transformed each doc and binary-collected them into
    /// a single NDJSON payload string. We just fire it into the elastic void.
    /// "In a world where sinks had too many responsibilities... one refactor dared to simplify."
    async fn drain(&mut self, payload: String) -> Result<()> {
        debug!(
            "🚰 Draining {} bytes to /_bulk — the payload has left the building, Elvis-style",
            payload.len()
        );
        self.submit_bulk_request(payload).await
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
        // in 10 seconds, it's not having a good time and neither are we. 30 second response
        // timeout because bulk requests can be meaty and we're not monsters.
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
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

        // 🚀 All checks passed. No buffer to init — we're I/O-only now. Clean. Light. Free.
        Ok(Self {
            sink_config: config,
            client,
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
    async fn submit_bulk_request(&self, request_body: String) -> Result<()> {
        // -- 📡 Build the bulk endpoint URL. The `_bulk` API: Elasticsearch's loading dock.
        // -- NDJSON only — no JSON arrays, no XML, no CSV, no hand-coded tab-separated values.
        // -- NDJSON. The only format Elasticsearch respects. Truly the format of people who
        // -- wanted JSON but also wanted to feel slightly superior about it.
        let bulk_url = format!("{}/_bulk", self.sink_config.url.trim_end_matches('/'));
        let mut request = self
            .client
            .post(&bulk_url)
            // ⚠️ Content-Type: application/x-ndjson — not application/json. VERY important.
            // Elasticsearch will return a 406 or silently misbehave without this header.
            // -- The x- prefix means "we made this up but we're committing to it." Classic.
            .header("Content-Type", "application/x-ndjson");

        // -- 🔒 Same auth dance as the index check — api_key beats basic auth in this club.
        if let Some(ref api_key) = self.sink_config.api_key {
            request = request.header("Authorization", format!("ApiKey {}", api_key));
        } else if let Some(ref username) = self.sink_config.username {
            request = request.basic_auth(username, self.sink_config.password.as_ref());
        }

        let response = request
            .body(request_body)
            .send()
            .await
            // -- 💀 "Failed to send bulk request" — micro-fiction, act one.
            // -- We gathered the documents. We serialized them. We built the NDJSON.
            // -- We formed the HTTP request with artisanal care. We called .send().
            // -- And the network layer, that capricious deity of bytes and routing tables,
            // -- looked upon our work... and dropped the packet. No response. No closure.
            // -- Just an Err. Like sending a love letter and getting a ECONNRESET back.
            .context("💀 The bulk request never made it to Elasticsearch. We launched the payload into the network and the network responded with what can only be described as 'not vibing with it.' Check connectivity, check timeouts, and check your feelings.")?;

        let status = response.status();
        if !status.is_success() {
            // -- 💀 We got a response! It just... wasn't good news.
            // The body is fetched for context — it usually contains an 'error' object
            // explaining which document caused the problem, or which shard is having
            // -- a rough morning. Elasticsearch error bodies are poetry. Dark poetry.
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!(
                "💀 The bulk request arrived, but Elasticsearch looked at our documents and said '{}'. The body of the response read: '{}'. We have no one to blame but ourselves, and possibly whoever wrote the mapping.",
                status,
                body
            );
        } else {
            // -- ✅ Sent! Gone! Into the index! No cap, this function absolutely slapped.
            trace!(
                "🚀 Bulk request landed successfully — documents have left the building, Elvis-style"
            );
        }

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
}
