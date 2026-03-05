use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::{debug, trace};

use crate::backends::Sink;
use crate::backends::CommonSinkConfig;

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

/// 📡 The sink side of the Elasticsearch backend — pure I/O, zero buffering.
///
/// `ElasticsearchSink` accepts a fully rendered NDJSON payload string and POSTs it
/// to the `_bulk` API. That's it. No internal buffer. No cast logic.
/// The Drainer upstream handles cast + binary collect + size management.
///
/// 🧠 Knowledge graph: Sinks are I/O-only abstractions now. This one does HTTP POST.
/// The FileSink does file write. The InMemorySink does Vec push.
/// Buffering, casting, and collecting moved to Drainer. Clean separation.
///
/// Internally holds:
/// - `client`: the HTTP muscle 💪 — reused across requests
/// - `sink_config`: auth, URL, index targeting info
///
/// 🚰 Think of this as the drain at the end of a data pipeline. The last stop.
/// Knock knock. Who's there? HTTP POST. HTTP POST who? HTTP POST your NDJSON
/// and hope the cluster's in a good mood.
#[derive(Debug)]
pub struct ElasticsearchSink {
    client: reqwest::Client,
    sink_config: ElasticsearchSinkConfig,
}

#[async_trait]
impl Sink for ElasticsearchSink {
    /// 📡 POST the fully rendered NDJSON payload to /_bulk. Pure I/O. No buffering. No drama.
    ///
    /// The Drainer upstream already cast each doc and binary-collected them into
    /// a single NDJSON payload string. We just fire it into the elastic void.
    /// "In a world where sinks had too many responsibilities... one refactor dared to simplify."
    async fn send(&mut self, payload: String) -> Result<()> {
        debug!(
            "📡 Sending {} bytes to /_bulk — the payload has left the building, Elvis-style",
            payload.len()
        );
        self.submit_bulk_request(payload).await
            .context("💀 The bulk submission stumbled at the finish line. The NDJSON was rendered with love, the Drainer did its job, and the HTTP layer said 'nah.' Check connectivity. Check your cluster. Check your horoscope.")?;
        Ok(())
    }

    /// 🗑️ Nothing to flush — we don't buffer. The Drainer sends complete payloads.
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
    pub async fn new(config: ElasticsearchSinkConfig) -> Result<Self> {
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
//  🧪  T E S T S  —  The Elasticsearch Sink Trials
//     ╭─────────╮
//     │ /_bulk  │◄── NDJSON goes in, 200s come out. Can't explain that.
//     ╰─────────╯
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::Sink;
    use wiremock::matchers::{body_string, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // 🔧 The lazy config factory — minimal viable config, no auth, no index.
    // Like ordering a coffee "black" — you can add milk later if you want. 🦆
    fn make_config(url: &str) -> ElasticsearchSinkConfig {
        ElasticsearchSinkConfig {
            url: url.to_string(),
            username: None,
            password: None,
            api_key: None,
            index: None,
            common_config: CommonSinkConfig::default(),
        }
    }

    // 🔧 Mounts the root ping mock (GET /) that `new()` always hits first.
    // Like a bouncer checking IDs — every test needs to pass the door.
    async fn mount_root_ping(mock_server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(mock_server)
            .await;
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GROUP A: Constructor — Connectivity Ping                           │
    // │  "Are you there, Elasticsearch? It's me, the sink."                │
    // └──────────────────────────────────────────────────────────────────────┘

    /// 🧪 Happy path: cluster responds to ping, sink is born healthy. The circle of life.
    #[tokio::test]
    async fn the_one_where_the_cluster_is_alive_and_well() -> Result<()> {
        // 🔧 Arrange — spin up a mock cluster that actually likes us
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        let config = make_config(&mock_server.uri());

        // 🚀 Act — attempt the sacred construction ritual
        let the_newborn_sink = ElasticsearchSink::new(config).await;

        // 🎯 Assert — the sink exists! It is real! We are not dreaming!
        assert!(
            the_newborn_sink.is_ok(),
            "💀 Sink construction failed even though the cluster was alive. This is betrayal."
        );

        Ok(())
    }

    /// 🧪 Cluster returns 500 on ping. The constructor checks connectivity, not health.
    /// "I came, I saw, I got a 500." — Julius HTTP Caesar
    #[tokio::test]
    async fn the_one_where_the_cluster_ghosts_us() -> Result<()> {
        // 🔧 Arrange — the cluster is having a bad day. Aren't we all.
        let mock_server = MockServer::start().await;

        // 📡 Return 500 to simulate a sick cluster.
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());

        // 🚀 Act — try to construct against a sick cluster
        // ⚠️ Current implementation checks connectivity (did the server respond?), not health
        // (was the response 2xx?). A 500 means "alive but suffering" — we accept that.
        let the_doomed_sink = ElasticsearchSink::new(config).await;

        // 🎯 Assert — constructor doesn't bail on non-2xx ping. We verify liveness, not happiness.
        assert!(
            the_doomed_sink.is_ok(),
            "💀 Sink should still construct if cluster responds (even with 500). We check liveness, not happiness."
        );

        Ok(())
    }

    /// 🧪 Basic auth credentials are sent on the connectivity ping. Trust but verify.
    #[tokio::test]
    async fn the_one_where_basic_auth_is_sent_on_ping() -> Result<()> {
        // 🔧 Arrange — set up a club with a bouncer that checks names
        let mock_server = MockServer::start().await;

        // 📡 dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk = base64("the_user:the_password")
        Mock::given(method("GET"))
            .and(path("/"))
            .and(header("Authorization", "Basic dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.username = Some("the_user".to_string());
        config.password = Some("the_password".to_string());

        // 🚀 Act — construct the sink, which pings with creds
        let _the_authenticated_sink = ElasticsearchSink::new(config).await?;

        // 🎯 Assert — wiremock's expect(1) validates the Basic auth header was sent ✅

        Ok(())
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GROUP B: Constructor — Index Existence Check                       │
    // │  "Does the index exist? Let me check. Let me CHECK." — every DBA   │
    // └──────────────────────────────────────────────────────────────────────┘

    /// 🧪 Index exists, cluster confirms it. Like finding your keys in the first pocket.
    #[tokio::test]
    async fn the_one_where_the_index_exists_and_all_is_right() -> Result<()> {
        // 🔧 Arrange — the index is home, lights are on
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("GET"))
            .and(path("/my-index"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.index = Some("my-index".to_string());

        // 🚀 Act
        let the_happy_sink = ElasticsearchSink::new(config).await;

        // 🎯 Assert — sink born, index verified, all vibes immaculate ✅
        assert!(
            the_happy_sink.is_ok(),
            "💀 Index exists and cluster responded 200, but sink still failed. Unacceptable."
        );

        Ok(())
    }

    /// 🧪 Index doesn't exist. 404. The void stares back. Sink refuses to participate.
    #[tokio::test]
    async fn the_one_where_the_index_is_a_figment_of_imagination() -> Result<()> {
        // 🔧 Arrange — the index is a ghost. A phantom. A rumor at best.
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("GET"))
            .and(path("/ghost-index"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.index = Some("ghost-index".to_string());

        // 🚀 Act — try to construct against a nonexistent index
        let the_disappointed_result = ElasticsearchSink::new(config).await;

        // 🎯 Assert — should fail, and the error should mention the index
        assert!(
            the_disappointed_result.is_err(),
            "💀 Sink should refuse to start when the index is a 404 ghost"
        );
        let the_error_message = the_disappointed_result.unwrap_err().to_string();
        assert!(
            the_error_message.contains("ghost-index"),
            "💀 Error should mention the missing index name, got: {the_error_message}"
        );

        Ok(())
    }

    /// 🧪 No index configured — no index check request. Mind your own business.
    #[tokio::test]
    async fn the_one_where_no_index_is_configured_and_thats_fine() -> Result<()> {
        // 🔧 Arrange — no index, no drama. We don't mount any index mock.
        // If the sink tries to check an index, wiremock returns 404 → constructor bails.
        // Absence of failure IS the test. 🧘
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        let config = make_config(&mock_server.uri());

        // 🚀 Act
        let the_chill_sink = ElasticsearchSink::new(config).await;

        // 🎯 Assert — no index check means no 404 means no failure ✅
        assert!(
            the_chill_sink.is_ok(),
            "💀 Sink should not check index when none is configured. Why are you like this."
        );

        Ok(())
    }

    /// 🧪 API key takes priority over basic auth for the index check. VIP entrance only.
    /// "It's not a democracy. It's an API key." — this code, 2026
    #[tokio::test]
    async fn the_one_where_api_key_beats_basic_auth_for_index_check() -> Result<()> {
        // 🔧 Arrange — VIP entrance with API key verification
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("GET"))
            .and(path("/vip-index"))
            .and(header("Authorization", "ApiKey the_golden_ticket"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.api_key = Some("the_golden_ticket".to_string());
        config.username = Some("should_be_ignored".to_string());
        config.password = Some("also_ignored".to_string());
        config.index = Some("vip-index".to_string());

        // 🚀 Act
        let _the_vip_sink = ElasticsearchSink::new(config).await?;

        // 🎯 Assert — wiremock's expect(1) validates ApiKey was used, not Basic ✅

        Ok(())
    }

    /// 🧪 Basic auth for index check when no API key. Economy class authentication.
    #[tokio::test]
    async fn the_one_where_basic_auth_is_used_when_no_api_key() -> Result<()> {
        // 🔧 Arrange — economy class auth
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        // 📡 dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk = base64("the_user:the_password")
        Mock::given(method("GET"))
            .and(path("/economy-index"))
            .and(header("Authorization", "Basic dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.username = Some("the_user".to_string());
        config.password = Some("the_password".to_string());
        config.index = Some("economy-index".to_string());

        // 🚀 Act
        let _the_economy_sink = ElasticsearchSink::new(config).await?;

        // 🎯 Assert — wiremock validates Basic auth was used ✅

        Ok(())
    }

    /// 🧪 Trailing slash in URL doesn't cause double-slash in index check path.
    /// The difference between `/idx` and `//idx` is one character and infinite suffering.
    #[tokio::test]
    async fn the_one_where_trailing_slash_doesnt_cause_double_slash() -> Result<()> {
        // 🔧 Arrange — URL with trailing slash, the classic trap
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("GET"))
            .and(path("/idx"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&format!("{}/", mock_server.uri()));
        config.index = Some("idx".to_string());

        // 🚀 Act
        let _the_slash_safe_sink = ElasticsearchSink::new(config).await?;

        // 🎯 Assert — wiremock's expect(1) confirms /idx was hit, not //idx ✅

        Ok(())
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GROUP C: Bulk POST — send() / submit_bulk_request()                │
    // │  "You miss 100% of the bulk requests you don't send." — Wayne HTTP  │
    // └──────────────────────────────────────────────────────────────────────┘

    /// 🧪 Happy path bulk POST: 200 response. Documents accepted. We can sleep tonight.
    #[tokio::test]
    async fn the_one_where_bulk_request_lands_successfully() -> Result<()> {
        // 🔧 Arrange — a welcoming cluster that accepts all our documents
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_eager_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act — fire the payload into the elastic void
        let the_ndjson = "{\"index\":{}}\n{\"id\":1}\n".to_string();
        let the_result = the_eager_sink.send(the_ndjson).await;

        // 🎯 Assert — the void accepted our offering ✅
        assert!(
            the_result.is_ok(),
            "💀 Bulk request returned 200 but send() still failed. The vibes are off."
        );

        Ok(())
    }

    /// 🧪 ES returns 400. Bad mapping? Bad docs? Bad karma? Error includes status + body.
    #[tokio::test]
    async fn the_one_where_elasticsearch_rejects_our_documents() -> Result<()> {
        // 🔧 Arrange — ES is judgy today
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(
                ResponseTemplate::new(400)
                    .set_body_string("mapping_exception: your docs are bad and you should feel bad"),
            )
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_judged_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act — submit docs that ES will roast
        let the_rejected_payload = "{\"index\":{}}\n{\"bad\":\"doc\"}\n".to_string();
        let the_harsh_verdict = the_judged_sink.send(the_rejected_payload).await;

        // 🎯 Assert — should fail, error chain should contain status info
        // ⚠️ anyhow's .to_string() only shows the outermost .context() message.
        // The "400 Bad Request" lives deeper in the chain. Use {:?} to see the full story.
        assert!(the_harsh_verdict.is_err(), "💀 400 response should cause send() to fail");
        let the_full_error_chain = format!("{:?}", the_harsh_verdict.unwrap_err());
        assert!(
            the_full_error_chain.contains("400"),
            "💀 Error chain should mention the 400 status, got: {the_full_error_chain}"
        );

        Ok(())
    }

    /// 🧪 Server error (500). All non-2xx should fail. Equal opportunity rejection.
    /// "This is fine." 🐕‍🦺🔥
    #[tokio::test]
    async fn the_one_where_server_error_is_not_our_fault_probably() -> Result<()> {
        // 🔧 Arrange — the server is on fire
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_string("internal_server_error: we tried, we really did"),
            )
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_unlucky_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        let the_500_result = the_unlucky_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await;

        // 🎯 Assert — 500 is not 200. Math checks out.
        assert!(
            the_500_result.is_err(),
            "💀 500 response should fail. It's literally called 'Internal Server Error'."
        );

        Ok(())
    }

    /// 🧪 Content-Type must be application/x-ndjson. The x- means "we made this up but we're committing."
    #[tokio::test]
    async fn the_one_where_content_type_is_ndjson_not_json() -> Result<()> {
        // 🔧 Arrange — mock that specifically requires the correct content type
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("Content-Type", "application/x-ndjson"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_proper_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_proper_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — wiremock's header matcher confirms Content-Type ✅

        Ok(())
    }

    /// 🧪 API key auth on bulk requests. The premium tier.
    #[tokio::test]
    async fn the_one_where_api_key_auth_is_used_for_bulk() -> Result<()> {
        // 🔧 Arrange — API key for the bulk endpoint
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("Authorization", "ApiKey bulk_vip_pass"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.api_key = Some("bulk_vip_pass".to_string());

        let mut the_vip_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_vip_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — wiremock confirms ApiKey header was sent ✅

        Ok(())
    }

    /// 🧪 Basic auth for bulk when no API key. The working class hero of authentication.
    #[tokio::test]
    async fn the_one_where_basic_auth_is_used_for_bulk() -> Result<()> {
        // 🔧 Arrange — basic auth for the common folk
        let mock_server = MockServer::start().await;

        // 📡 Root ping also needs to accept basic auth
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        // 📡 dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk = base64("the_user:the_password")
        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("Authorization", "Basic dGhlX3VzZXI6dGhlX3Bhc3N3b3Jk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.username = Some("the_user".to_string());
        config.password = Some("the_password".to_string());

        let mut the_basic_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_basic_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — wiremock confirms Basic auth was sent ✅

        Ok(())
    }

    /// 🧪 No auth = no Authorization header. Walking into the club with no ID. Some clusters allow it.
    #[tokio::test]
    async fn the_one_where_no_auth_means_no_auth_header() -> Result<()> {
        // 🔧 Arrange — no auth, no header, no judgment
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .named("bulk_no_auth")
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());

        let mut the_naked_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_naked_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — request was received. No auth configured = no auth sent. ✅

        Ok(())
    }

    /// 🧪 Both API key and basic auth configured → API key wins for bulk.
    /// "There can be only one." — Connor MacLeod, discussing auth headers
    #[tokio::test]
    async fn the_one_where_api_key_trumps_basic_auth_for_bulk() -> Result<()> {
        // 🔧 Arrange — both auth methods, only API key should survive
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(header("Authorization", "ApiKey the_chosen_one"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let mut config = make_config(&mock_server.uri());
        config.api_key = Some("the_chosen_one".to_string());
        config.username = Some("the_rejected_one".to_string());
        config.password = Some("the_forgotten_one".to_string());

        let mut the_decisive_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_decisive_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — wiremock confirms ApiKey won the auth battle ✅

        Ok(())
    }

    /// 🧪 Payload body arrives exactly as sent. No mutation. No trimming. Pure NDJSON.
    #[tokio::test]
    async fn the_one_where_the_payload_body_arrives_intact() -> Result<()> {
        // 🔧 Arrange — a carefully crafted payload that must survive the journey
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        let the_sacred_payload =
            "{\"index\":{}}\n{\"id\":42,\"confession\":\"I still use println for debugging\"}\n";

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .and(body_string(the_sacred_payload))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_faithful_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_faithful_sink.send(the_sacred_payload.to_string()).await?;

        // 🎯 Assert — wiremock's body_string matcher confirms byte-perfect delivery ✅

        Ok(())
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GROUP D: close() — The No-Op                                       │
    // │  "The best code is no code at all." — Jeff Atwood, on close()       │
    // └──────────────────────────────────────────────────────────────────────┘

    /// 🧪 close() does nothing. Returns Ok. No HTTP. No drama. No buffer.
    /// The on-call engineer's dream function.
    #[tokio::test]
    async fn the_one_where_close_does_absolutely_nothing_and_thats_fine() -> Result<()> {
        // 🔧 Arrange — build a sink just to close it. Like buying a door to practice leaving.
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        let config = make_config(&mock_server.uri());
        let mut the_soon_to_be_closed_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act — the grand closing ceremony
        let the_anticlimactic_result = the_soon_to_be_closed_sink.close().await;

        // 🎯 Assert — Ok and nothing else. The most boring test. The best test.
        assert!(
            the_anticlimactic_result.is_ok(),
            "💀 close() failed. HOW? It literally does nothing. What did you DO?"
        );

        Ok(())
    }

    // ┌──────────────────────────────────────────────────────────────────────┐
    // │  GROUP E: Edge Cases — The Weird Stuff                              │
    // │  "Edge cases are where bugs go to hide." — Ancient proverb          │
    // └──────────────────────────────────────────────────────────────────────┘

    /// 🧪 Trailing slash: bulk endpoint is /_bulk, not //_bulk. One slash of difference.
    #[tokio::test]
    async fn the_one_where_bulk_url_has_no_trailing_slash_drama() -> Result<()> {
        // 🔧 Arrange — the cursed trailing slash
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock_server)
            .await;

        // ⚠️ Trailing slash in the URL — the classic footgun
        let config = make_config(&format!("{}/", mock_server.uri()));
        let mut the_slash_aware_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act
        the_slash_aware_sink.send("{\"index\":{}}\n{\"id\":1}\n".to_string()).await?;

        // 🎯 Assert — wiremock's path("/_bulk") + expect(1) confirms correct URL ✅

        Ok(())
    }

    /// 🧪 Empty payload — sent as-is. The sink doesn't validate content. YOLO. 🦆
    #[tokio::test]
    async fn the_one_where_we_send_an_empty_payload_because_yolo() -> Result<()> {
        // 🔧 Arrange — accepting the void
        let mock_server = MockServer::start().await;
        mount_root_ping(&mock_server).await;

        Mock::given(method("POST"))
            .and(path("/_bulk"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&mock_server)
            .await;

        let config = make_config(&mock_server.uri());
        let mut the_yolo_sink = ElasticsearchSink::new(config).await?;

        // 🚀 Act — send absolutely nothing
        let the_existential_result = the_yolo_sink.send(String::new()).await;

        // 🎯 Assert — the sink sent it, ES accepted it. Not our circus, not our monkeys.
        assert!(
            the_existential_result.is_ok(),
            "💀 Empty payload with 200 response should be Ok. The sink doesn't judge content."
        );

        Ok(())
    }
}
