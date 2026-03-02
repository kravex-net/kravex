// ai
//! 🪣📡🏗️ S3 Rally Source — streaming benchmark data from the cloud, one page at a time.
//!
//! COLD OPEN — EXT. DATA CENTER — 3:47 AM
//!
//! The on-call engineer stared at the terminal. "We need the geonames dataset,"
//! they whispered. "All 11 million documents. From S3. Into a file. By morning."
//! The cursor blinked. The S3RallySource blinked back. "I got you, fam," it said,
//! and began streaming at wire speed.
//!
//! This module implements `Source` for AWS S3, specifically tailored for
//! OpenSearch Benchmark (Rally) track data. Given a track name and an S3 bucket,
//! it streams the object line-by-line using `BufReader` over the SDK's async reader.
//!
//! 🧠 Knowledge graph:
//! - `S3RallySourceConfig`: track name, bucket, region, optional key override, CommonSourceConfig
//! - `RallyTrack`: enum of all known benchmark tracks (validated at deserialization)
//! - `S3RallySource`: wraps `BufReader<Box<dyn AsyncRead + Send + Unpin>>` — same read pattern as FileSource
//! - Transport: `GetObject` → `ByteStream::into_async_read()` → `BufReader` → `read_line()`
//! - The `into_async_read()` gives us `tokio::io::AsyncRead`, so we get the exact same
//!   BufReader line-reading loop as FileSource. Same contract. Different origin. Same vibes.

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{self, AsyncBufReadExt, AsyncRead};
use tracing::trace;

use crate::backends::common_config::CommonSourceConfig;
use crate::backends::Source;
use crate::progress::ProgressMetrics;

// ============================================================
//  🏷️ RallyTrack — the enum of known benchmark tracks
//  "He who passes an invalid track name, panics at deserialization." — Ancient proverb 📜
// ============================================================

/// 🏎️ Known OpenSearch Benchmark / Elasticsearch Rally tracks.
///
/// Each variant represents a publicly available benchmark dataset.
/// Validated at config deserialization time — no invalid track names
/// survive past the TOML parser. Like a bouncer, but for data.
///
/// 🧠 Knowledge graph: track names map to S3 key prefixes via `default_key()`.
/// Convention: `{track_name}/documents.json` unless overridden in config.
///
/// 📜 Ancient proverb: "He who hardcodes the track name in a match arm,
/// adds a new variant and forgets the match arm."
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RallyTrack {
    /// 📊 Big5 — five aggregation-heavy operations, the CrossFit of benchmarks
    Big5,
    /// 🖱️ ClickBench — click analytics, because someone is always clicking somewhere
    Clickbench,
    /// 📅 Eventdata — time-series event data, like a diary but for servers
    Eventdata,
    /// 🌍 Geonames — 11M+ geographic names, the atlas of the internet
    Geonames,
    /// 📍 Geopoint — lat/lon points, for when you need to know WHERE the data is
    Geopoint,
    /// 📐 Geopointshape — geopoints with shapes, geometry class but make it distributed
    Geopointshape,
    /// 🗺️ Geoshape — complex geographic shapes, polygons have feelings too
    Geoshape,
    /// 📡 HttpLogs — web server access logs, the autobiography of nginx
    HttpLogs,
    /// 🪆 Nested — nested documents, Russian dolls of JSON
    Nested,
    /// 🧠 NeuralSearch — vector search with neural models, the AI searching for AI
    NeuralSearch,
    /// 🌊 Noaa — weather data from NOAA, because even benchmarks check the forecast
    Noaa,
    /// 🌊🧠 NoaaSemanticSearch — NOAA + semantic search, weather meets word embeddings
    NoaaSemanticSearch,
    /// 🚕 NycTaxis — NYC taxi ride data, the Uber of benchmark datasets
    NycTaxis,
    /// 🔍 Percolator — reverse search, where the query is the document and the document is the query
    Percolator,
    /// 📚 Pmc — PubMed Central papers, 574K academic papers walk into a benchmark
    Pmc,
    /// 💬 So — Stack Overflow data, questions about questions about questions
    So,
    /// 🦠 TreccovidSemanticSearch — COVID-19 research + semantic search, a 2020 mood
    TreccovidSemanticSearch,
    /// 🧭 Vectorsearch — vector similarity search, nearest neighbors in high-dimensional space
    Vectorsearch,
}

impl RallyTrack {
    /// 🏷️ Returns the snake_case string representation of the track name.
    ///
    /// Used to build the default S3 key: `{track_name}/documents.json`.
    /// If you add a variant, add a match arm. If you forget, rustc will yell at you.
    /// That's the beauty of exhaustive matching — the compiler is the QA team. 🧪
    pub fn as_str(&self) -> &str {
        // 🧠 These match the serde `rename_all = "snake_case"` names exactly.
        // TOML says `track = "geonames"`, serde deserializes to `RallyTrack::Geonames`,
        // and this method converts back to `"geonames"` for the S3 key lookup.
        // -- 🎬 "You shall not pass... an invalid variant through this match." — Gandalf, probably
        match self {
            Self::Big5 => "big5",
            Self::Clickbench => "clickbench",
            Self::Eventdata => "eventdata",
            Self::Geonames => "geonames",
            Self::Geopoint => "geopoint",
            Self::Geopointshape => "geopointshape",
            Self::Geoshape => "geoshape",
            Self::HttpLogs => "http_logs",
            Self::Nested => "nested",
            Self::NeuralSearch => "neural_search",
            Self::Noaa => "noaa",
            Self::NoaaSemanticSearch => "noaa_semantic_search",
            Self::NycTaxis => "nyc_taxis",
            Self::Percolator => "percolator",
            Self::Pmc => "pmc",
            Self::So => "so",
            Self::TreccovidSemanticSearch => "treccovid_semantic_search",
            Self::Vectorsearch => "vectorsearch",
        }
    }

    /// 🗝️ Returns the default S3 object key for this track.
    ///
    /// Convention: `{track_name}/documents.json` — because naming things
    /// is one of the two hard problems in computer science (the other being
    /// cache invalidation and off-by-one errors).
    pub fn default_key(&self) -> String {
        // -- 🧙 "One does not simply walk into S3 without a key." — Boromir, cloud architect
        format!("{}/documents.json", self.as_str())
    }
}

impl std::fmt::Display for RallyTrack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // -- 🎭 "To display, or not to display, that is the question." — Hamlet, the debugger
        write!(f, "{}", self.as_str())
    }
}

// ============================================================
//  🔧 S3RallySourceConfig — knobs, dials, and one track name
// ============================================================

/// 🔧 Configuration for the S3 Rally source backend.
///
/// KNOWLEDGE GRAPH: config lives co-located with the backend that uses it.
/// Same ethos as `FileSourceConfig` in `file_source.rs` — no scavenger hunts
/// at 2am wondering "where is that config struct defined?" It's RIGHT HERE.
///
/// 📐 Design note: `track` is required (validated enum). `bucket` and `region`
/// are required strings. `key` is optional — defaults to `{track}/documents.json`.
/// `common_config` defaults via serde if omitted.
///
/// 🦆 The duck approves of this config. It's clean. It's typed. It won't crash
/// your 3am deploy. (The duck makes no guarantees about your 4am deploy.)
#[derive(Debug, Deserialize, Clone)]
pub struct S3RallySourceConfig {
    /// 🏎️ Which Rally benchmark track to download — validated enum, no freestyling
    pub track: RallyTrack,
    /// 🪣 The S3 bucket name — where the data lives, sleeps, and waits for us
    pub bucket: String,
    /// 🌎 AWS region — defaults to "us-east-1" because that's where data goes to retire
    #[serde(default = "default_s3_region")]
    pub region: String,
    /// 🗝️ Optional S3 key override — if None, defaults to `{track}/documents.json`
    #[serde(default)]
    pub key: Option<String>,
    /// 📦 Batch configuration — shared with all source backends
    #[serde(default = "default_s3_rally_common_source_config")]
    pub common_config: CommonSourceConfig,
}

impl S3RallySourceConfig {
    /// 🗝️ Resolves the S3 object key — user override wins, else default convention.
    ///
    /// "Timeout exceeded: We waited for the key. And waited. Like a dog at the window.
    /// But the key was in the config all along."
    pub fn resolved_key(&self) -> String {
        self.key
            .clone()
            .unwrap_or_else(|| self.track.default_key())
    }
}

/// 🌎 Default region — us-east-1. The Florida of AWS regions. Everyone ends up there eventually.
/// Like the Hotel California, you can check out but your data never leaves.
fn default_s3_region() -> String {
    // -- 🏖️ If you don't choose a region, the region chooses you. And it chose Florida.
    "us-east-1".to_string()
}

/// 🔧 Default batch config for S3 sources — same defaults as CommonSourceConfig::default().
///
/// This exists purely so serde can call it when `common_config` is absent from TOML.
/// The `#[serde(default = "...")]` attribute demands a named function. We oblige.
fn default_s3_rally_common_source_config() -> CommonSourceConfig {
    // -- ✅ "It just works" — said the developer who had not yet tested it in production. 🤞
    CommonSourceConfig::default()
}

// ============================================================
//  🪣 S3RallySource — the streaming reader
//  "In a world where bytes flow from the cloud...
//   one BufReader dared to read_line()."
// ============================================================

// 🧠 Type alias for the boxed async reader — we erase the concrete type from
// `ByteStream::into_async_read()` because storing `impl AsyncRead` in a struct
// field requires type erasure. The Box adds one pointer indirection, which is
// literally nothing compared to the network latency of reading from S3.
// The borrow checker doesn't care. The allocator barely notices. We move on.
type S3AsyncReader = Box<dyn AsyncRead + Send + Unpin>;

/// 🪣 S3RallySource — streams Rally benchmark data from S3, page by page.
///
/// Mirrors [`FileSource`](super::file::FileSource) exactly:
/// - Wraps a `BufReader` (but over an S3 byte stream instead of a file handle)
/// - Returns raw pages of newline-delimited JSON via `next_page()`
/// - Respects `max_batch_size_docs` and `max_batch_size_bytes`
/// - Tracks progress via `ProgressMetrics`
///
/// 🧠 Knowledge graph:
/// - Transport: AWS SDK `GetObject` → `ByteStream::into_async_read()` → `BufReader`
/// - `read_line()` loop identical to `FileSource::next_page()` — same contract, different pipe
/// - Progress bar shows `s3://{bucket}/{key}` as source label
/// - Content length from HEAD request (may be 0 if unknown — progress bar adapts)
///
/// 🐛 Known edge case: if someone is overwriting the S3 object while we read,
/// the content length from HEAD will be wrong. But honestly, if you're doing that,
/// you have bigger problems than an inaccurate progress bar.
pub(crate) struct S3RallySource {
    // 🔗 BufReader wrapping the S3 byte stream — line-by-line reading, async style.
    // The inner reader is a boxed trait object because `into_async_read()` returns
    // an opaque impl type. We pay one vtable indirection per read. The network RTT
    // to S3 laughs at this cost.
    buf_reader: io::BufReader<S3AsyncReader>,
    source_config: S3RallySourceConfig,
    // 📊 Progress tracker — feeds the TUI progress table.
    // Without it, you're downloading blind. With it, you're downloading blind
    // but at least there's a pretty bar to watch.
    progress: ProgressMetrics,
}

// 🐛 Debug impl excludes `buf_reader` — AsyncRead trait objects don't impl Debug,
// and nobody debugging an S3 source wants to see the internal BufReader state anyway.
// Same pattern as FileSource. Consistency is a feature. So is not panicking in Debug.
impl std::fmt::Debug for S3RallySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3RallySource")
            .field("source_config", &self.source_config)
            .finish()
    }
}

impl S3RallySource {
    /// 🚀 Creates a new S3RallySource — connects to AWS, validates the object exists,
    /// and opens the byte stream for async reading.
    ///
    /// This is where the magic happens:
    /// 1. Build AWS config from environment (env vars, IAM role, ~/.aws/config)
    /// 2. HEAD request → get content length for progress bar
    /// 3. GetObject → open the byte stream
    /// 4. `into_async_read()` → `BufReader` → ready to `read_line()`
    ///
    /// 💀 Fails if: credentials are missing, bucket doesn't exist, key doesn't exist,
    /// AWS is down (it happens), or the ghost in the machine is feeling uncooperative.
    ///
    /// "Config not found: We looked everywhere. Under the couch. Behind the fridge.
    /// In the junk drawer. Nothing. But the S3 bucket? That we found."
    pub(crate) async fn new(source_config: S3RallySourceConfig) -> Result<Self> {
        let the_resolved_key = source_config.resolved_key();

        // 🔧 Build AWS config from the environment — credentials, region, the works.
        // aws-config's `from_env()` checks: env vars → ~/.aws/config → IAM role → hope.
        let the_aws_config = aws_config::from_env()
            .region(aws_sdk_s3::config::Region::new(
                source_config.region.clone(),
            ))
            .load()
            .await;

        let the_s3_client = aws_sdk_s3::Client::new(&the_aws_config);

        // 📏 HEAD request — grab content length for the progress bar.
        // If this fails, the object probably doesn't exist. Or we don't have permission.
        // Either way, we fail loud here at startup, not silent in the hot path.
        let the_head_response = the_s3_client
            .head_object()
            .bucket(&source_config.bucket)
            .key(&the_resolved_key)
            .send()
            .await
            .context(format!(
                "💀 HEAD request failed for s3://{}/{}. The bucket ghosted us. \
                 Like my college roommate. Kevin, if you're reading this, I want my blender back. \
                 Check: bucket name, key path, region, and credentials.",
                source_config.bucket, the_resolved_key
            ))?;

        // 📏 Content length might be None (multipart uploads, etc.) — default to 0.
        // The progress bar will show "unknown" which is honest if not reassuring.
        let the_content_length = the_head_response.content_length().unwrap_or(0);
        let the_content_length_u64 = if the_content_length > 0 {
            the_content_length as u64
        } else {
            0u64
        };

        // 🚀 GetObject — open the byte stream. This is the main event.
        // The response body is a ByteStream that we'll convert to AsyncRead.
        let the_get_response = the_s3_client
            .get_object()
            .bucket(&source_config.bucket)
            .key(&the_resolved_key)
            .send()
            .await
            .context(format!(
                "💀 GetObject failed for s3://{}/{}. The data is there (HEAD said so), \
                 but S3 won't let us read it. This is the digital equivalent of \
                 being told 'we have food at home' by the cloud. \
                 Check: IAM permissions, bucket policy, KMS key access.",
                source_config.bucket, the_resolved_key
            ))?;

        // 🔗 Convert ByteStream → AsyncRead → BufReader
        // `into_async_read()` gives us `impl AsyncRead + Send + Unpin`.
        // We box it for type erasure (struct fields can't hold `impl Trait`).
        // The vtable indirection is ~0.3ns per call. The S3 round trip is ~50ms.
        // If you're worried about the Box overhead, I have a bridge to sell you.
        let the_async_reader = the_get_response.body.into_async_read();
        let the_boxed_reader: S3AsyncReader = Box::new(the_async_reader);
        let the_buf_reader = io::BufReader::new(the_boxed_reader);

        // 📊 Progress metrics — source label is the S3 URI for display in the TUI.
        let the_source_label = format!(
            "s3://{}/{}",
            source_config.bucket, the_resolved_key
        );
        let the_progress = ProgressMetrics::new(the_source_label, the_content_length_u64);

        Ok(Self {
            buf_reader: the_buf_reader,
            source_config,
            progress: the_progress,
        })
    }
}

#[async_trait]
impl Source for S3RallySource {
    /// 📄 Read the next page of lines from the S3 byte stream. Returns `None` when exhausted.
    ///
    /// 🧠 Knowledge graph: identical loop to `FileSource::next_page()` — same exit conditions:
    ///   1. `max_batch_size_docs`: line count cap. Don't build a page the size of Ohio.
    ///   2. `max_batch_size_bytes`: byte cap. Memory is finite, even on EC2 instances that cost
    ///      more per hour than a lawyer.
    ///   3. Stream exhaustion: `read_line()` returns 0 bytes = EOF.
    ///
    /// The raw page is newline-delimited JSON — same as FileSource. The downstream
    /// Composer handles transform + assembly. We just pump bytes.
    ///
    /// 📜 "He who reads the entire S3 object into one String, pays the OOM tax in production."
    async fn next_page(&mut self) -> Result<Option<String>> {
        let mut page = String::with_capacity(
            self.source_config.common_config.max_batch_size_bytes,
        );
        let mut total_bytes_read = 0usize;
        let mut line_count = 0usize;
        // ⚠️ 1MB initial capacity per line — because Rally JSON documents can be CHONKY.
        // -- Like, "is that a document or a novella?" levels of chonky.
        let mut line = String::with_capacity(1024 * 1024);

        for _ in 0..=self.source_config.common_config.max_batch_size_docs {
            let bytes_read = self.buf_reader.read_line(&mut line).await?;
            if bytes_read == 0 {
                break;
            }

            total_bytes_read += bytes_read;
            // 🧹 Strip trailing newlines — same cleanup as FileSource.
            // S3 data is just as newline-happy as local files. Bytes don't care about origin stories.
            let trimmed = line.trim_end_matches('\n').trim_end_matches('\r');
            if !trimmed.is_empty() {
                // 🔗 Separate lines with \n — no trailing newline. Composer handles the rest.
                if !page.is_empty() {
                    page.push('\n');
                }
                page.push_str(trimmed);
                line_count += 1;
            }
            line.clear();

            if total_bytes_read > self.source_config.common_config.max_batch_size_bytes {
                break;
            }

            if line_count >= self.source_config.common_config.max_batch_size_docs {
                break;
            }
        }

        trace!(
            "🪣 hauled {} bytes from S3 like a digital bucket brigade — {}🧵 lines in this page",
            total_bytes_read,
            line_count
        );
        self.progress
            .update(total_bytes_read as u64, line_count as u64);

        // 📄 Empty page = stream exhausted. The bucket is empty. The well is dry. Return None. 🏁
        if page.is_empty() {
            Ok(None)
        } else {
            Ok(Some(page))
        }
    }
}

// ============================================================
//  🧪 Tests — "trust but verify" is for diplomats.
//  Engineers say "trust nothing, test everything, blame DNS."
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// 🧪 Verify all track names deserialize correctly from their snake_case TOML form.
    /// If this test fails, someone added a variant and forgot the serde rename. Shame. 🔔
    #[test]
    fn the_one_where_every_rally_track_deserializes_from_snake_case() {
        // 🏎️ Every track name as it would appear in TOML config
        let the_track_names_and_their_destiny = vec![
            ("\"big5\"", RallyTrack::Big5),
            ("\"clickbench\"", RallyTrack::Clickbench),
            ("\"eventdata\"", RallyTrack::Eventdata),
            ("\"geonames\"", RallyTrack::Geonames),
            ("\"geopoint\"", RallyTrack::Geopoint),
            ("\"geopointshape\"", RallyTrack::Geopointshape),
            ("\"geoshape\"", RallyTrack::Geoshape),
            ("\"http_logs\"", RallyTrack::HttpLogs),
            ("\"nested\"", RallyTrack::Nested),
            ("\"neural_search\"", RallyTrack::NeuralSearch),
            ("\"noaa\"", RallyTrack::Noaa),
            ("\"noaa_semantic_search\"", RallyTrack::NoaaSemanticSearch),
            ("\"nyc_taxis\"", RallyTrack::NycTaxis),
            ("\"percolator\"", RallyTrack::Percolator),
            ("\"pmc\"", RallyTrack::Pmc),
            ("\"so\"", RallyTrack::So),
            (
                "\"treccovid_semantic_search\"",
                RallyTrack::TreccovidSemanticSearch,
            ),
            ("\"vectorsearch\"", RallyTrack::Vectorsearch),
        ];

        for (the_json_str, the_expected_variant) in the_track_names_and_their_destiny {
            let the_deserialized: RallyTrack = serde_json::from_str(the_json_str).unwrap_or_else(
                |e| {
                    panic!(
                        "💀 Failed to deserialize track '{}': {}. \
                     The serde gods are displeased. Check your rename_all.",
                        the_json_str, e
                    )
                },
            );
            assert_eq!(
                the_deserialized, the_expected_variant,
                "Track '{}' should deserialize to {:?}",
                the_json_str, the_expected_variant
            );
        }
    }

    /// 🧪 Verify `as_str()` round-trips with serde deserialization.
    /// If `as_str()` returns something different from what serde expects, we have a problem.
    /// And by "problem" I mean "a 3am incident where the S3 key is wrong."
    #[test]
    fn the_one_where_as_str_matches_serde_names() {
        let the_tracks_under_oath = vec![
            RallyTrack::Big5,
            RallyTrack::Clickbench,
            RallyTrack::Geonames,
            RallyTrack::HttpLogs,
            RallyTrack::NeuralSearch,
            RallyTrack::NycTaxis,
            RallyTrack::Pmc,
            RallyTrack::So,
        ];

        for track in the_tracks_under_oath {
            let the_str = track.as_str();
            // 🔄 Round-trip: as_str → serde deserialize → should give same variant
            let the_json = format!("\"{}\"", the_str);
            let the_roundtripped: RallyTrack = serde_json::from_str(&the_json).unwrap_or_else(
                |e| {
                    panic!(
                        "💀 as_str() returned '{}' for {:?} but serde can't parse it back: {}. \
                     The round-trip is broken. Fix as_str() or the serde rename.",
                        the_str, track, e
                    )
                },
            );
            assert_eq!(
                the_roundtripped, track,
                "Round-trip failed for {:?} — as_str() returned '{}'",
                track, the_str
            );
        }
    }

    /// 🧪 Verify `default_key()` builds the expected S3 key from the track name.
    #[test]
    fn the_one_where_default_keys_follow_the_convention() {
        assert_eq!(
            RallyTrack::Geonames.default_key(),
            "geonames/documents.json"
        );
        assert_eq!(
            RallyTrack::HttpLogs.default_key(),
            "http_logs/documents.json"
        );
        assert_eq!(RallyTrack::Pmc.default_key(), "pmc/documents.json");
        assert_eq!(
            RallyTrack::NycTaxis.default_key(),
            "nyc_taxis/documents.json"
        );
    }

    /// 🧪 Verify `resolved_key()` respects the user override when present.
    #[test]
    fn the_one_where_config_key_override_wins_over_convention() {
        let the_config_with_override = S3RallySourceConfig {
            track: RallyTrack::Geonames,
            bucket: "my-bucket".to_string(),
            region: "us-west-2".to_string(),
            key: Some("custom/path/to/data.json".to_string()),
            common_config: CommonSourceConfig::default(),
        };

        assert_eq!(
            the_config_with_override.resolved_key(),
            "custom/path/to/data.json"
        );
    }

    /// 🧪 Verify `resolved_key()` falls back to default when no override is given.
    #[test]
    fn the_one_where_resolved_key_defaults_to_track_convention() {
        let the_config_without_override = S3RallySourceConfig {
            track: RallyTrack::Pmc,
            bucket: "rally-data".to_string(),
            region: "eu-west-1".to_string(),
            key: None,
            common_config: CommonSourceConfig::default(),
        };

        assert_eq!(
            the_config_without_override.resolved_key(),
            "pmc/documents.json"
        );
    }

    /// 🧪 Verify the full S3RallySourceConfig deserializes from JSON (simulating TOML structure).
    #[test]
    fn the_one_where_config_deserializes_like_a_well_behaved_struct() {
        let the_config_json = r#"{
            "track": "geonames",
            "bucket": "my-rally-bucket",
            "region": "us-west-2"
        }"#;

        let the_config: S3RallySourceConfig = serde_json::from_str(the_config_json)
            .expect("💀 Config deserialization failed. The JSON was valid. The struct was not amused.");

        assert_eq!(the_config.track, RallyTrack::Geonames);
        assert_eq!(the_config.bucket, "my-rally-bucket");
        assert_eq!(the_config.region, "us-west-2");
        assert!(the_config.key.is_none());
        // 📦 common_config should have defaults
        assert!(the_config.common_config.max_batch_size_docs > 0);
    }

    /// 🧪 Verify the default region is us-east-1 when not specified.
    #[test]
    fn the_one_where_region_defaults_to_the_florida_of_aws() {
        let the_config_json = r#"{
            "track": "pmc",
            "bucket": "some-bucket"
        }"#;

        let the_config: S3RallySourceConfig = serde_json::from_str(the_config_json)
            .expect("💀 Config should parse without region — it has a default. Serde disagrees.");

        assert_eq!(the_config.region, "us-east-1");
    }

    /// 🧪 Verify an unknown track name fails deserialization.
    /// This is the whole point of the enum — invalid tracks don't make it past the parser.
    #[test]
    fn the_one_where_fake_track_names_get_rejected_at_the_door() {
        let the_bogus_json = r#"{ "track": "definitely_not_a_real_track", "bucket": "b" }"#;

        let the_result = serde_json::from_str::<S3RallySourceConfig>(the_bogus_json);
        assert!(
            the_result.is_err(),
            "Fake track names should fail deserialization. The bouncer is sleeping."
        );
    }

    /// 🧪 Verify Display impl for RallyTrack matches as_str().
    #[test]
    fn the_one_where_display_and_as_str_agree_on_reality() {
        let the_track = RallyTrack::NycTaxis;
        assert_eq!(format!("{}", the_track), the_track.as_str());
        assert_eq!(format!("{}", the_track), "nyc_taxis");
    }
}
