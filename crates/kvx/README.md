# Summary

Core library for kravex — the data migration engine. Raw pages, Cow-powered zero-copy, Composer-based payload assembly, and a clean config ownership model.

# Description

`kvx` provides the foundational primitives for search migration: throttling, cutover logic, retry/recovery, and adaptive throughput. This crate is consumed by `kvx-cli` and any future integrations.

# Knowledge Graph

- **Workspace member**: `crates/kvx`
- **Dependents**: `kvx-cli`
- **Dependencies**: anyhow, async-channel, figment, reqwest, serde, serde_json, tokio, tracing, async-trait, futures, indicatif, comfy-table, aws-sdk-s3, aws-config
- **Edition**: 2024
- **Modules**:
  - `app_config` — `AppConfig`, `RuntimeConfig`, `SourceConfig`, `SinkConfig`, `ControllerConfig` (Figment-based config loading; owns all top-level config enums)
  - `backends` — backend wiring + re-exports; includes `CommonSinkConfig`, `CommonSourceConfig` (backend-shared config primitives)
  - `backends/common_config` — `CommonSinkConfig`, `CommonSourceConfig` (live here to avoid circular dep with `app_config`)
  - `backends/{source,sink}` — `Source`/`Sink` traits + `SourceBackend`/`SinkBackend` enums. Source now includes `set_page_size_hint()`.
  - `backends/elasticsearch/{elasticsearch_source,elasticsearch_sink}` — ES backend impls
  - `backends/file/{file_source,file_sink}` — file backend impls
  - `backends/in_mem/{in_mem_source,in_mem_sink}` — in-memory test backend
  - `backends/s3_rally/s3_rally_source` — S3 Rally benchmark source (streams track data from S3 via AWS SDK)
  - `controllers` — `Controller` trait + `ControllerConfig` + `ControllerBackend` enum (adaptive batch sizing)
  - `controllers/config_controller` — `ConfigController` (static — returns configured batch size, ignores measurements)
  - `controllers/pid_bytes_to_doc_count` — `PidBytesToDocCount` (PID feedback loop: measures response bytes, outputs doc count)
  - `composers` — `Composer` trait + `NdjsonComposer`/`JsonArrayComposer` + `ComposerBackend` dispatcher
  - `transforms` — `Transform` trait + `DocumentTransformer` enum (Cow-based)
  - `supervisors` — pipeline orchestration (Supervisor + workers); no config submodule — config lives in `app_config`
  - `common` — `Hit`/`HitBatch` (legacy dead code)
  - `progress` — TUI metrics

## Pipeline Architecture (current — Raw Pages + Composer + Controller)
```
Controller.output() → batch_size_hint
  → Source.set_page_size_hint(hint)
  → Source.next_page() → Option<String> (raw page)
  → Controller.measure(page.len())
  → channel(String)
  → SinkWorker buffers Vec<String> (by byte size threshold)
  → Composer.compose(&buffer, &transformer) → final payload String
  → Sink.send(payload)
```

## Module Dependency Graph
```
lib.rs ──► app_config (RuntimeConfig, SourceConfig, SinkConfig, ControllerConfig)
  │              │
  │              ▼
  │         backends ──► backends/common_config (CommonSinkConfig, CommonSourceConfig)
  │              │              ↑ (imported by backend-specific configs to embed)
  │              ▼
  │         supervisors ──► workers (SourceWorker w/ ControllerBackend, SinkWorker)
  │              │                │
  ├──► transforms ◄───────────────┘ (called by Composer)
  ├──► composers  ◄── SinkWorker (holds ComposerBackend + DocumentTransformer)
  └──► controllers ◄── SourceWorker (holds ControllerBackend, feeds output to Source)

```

# Key Concepts

- **Sources return `Option<String>`**: one raw page per call, content uninterpreted. `None` = EOF. Source is maximally ignorant — it's a faucet, not a chef.
- **Sinks are I/O-only**: accept a fully rendered payload `String`, send it (HTTP POST, file write, memory push)
- **SinkWorker buffers raw pages** by byte size, flushes via Composer when buffer approaches `max_request_size_bytes`
- **Transform** (`DocumentTransformer`): per-page format conversion. Returns `Vec<Cow<str>>` items:
  - `Cow::Borrowed` = zero-copy passthrough (no allocation!)
  - `Cow::Owned` = format conversion (Rally→ES bulk, etc.)
- **Composer** (`ComposerBackend`): transform + assemble in one shot:
  - ES/File → `NdjsonComposer`: items joined with `\n`, trailing `\n`
  - InMemory → `JsonArrayComposer`: `[item,item,item]`, zero serde
- **All abstractions follow the same pattern**: trait → concrete impls → enum dispatcher → from_config resolver
- **Zero-copy passthrough**: NDJSON→NDJSON scenarios (file-to-file) — Cow borrows from buffered pages, no per-doc allocation

## Architecture Pattern (used by backends, transforms, composers, controllers)
```
┌──────────────────┐ ┌──────────────────────┐ ┌─────────────────────┐ ┌─────────────────────┐
│ trait Source      │ │ trait Transform       │ │ trait Composer      │ │ trait Controller     │
│   fn next_page() │ │   fn transform(&str)  │ │   fn compose(pages) │ │   fn output() → usize│
│   → Option<Str>  │ │   → Vec<Cow<str>>     │ │   → String          │ │   fn measure(f64)    │
└────────┬─────────┘ └────────┬─────────────┘ └────────┬────────────┘ └────────┬────────────┘
         │                    │                         │                       │
┌────────┴─────────┐ ┌────────┴─────────────┐ ┌────────┴────────────┐ ┌────────┴────────────┐
│ FileSource       │ │ RallyS3ToEs          │ │ NdjsonComposer      │ │ ConfigController     │
│ InMemorySource   │ │ Passthrough          │ │ JsonArrayComposer   │ │ PidBytesToDocCount   │
│ ElasticsearchSrc │ │                      │ │                     │ │                      │
│ S3RallySource    │ │                      │ │                     │ │                      │
└────────┬─────────┘ └────────┬─────────────┘ └────────┬────────────┘ └────────┬────────────┘
         │                    │                         │                       │
┌────────┴─────────┐ ┌────────┴─────────────┐ ┌────────┴────────────┐ ┌────────┴────────────┐
│ enum SourceBknd  │ │ enum DocTransformer   │ │ enum ComposerBknd   │ │ enum ControllerBknd  │
│   match dispatch │ │   match dispatch      │ │   match dispatch    │ │   match dispatch     │
└──────────────────┘ └──────────────────────┘ └─────────────────────┘ └──────────────────────┘
```

## Resolution Tables

### Transform Resolution (from SourceConfig × SinkConfig)
| SourceConfig | SinkConfig | Resolves to |
|---|---|---|
| File | Elasticsearch | `RallyS3ToEs` — splits page by `\n`, transforms each doc |
| S3Rally | Elasticsearch | `RallyS3ToEs` — same as File→ES (future pipeline) |
| File | File | `Passthrough` — returns entire page as `Cow::Borrowed` |
| S3Rally | File | `Passthrough` — download data as-is to local file |
| InMemory | InMemory | `Passthrough` |
| Elasticsearch | File | `Passthrough` |
| other | other | `panic!` at resolve time |

### Composer Resolution (from SinkConfig)
| SinkConfig | Composer | Wire Format |
|---|---|---|
| Elasticsearch | `NdjsonComposer` | `item\nitem\n` |
| File | `NdjsonComposer` | `item\nitem\n` |
| InMemory | `JsonArrayComposer` | `[item,item]` |

### Controller Resolution (from `[controller]` config section)
| Config type | Controller | Behavior |
|---|---|---|
| `static` (default) | `ConfigController` | Returns configured `max_batch_size_docs`, ignores measurements |
| `pid_bytes_to_doc_count` | `PidBytesToDocCount` | PID feedback: measures response bytes → adjusts doc count output |

## Responsibility Boundaries

| Component | Responsibility |
|---|---|
| Controller | Adaptive batch sizing: `output()` → doc count hint, `measure()` ← response bytes |
| Source | Read raw page, return `Option<String>`. Accepts `set_page_size_hint()`. Format-ignorant. |
| Channel | Carry `String` (raw pages) between workers |
| SinkWorker | Buffer pages by byte size, flush via Composer |
| Composer | Transform pages (via Transformer) + assemble wire-format payload |
| Transform | Per-page → `Vec<Cow<str>>` items (Borrowed=passthrough, Owned=conversion) |
| Sink | Pure I/O: HTTP POST, file write, memory push |

# Notes for future reference

- POC/MVP stage — API surface is unstable
- `Hit`/`HitBatch` in `common.rs` are now dead code — pipeline uses raw pages throughout
- Rally S3 transform splits page by `\n`, transforms each doc individually, strips 6 top-level metadata fields; nested refs survive
- ES bulk action line includes `_id` only; `_index`/`routing` set by sink URL
- Passthrough doesn't validate or split — returns entire page as one `Cow::Borrowed` item
- `escape_json_string()` avoids serde round-trip for action line construction
- `channel_data.rs` still empty — to be removed
- ES sink no longer buffers — SinkWorker handles all buffering via byte-size threshold + epsilon
- `BUFFER_EPSILON_BYTES` = 64 KiB headroom to avoid exceeding max request size after transformation
- Backend code split: each backend type has its own `{type}_source.rs` / `{type}_sink.rs`
- Core Source/Sink traits in `backends/source.rs` and `backends/sink.rs`
- Transforms and composers are Clone+Copy (zero-sized structs) — each SinkWorker gets its own copy
- `SinkConfig::max_request_size_bytes()` helper is now on `SinkConfig` in `app_config.rs`
- **Config ownership**: `RuntimeConfig`/`SourceConfig`/`SinkConfig` → `app_config.rs`; `CommonSinkConfig`/`CommonSourceConfig` → `backends/common_config.rs` (re-exported from `backends`)
- `supervisors/config.rs` — **deleted**. No backwards-compat shim remains. All callers updated.

# Aggregated Context Memory Across Sessions for Current and Future Use

- Initial scaffold: empty `lib.rs`
- v1 transforms: IngestTransform/EgressTransform + Hit intermediate — **superseded**
- v2 transforms: direct pair functions + dead traits — **superseded**
- v3 transforms: mirrors backends pattern. `Transform` trait → concrete struct impls → `DocumentTransformer` enum dispatch → `from_configs()` resolver
- v4 pipeline refactor: Sources return `Vec<String>`, Sinks are I/O-only (`send(payload)`), SinkWorker does transform + binary collect. Hit/HitBatch phased out of pipeline.
- v5 collectors: Extracted payload assembly into `PayloadCollector` trait + `NdjsonCollector`/`JsonArrayCollector` — **superseded by v10 composers**
- v6-v9 backend file splits: separated backend implementations into dedicated files with re-export shims
- v10 raw pages + composers (current): Source returns `Option<String>` (raw page), Transform returns `Vec<Cow<str>>` (zero-copy), Composer replaces Collector (transform+assemble in one shot), SinkWorker buffers by byte size. 31 tests passing.
- v11 config migration (complete): `RuntimeConfig`/`SourceConfig`/`SinkConfig` → `app_config.rs`; `CommonSinkConfig`/`CommonSourceConfig` → `backends/common_config.rs`; `supervisors/config.rs` deleted; all callers updated. 31 tests passing.
- v12 S3 Rally source: `S3RallySource` streams Rally benchmark track data from S3. `RallyTrack` enum validates track names. Config: track, bucket, region, optional key override, CommonSourceConfig. Transport: `GetObject` → `ByteStream::into_async_read()` → `BufReader` → `read_line()` (same loop as FileSource). Transform routing: S3Rally→File = Passthrough, S3Rally→ES = RallyS3ToEs. 44 tests passing.
- v13 PID controller (current): Adaptive batch sizing via `Controller` trait. Two impls: `ConfigController` (static, default) and `PidBytesToDocCount` (PID feedback loop ported from C#). `Source` trait gains `set_page_size_hint()`. `SourceWorker` owns `ControllerBackend`, runs feedback loop: `output()` → `set_page_size_hint()` → `next_page()` → `measure()`. `ControllerConfig` in `AppConfig` (`[controller]` TOML section). PID gains auto-calculated. EMA smoothing on measurements (α=0.25) and adjustments (α=0.75). Anti-windup clamping. 66 tests passing.

## S3 Rally Source Configuration Example
```toml
[source_config.S3Rally]
track = "geonames"
bucket = "my-rally-data"
region = "us-east-1"
# key = "custom/path/documents.json"  # optional override, defaults to {track}/documents.json

[sink_config.File]
file_name = "output.json"

[runtime]
queue_capacity = 8
sink_parallelism = 1
```

### Available Rally Tracks
big5, clickbench, eventdata, geonames, geopoint, geopointshape, geoshape, http_logs, nested, neural_search, noaa, noaa_semantic_search, nyc_taxis, percolator, pmc, so, treccovid_semantic_search, vectorsearch

## Controller Configuration Example
```toml
# Static (default — preserves existing behavior, no PID):
[controller]
type = "static"

# PID bytes-to-doc-count (adaptive batch sizing):
[controller]
type = "pid_bytes_to_doc_count"
desired_response_size_bytes = 5242880.0  # 5MB target response size
initial_doc_count = 1000                  # starting guess
min_doc_count = 10                        # floor
max_doc_count = 50000                     # ceiling
```

## PID Controller Theory
Set-point = desired response bytes. Measured = actual response bytes (EMA-smoothed, α=0.25). Error = desired - measured. PID gains auto-calculated: Kp = max(max_docs, desired)/min(max_docs, desired), Ki = Kp/50, Kd = sqrt(Kp×Ki). Adjustment = (P+I+D)/avg_response_size, clamped to [-100,+100]. Output = EMA(current + adj, α=0.75), clamped to [min,max].
