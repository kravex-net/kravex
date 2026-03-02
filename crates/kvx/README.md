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
  - `app_config` — `AppConfig`, `RuntimeConfig`, `SourceConfig`, `SinkConfig` (Figment-based config loading; owns all top-level config enums)
  - `backends` — backend wiring + re-exports; includes `CommonSinkConfig`, `CommonSourceConfig` (backend-shared config primitives)
  - `backends/common_config` — `CommonSinkConfig`, `CommonSourceConfig` (live here to avoid circular dep with `app_config`)
  - `backends/{source,sink}` — `Source`/`Sink` traits + `SourceBackend`/`SinkBackend` enums
  - `backends/elasticsearch/{elasticsearch_source,elasticsearch_sink}` — ES backend impls
  - `backends/file/{file_source,file_sink}` — file backend impls
  - `backends/in_mem/{in_mem_source,in_mem_sink}` — in-memory test backend
  - `backends/s3_rally/s3_rally_source` — S3 Rally benchmark source (streams track data from S3 via AWS SDK)
  - `composers` — `Composer` trait + `NdjsonComposer`/`JsonArrayComposer` + `ComposerBackend` dispatcher
  - `collectors` — `PayloadCollector` trait + `NdjsonCollector`/`JsonArrayCollector` + `CollectorBackend` dispatcher
  - `transforms` — `Transform` trait + `DocumentTransformer` enum (Cow-based)
  - `supervisors` — pipeline orchestration (Supervisor + workers); no config submodule — config lives in `app_config`
  - `common` — `Hit`/`HitBatch` (legacy dead code)
  - `progress` — TUI metrics

## Pipeline Architecture (current — Raw Pages + Composer)
```
Source.next_page() → Option<String> (raw page)
  → channel(String)
  → SinkWorker buffers Vec<String> (by byte size threshold)
  → Composer.compose(&buffer, &transformer) → final payload String
  → Sink.send(payload)
```

## Module Dependency Graph
```
lib.rs ──► app_config (RuntimeConfig, SourceConfig, SinkConfig)
  │              │
  │              ▼
  │         backends ──► backends/common_config (CommonSinkConfig, CommonSourceConfig)
  │              │              ↑ (imported by backend-specific configs to embed)
  │              ▼
  │         supervisors ──► workers (SourceWorker, SinkWorker)
  │              │                │
  ├──► transforms ◄───────────────┘ (called by Composer)
  └──► composers  ◄── SinkWorker (holds ComposerBackend + DocumentTransformer)

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

## Architecture Pattern (used by backends, transforms, composers)
```
┌──────────────────┐   ┌──────────────────────┐   ┌─────────────────────┐
│ trait Source      │   │ trait Transform       │   │ trait Composer      │
│   fn next_page() │   │   fn transform(&str)  │   │   fn compose(pages) │
│   → Option<Str>  │   │   → Vec<Cow<str>>     │   │   → String          │
└────────┬─────────┘   └────────┬─────────────┘   └────────┬────────────┘
         │                      │                           │
┌────────┴─────────┐   ┌────────┴─────────────┐   ┌────────┴────────────┐
│ FileSource       │   │ RallyS3ToEs          │   │ NdjsonComposer      │
│ InMemorySource   │   │ Passthrough          │   │ JsonArrayComposer   │
│ ElasticsearchSrc │   │                      │   │                     │
│ S3RallySource    │   │                      │   │                     │
└────────┬─────────┘   └────────┬─────────────┘   └────────┬────────────┘
         │                      │                           │
┌────────┴─────────┐   ┌────────┴─────────────┐   ┌────────┴────────────┐
│ enum SourceBknd  │   │ enum DocTransformer   │   │ enum ComposerBknd   │
│   match dispatch │   │   match dispatch      │   │   match dispatch    │
└──────────────────┘   └──────────────────────┘   └─────────────────────┘
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

## Responsibility Boundaries

| Component | Responsibility |
|---|---|
| Source | Read raw page, return `Option<String>`. Format-ignorant. |
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
- v12 S3 Rally source (current): `S3RallySource` streams Rally benchmark track data from S3. `RallyTrack` enum validates track names. Config: track, bucket, region, optional key override, CommonSourceConfig. Transport: `GetObject` → `ByteStream::into_async_read()` → `BufReader` → `read_line()` (same loop as FileSource). Transform routing: S3Rally→File = Passthrough, S3Rally→ES = RallyS3ToEs. 44 tests passing.

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
