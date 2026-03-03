# Summary

Core library for kravex — the zero-config search migration engine. Raw pages, Cow-powered zero-copy, Composer-based payload assembly, adaptive throttling, graceful cancellation. The borrow checker approved this message (after 47 rejections).

# Description

`kvx` provides the foundational primitives for search migration: throttling, cutover logic, retry/recovery, and adaptive throughput. Consumed by `kvx-cli` and future integrations. Now with 6 phases of refactoring and a fresh coat of existential dread.

# Knowledge Graph

- **Workspace member**: `crates/kvx`
- **Dependents**: `kvx-cli`
- **Dependencies**: anyhow, async-channel, figment, reqwest, serde, serde_json, tokio, tokio-util, tracing, async-trait, futures, indicatif, comfy-table, aws-sdk-s3, aws-config
- **Edition**: 2024
- **Test count**: 117 (kvx crate)
- **Modules**:
  - `app_config` — `AppConfig`, `RuntimeConfig`, `ControllerConfig` + module subdir (`source_config.rs`, `sink_config.rs`); `build_backend()` factory on enums; re-exports `ThrottleAppConfig`, `SourceThrottleConfig`, `SinkThrottleConfig`
  - `app_config/source_config` — `SourceConfig` enum + `build_backend()` factory
  - `app_config/sink_config` — `SinkConfig` enum + `build_backend()` factory
  - `backends` — backend wiring + re-exports; `Source`/`Sink` traits + `SourceBackend`/`SinkBackend` enums
  - `backends/{elasticsearch,opensearch,file,in_mem,s3_rally}` — per-backend dirs with `{type}_source.rs` / `{type}_sink.rs`
  - `throttlers` — ALL throttle/controller logic consolidated here: `ThrottleAppConfig` (TOML `[throttle]`), `SourceThrottleConfig` (`[throttle.source]`), `SinkThrottleConfig` (`[throttle.sink]`), `ControllerConfig`, `ThrottleControllerBackend`, `ThrottleController` trait, `StaticThrottleController`, `PidControllerBytesToMs` (LICENSE-EE/BSL), `Controller` trait, `ControllerBackend`, `ConfigController`, `PidBytesToDocCount`
  - `throttlers/config_controller` — `ConfigController` (static batch sizing)
  - `throttlers/pid_bytes_to_doc_count` — `PidBytesToDocCount` (PID batch sizing)
  - `throttlers/pid_bytes_to_ms` — `PidControllerBytesToMs` (PID sink throttle)
  - `throttlers/static_throttle` — `StaticThrottleController`
  - `workers` — flattened from `supervisors/workers/`; `SourceWorker` + `SinkWorker` (both own `CancellationToken`, use `tokio::select!`)
  - `workers/source_worker` — `SourceWorker` (owns `ControllerBackend`, calls `source.pump(hint)`)
  - `workers/sink_worker` — `SinkWorker` (buffers raw pages, calls `sink.drain(payload)`, owns `ThrottleControllerBackend`)
  - `supervisors` — `Supervisor` orchestration (spawns workers, passes `CancellationToken`)
  - `composers` — `Composer` trait + `NdjsonComposer`/`JsonArrayComposer` + `ComposerBackend`
  - `transforms` — `Transform` trait + `DocumentTransformer` enum (Cow-based); `RallyS3ToEs`, `EsHitToBulk`, `Passthrough`
  - `progress` — TUI metrics
  - *(`common` — removed: Hit/HitBatch dead code purged)*

## Pipeline Architecture (v17 — Phase 1-6 refactor complete)
```
ThrottleControllerBackend.output() → dynamic byte target
ControllerBackend.output() → doc_count_hint
  → Source.pump(doc_count_hint) → Option<String> (raw page)
  → ControllerBackend.measure(page.len())
  → channel(String)
  → SinkWorker buffers Vec<String> (by byte target)
  → Composer.compose(&buffer, &transformer) → final payload String
  → Sink.drain(payload) → measure duration → ThrottleControllerBackend.measure(ms)
  → ThrottleControllerBackend.output() → next cycle's byte target
  ↑ tokio::select! on CancellationToken at every loop iteration (workers)
```

## Module Dependency Graph
```
lib.rs ──► app_config (AppConfig, RuntimeConfig, ControllerConfig)
  │              │
  │         app_config/source_config (SourceConfig, build_backend())
  │         app_config/sink_config   (SinkConfig,   build_backend())
  │              │
  │              ▼
  │         backends ──► {elasticsearch,opensearch,file,in_mem,s3_rally}
  │              │
  ├──► throttlers ◄── app_config (ThrottleAppConfig re-exported)
  │         ◄── workers/source_worker (ControllerBackend)
  │         ◄── workers/sink_worker   (ThrottleControllerBackend)
  ├──► workers ──► supervisors (spawned with CancellationToken)
  ├──► transforms ◄── Composer (called during compose)
  └──► composers  ◄── SinkWorker (holds ComposerBackend + DocumentTransformer)
```

# Key Concepts

- **`source.pump(doc_count_hint)`**: replaces `set_page_size_hint()` + `next_page()`. One call, hint included. Returns `Option<String>` (None = EOF).
- **`sink.drain(payload)`**: replaces `sink.send()`. Semantically: drain the buffer into the sink.
- **`ThrottleAppConfig`**: top-level `[throttle]` TOML section. Contains `source: SourceThrottleConfig` and `sink: SinkThrottleConfig`. No longer embedded in backend configs.
- **`SourceThrottleConfig`** (`[throttle.source]`): `max_batch_size_bytes`, `max_batch_size_docs`, `ControllerConfig`.
- **`SinkThrottleConfig`** (`[throttle.sink]`): `max_request_size_bytes`, `ThrottleConfig` (Static or Pid).
- **`build_backend()`**: factory method on `SourceConfig`/`SinkConfig` enums. Replaces ad-hoc resolver functions. Config owns its own instantiation.
- **`ThrottleControllerBackend::from_config()`**: factory from `SinkThrottleConfig`. Added `Clone`.
- **`CancellationToken`** (tokio-util): passed to every worker. Workers use `tokio::select!` to exit cleanly on `kvx::stop()`.
- **`throttlers` module**: consolidated from `controllers/`. Owns ALL throttle + controller logic.
- **`workers` module**: flattened from `supervisors/workers/`. `SourceWorker` + `SinkWorker` are top-level.
- **`AppConfig` fields**: `source`, `sink`, `runtime`, `throttle` (renamed from `source_config`/`sink_config`).
- **Zero-copy passthrough**: NDJSON→NDJSON via `Cow::Borrowed`. No per-doc alloc.
- **All abstractions**: trait → concrete impls → enum dispatcher → `from_config()` factory.
- **Transforms/composers are Clone+Copy** (zero-sized structs). Each SinkWorker gets its own copy.

## Architecture Pattern
```
┌─────────────────┐  ┌────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐
│ trait Source     │  │ trait Transform     │  │ trait Composer       │  │ trait Controller      │
│   pump(hint)     │  │   transform(&str)   │  │   compose(pages)     │  │   output() → usize   │
│   → Option<Str>  │  │   → Vec<Cow<str>>   │  │   → String           │  │   measure(f64)       │
└────────┬────────┘  └────────┬────────────┘  └────────┬────────────┘  └────────┬─────────────┘
         │                    │                         │                        │
┌────────┴────────┐  ┌────────┴────────────┐  ┌────────┴────────────┐  ┌────────┴─────────────┐
│ FileSource       │  │ RallyS3ToEs         │  │ NdjsonComposer       │  │ ConfigController      │
│ InMemorySource   │  │ EsHitToBulk         │  │ JsonArrayComposer    │  │ PidBytesToDocCount    │
│ ElasticsearchSrc │  │ Passthrough         │  │                      │  │                       │
│ OpenSearchSource │  │                     │  │                      │  │                       │
│ S3RallySource    │  │                     │  │                      │  │                       │
└────────┬────────┘  └────────┬────────────┘  └────────┬────────────┘  └────────┬─────────────┘
         │                    │                         │                        │
┌────────┴────────┐  ┌────────┴────────────┐  ┌────────┴────────────┐  ┌────────┴─────────────┐
│ SourceBackend    │  │ DocumentTransformer  │  │ ComposerBackend      │  │ ControllerBackend     │
│  (enum dispatch) │  │  (enum dispatch)     │  │  (enum dispatch)     │  │  (enum dispatch)      │
└─────────────────┘  └────────────────────┘  └─────────────────────┘  └──────────────────────┘
```

## Resolution Tables

### Transform Resolution (SourceConfig × SinkConfig)
| SourceConfig | SinkConfig | Resolves to |
|---|---|---|
| File | Elasticsearch | `RallyS3ToEs` |
| S3Rally | Elasticsearch | `RallyS3ToEs` |
| File | OpenSearch | `RallyS3ToEs` |
| S3Rally | OpenSearch | `RallyS3ToEs` |
| Elasticsearch | OpenSearch | `EsHitToBulk` |
| OpenSearch | Elasticsearch | `EsHitToBulk` |
| OpenSearch | OpenSearch | `EsHitToBulk` |
| Elasticsearch | Elasticsearch | `EsHitToBulk` |
| File | File | `Passthrough` |
| S3Rally | File | `Passthrough` |
| Elasticsearch | File | `Passthrough` |
| OpenSearch | File | `Passthrough` |
| InMemory | InMemory | `Passthrough` |

### Composer Resolution (from SinkConfig)
| SinkConfig | Composer | Wire Format |
|---|---|---|
| Elasticsearch | `NdjsonComposer` | `item\nitem\n` |
| OpenSearch | `NdjsonComposer` | `item\nitem\n` |
| File | `NdjsonComposer` | `item\nitem\n` |
| InMemory | `JsonArrayComposer` | `[item,item]` |

### Controller Resolution (from `[throttle.source]` → `controller` field)
| Config type | Controller | Behavior |
|---|---|---|
| `static` (default) | `ConfigController` | Returns configured `max_batch_size_docs`, ignores measurements |
| `pid_bytes_to_doc_count` | `PidBytesToDocCount` | PID feedback: measures response bytes → adjusts doc count |

### ThrottleController Resolution (from `[throttle.sink]` → `mode` field)
| ThrottleConfig mode | Controller | Behavior |
|---|---|---|
| `Static` (default) | `StaticThrottleController` | Fixed bytes, no feedback loop |
| `Pid` | `PidControllerBytesToMs` | PID feedback: measure(ms) → adjust byte output (LICENSE-EE/BSL) |

## Responsibility Boundaries
| Component | Responsibility |
|---|---|
| Source | `pump(hint)` → raw page. Format-ignorant. None = EOF. |
| SourceWorker | Owns `ControllerBackend`. Calls `output()` → `pump(hint)` → `measure()`. |
| Channel | Carry `String` (raw pages) |
| SinkWorker | Buffer raw pages by byte target, flush via Composer, call `drain()`, measure duration |
| ThrottleControllerBackend | Dynamic max request size. Static: fixed. PID: latency feedback. |
| Composer | Transform pages (via Transformer) + assemble wire-format payload |
| Transform | Per-page → `Vec<Cow<str>>` (Borrowed=passthrough, Owned=conversion) |
| Sink | Pure I/O: `drain(payload)` → HTTP POST / file write / memory push |
| CancellationToken | Graceful shutdown signal. Workers select! on it at every loop. |

## Public API (for kvx-cli)
- `kvx::app_config::{AppConfig, RuntimeConfig, SourceConfig, SinkConfig, ThrottleAppConfig, SourceThrottleConfig, SinkThrottleConfig}`
- `kvx::transforms::{FlowDescriptor, supported_flows()}`
- `kvx::backends::{FileSourceConfig, FileSinkConfig, ElasticsearchSourceConfig, ElasticsearchSinkConfig, S3RallySourceConfig, RallyTrack}`
- `kvx::stop()` — triggers `CancellationToken` for graceful shutdown

# Notes for future reference

- POC/MVP stage — API surface is unstable
- `common.rs` (`Hit`/`HitBatch`) was purged — pipeline uses raw pages throughout
- `controllers/` module **renamed** to `throttlers/` in Phase 1 — `ThrottleConfig` moved to `throttlers.rs`
- `supervisors/workers/` **flattened** to `workers/` in Phase 3
- `AppConfig` fields renamed: `source_config` → `source`, `sink_config` → `sink` (Phase 4)
- `app_config.rs` **split** into module dir in Phase 5: `app_config/source_config.rs` + `app_config/sink_config.rs`
- Factory functions moved to enum methods: `build_backend()` on `SourceConfig`/`SinkConfig` (Phase 5)
- `ThrottleControllerBackend` gained `Clone` + `from_config()` in Phase 5
- `CancellationToken` (tokio-util) added Phase 6 — workers use `tokio::select!` — `kvx::stop()` is public API
- `BUFFER_EPSILON_BYTES` = 64 KiB headroom before max request size
- PID (batch sizing): Kp = max/min, Ki = Kp/50, Kd = sqrt(Kp×Ki), EMA α=0.25 (measure) / α=0.75 (adj)
- PID (throttle): gains auto-tuned from ratio initial→set_point, EMA α=0.25, anti-windup ±5×set_point
- Each SinkWorker owns its ThrottleControllerBackend exclusively — no shared mutable state, no Mutex
- `escape_json_string()` avoids serde round-trip for action line construction (in rally_s3_to_es + es_hit_to_bulk)
- ES/OS source: PIT + `search_after` pagination; output NDJSON with `_id`, `_index`, `_source` per hit
- OpenSearch sink: `danger_accept_invalid_certs` for dev clusters; SigV4 stubbed

# Aggregated Context Memory Across Sessions for Current and Future Use

- v1-v9: See prior README revisions (transforms, collectors, backend file splits, etc.)
- v10 raw pages + composers: Source → `Option<String>`, Transform → `Vec<Cow<str>>`, Composer replaces Collector
- v11 config migration: `RuntimeConfig`/`SourceConfig`/`SinkConfig` → `app_config.rs`; `supervisors/config.rs` deleted
- v12 S3 Rally source: `S3RallySource` streams Rally benchmark data from S3 via AWS SDK
- v13 public API + FlowDescriptor: `supported_flows()`, `RallyTrack::from_str()`, drift-detection test
- v14 PID controller (batch sizing): `Controller` trait, `PidBytesToDocCount`, `set_page_size_hint()` on Source
- v15 PID controller throttling: `ThrottleController` trait, `PidControllerBytesToMs`, `ThrottleConfig` in `ThrottleAppConfig`
- v16 OpenSearch backend + ES source: real PIT + search_after, `EsHitToBulk` transform, 69 tests
- v17 six-phase refactor (101 kvx tests, 9 kvx-cli tests = 110 total):
  - Phase 1: `controllers/` → `throttlers/`; `ThrottleConfig` moved to `throttlers.rs`
  - Phase 2: `sink.send()` → `sink.drain()`; `source.next_page()` → `source.pump(doc_count_hint)`; `set_page_size_hint()` eliminated
  - Phase 3: `supervisors/workers/` → `workers/` (top-level flat)
  - Phase 4: `ThrottleAppConfig` with `[throttle.source]` + `[throttle.sink]`; `source_config`/`sink_config` → `source`/`sink` in `AppConfig`
  - Phase 5: `ThrottleControllerBackend` gains `Clone` + `from_config()`; `app_config.rs` split to module dir; `build_backend()` factory on config enums
  - Phase 6: `CancellationToken` (tokio-util) added; workers use `tokio::select!`; `kvx::stop()` public API

## Current TOML Config Schema (v17)
```toml
[runtime]
queue_capacity = 8
sink_parallelism = 8

[source.File]
file_name = "input.json"

[sink.Elasticsearch]
url = "http://localhost:9200"
index = "target-index"

[throttle.source]
max_batch_size_bytes = 131072
max_batch_size_docs = 10000

[throttle.sink]
max_request_size_bytes = 131072
# mode = "Pid"
# set_point_ms = 8000
# min_bytes = 1048576
# max_bytes = 104857600
# initial_output_bytes = 10485760
```

- v18 code review + hardening (current — 117 kvx tests, 9 kvx-cli tests = 126 total):
  - Blanket `#![allow(dead_code, unused_variables, unused_imports)]` removed from both crates
  - 9 unused imports/variables cleaned up (file_sink, file_source, in_mem_sink, composers, supervisors)
  - All 17 clippy warnings resolved: collapsible_if, derivable_impl, needless_borrow, doc formatting, is_multiple_of, empty doc line, large_enum_variant (Box<RunArgs>)
  - `SinkBackend`/`SourceBackend` enums changed from `pub` to `pub(crate)` — fixes 7 private_interfaces warnings
  - CI/CD enhanced: `cargo clippy -- -D warnings`, `cargo fmt --check`, cargo caching, pinned toolchain
  - +16 new unit tests: ES sink (3), ES source (4), File sink (3), File source (3), progress.rs (3)
  - `max_request_size_bytes` param in `start_workers` annotated as unused (TODO: wire to SinkWorker)

## Available Rally Tracks
big5, clickbench, eventdata, geonames, geopoint, geopointshape, geoshape, http_logs, nested, neural_search, noaa, noaa_semantic_search, nyc_taxis, percolator, pmc, so, treccovid_semantic_search, vectorsearch
