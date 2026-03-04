# Summary

Core library for kravex — the zero-config search migration engine. Raw pages, Cow-powered zero-copy, Composer-based payload assembly, adaptive throttling, graceful cancellation. The borrow checker approved this message (after 47 rejections).

# Description

`kvx` provides the foundational primitives for search migration: throttling, cutover logic, retry/recovery, and adaptive throughput. Consumed by `kvx-cli` and future integrations. Now with 6 phases of refactoring and a fresh coat of existential dread.

# Knowledge Graph

- **Workspace member**: `crates/kvx`
- **Dependents**: `kvx-cli`
- **Dependencies**: anyhow, async-channel, figment, reqwest, serde, serde_json, tokio, tokio-util, tracing, async-trait, futures, indicatif, comfy-table, aws-sdk-s3, aws-config
- **Edition**: 2024
- **Test count**: 157 (134 kvx + 9 kvx-cli + 14 composer/source tests updated for PoolBuffer)
- **Dependencies added**: `base64 = "0.22"` (pre-computed auth headers)
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
  - `workers/source_worker` — `SourceWorker` (owns `ControllerBackend`, calls `source.pump(hint)`, sends `PoolBuffer` via channel)
  - `workers/sink_worker` — `SinkWorker` (buffers `Vec<PoolBuffer>`, calls `sink.drain(PoolBuffer)`, owns `ThrottleControllerBackend`)
  - `supervisors` — `Supervisor` orchestration (spawns workers, passes `CancellationToken`, creates `channel(PoolBuffer)`)
  - `composers` — `Composer` trait + `NdjsonComposer`/`JsonArrayComposer` + `ComposerBackend`; accepts `&[PoolBuffer]`, returns `PoolBuffer`
  - `transforms` — `Transform` trait + `DocumentTransformer` enum; `transform_into_pool_buffer()` hot path; `RallyS3ToEs`, `EsHitToBulk`, `NdjsonToBulk`, `Passthrough`
  - `buffer_pool` — `PoolBuffer` struct (Vec<u8> wrapper), global `BufferPool` with 8 size buckets (64B–8MB), TLS fast path + shared fallback; `rent()`, `rent_from_string()` free fns; `Write` + `PartialEq<str>` + `From<String>` + `From<Vec<u8>>` traits
  - `cache_aligned` — `CacheAligned<T>` wrapper with `#[repr(align(64))]` to prevent false sharing across CPU cache lines
  - `progress` — TUI metrics
  - *(`common` — removed: Hit/HitBatch dead code purged)*

## Pipeline Architecture (v21 — buffer pool + PoolBuffer pipeline)
```
ThrottleControllerBackend.output() → dynamic byte target
ControllerBackend.output() → doc_count_hint
  → Source.pump(doc_count_hint) → Option<PoolBuffer> (raw page from pool)
  → ControllerBackend.measure(page.len())
  → channel(PoolBuffer)
  → SinkWorker buffers Vec<PoolBuffer> (by byte target)
  → Composer.compose(&[PoolBuffer], &transformer) → PoolBuffer payload
  → Sink.drain(PoolBuffer) → measure duration → ThrottleControllerBackend.measure(ms)
  → ThrottleControllerBackend.output() → next cycle's byte target
  ↑ tokio::select! on CancellationToken at every loop iteration (workers)

Buffer lifecycle: rent() → fill (source/transform) → channel → compose → drain → Drop (return to pool)
Pool architecture: 8 size buckets (64B, 256B, 1KB, 4KB, 16KB, 64KB, 256KB, 8MB)
  → TLS per-thread cache (fast path, no contention)
  → Shared Arc<Mutex> fallback (rare, cross-thread returns)
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
  │              │         ◄── buffer_pool (PoolBuffer in Source/Sink traits)
  │              │
  ├──► buffer_pool ◄── backends, composers, workers, transforms (PoolBuffer everywhere)
  ├──► cache_aligned (CacheAligned<T> — false sharing prevention)
  ├──► throttlers ◄── app_config (ThrottleAppConfig re-exported)
  │         ◄── workers/source_worker (ControllerBackend)
  │         ◄── workers/sink_worker   (ThrottleControllerBackend)
  ├──► workers ──► supervisors (spawned with CancellationToken)
  ├──► transforms ◄── Composer (transform_into_pool_buffer() hot path)
  └──► composers  ◄── SinkWorker (holds ComposerBackend + DocumentTransformer)
```

# Key Concepts

- **`source.pump(doc_count_hint)`**: one call, hint included. Returns `Option<PoolBuffer>` (None = EOF). Buffer rented from global pool, filled by source backend.
- **`sink.drain(payload)`**: accepts `PoolBuffer`. ES/OS sinks call `payload.into_vec()` → reqwest body (zero-copy handoff). File sink borrows `payload.as_bytes()`. InMemory converts via `as_str()`.
- **`ThrottleAppConfig`**: top-level `[throttle]` TOML section. Contains `source: SourceThrottleConfig` and `sink: SinkThrottleConfig`. No longer embedded in backend configs.
- **`SourceThrottleConfig`** (`[throttle.source]`): `max_batch_size_bytes`, `max_batch_size_docs`, `ControllerConfig`.
- **`SinkThrottleConfig`** (`[throttle.sink]`): `max_request_size_bytes`, `ThrottleConfig` (Static or Pid).
- **`build_backend()`**: factory method on `SourceConfig`/`SinkConfig` enums. Replaces ad-hoc resolver functions. Config owns its own instantiation.
- **`ThrottleControllerBackend::from_config()`**: factory from `SinkThrottleConfig`. Added `Clone`.
- **`CancellationToken`** (tokio-util): passed to every worker. Workers use `tokio::select!` to exit cleanly on `kvx::stop()`.
- **`throttlers` module**: consolidated from `controllers/`. Owns ALL throttle + controller logic.
- **`workers` module**: flattened from `supervisors/workers/`. `SourceWorker` + `SinkWorker` are top-level.
- **`AppConfig` fields**: `source`, `sink`, `runtime`, `throttle` (renamed from `source_config`/`sink_config`).
- **Zero-copy passthrough**: NDJSON→NDJSON via `Cow::Borrowed` (transform) or direct buffer copy (transform_into_pool_buffer). No per-doc alloc.
- **`PoolBuffer`**: Vec<u8> wrapper with pool lifecycle. `rent(size)` allocates from TLS cache or shared pool. `push_str()`, `push_byte()`, `extend_from_slice()` for writes. `as_str()`, `as_bytes()` for reads. `into_vec()` for ownership transfer. `Drop` returns buffer to pool. Implements both `std::io::Write` (for `serde_json::to_writer`) and `std::fmt::Write` (for `write!()` macro) — universal write target.
- **`BufferPool`**: global pool with 8 size buckets (64B–8MB). TLS per-thread fast path (no contention). Shared `Arc<Mutex>` fallback for cross-thread returns. Inspired by C# ArrayPool.
- **`CacheAligned<T>`**: `#[repr(align(64))]` wrapper preventing false sharing. Adjacent CacheAligned values guaranteed on different cache lines.
- **`transform_into_pool_buffer()`**: hot-path Transform method. Reads from source PoolBuffer, writes directly to output PoolBuffer. Zero per-doc heap allocations — `fmt::Write` for action lines, `io::Write` for serde body, `push_str()` for raw JSON memcpy.
- **Pre-computed bulk URL + auth header**: ES/OS sinks compute `format!()` URL and `Authorization: Basic <base64>` header once at construction — zero per-request string formatting.
- **`apply_auth_raw()` removed** from `opensearch_sink.rs` — replaced by pre-computed header field.
- **`sink_config` field removed** from both ES and OS sinks — backend configs are pure connection detail, no sizing bleed.
- **Response body skip**: when `content-length <= 2`, skip response body read entirely (empty bulk = `{}` or `[]`).
- **TCP_NODELAY + pool_idle_timeout**: `reqwest` clients built with `tcp_nodelay(true)` + `pool_idle_timeout(30s)` — reduces latency under high concurrency.
- **All abstractions**: trait → concrete impls → enum dispatcher → `from_config()` factory.
- **Transforms/composers are Clone+Copy** (zero-sized structs). Each TransformWorker gets its own copy. SinkWorker no longer owns transform/compose — pure I/O only.

## Architecture Pattern
```
┌──────────────────┐  ┌─────────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│ trait Source      │  │ trait Transform          │  │ trait Composer        │  │ trait Controller      │
│   pump(hint)      │  │   transform(&str)        │  │   compose(&[PBuf])   │  │   output() → usize   │
│   → Option<PBuf>  │  │   transform_into_pbuf()  │  │   → PoolBuffer       │  │   measure(f64)       │
└────────┬─────────┘  └────────┬──────────────────┘  └────────┬───────────┘  └────────┬─────────────┘
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

Two-Channel Pipeline (v22):
  Source → [Channel1: raw PoolBuffer] → TransformWorker(s) → [Channel2: composed PoolBuffer] → SinkWorker(s) → Sink
              (MPMC bounded)            N = num_cpus - 1        (MPMC bounded)                   N = sink_parallelism
                                        CPU-bound: transform    Pure I/O: concat + drain
                                        + compose per page      No transform, no compose
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
| Source | `pump(hint)` → raw PoolBuffer page. Format-ignorant. None = EOF. |
| SourceWorker | Owns `ControllerBackend`. Calls `output()` → `pump(hint)` → `measure()`. Sends raw PoolBuffer to Channel1. |
| Channel1 | Carry raw `PoolBuffer` pages: Source → TransformWorker(s) |
| TransformWorker | Receives raw PoolBuffer via `recv_blocking()`, calls `compose(&[page])` directly (sync), sends via `send_blocking()`. Entire loop in single `spawn_blocking` — pure CPU, zero async hops. N = `transform_parallelism` (default: num_cpus - 1). |
| Channel2 | Carry composed `PoolBuffer` payloads: TransformWorker(s) → SinkWorker(s) |
| SinkWorker | Buffer composed PoolBuffers by byte target. Single-payload fast path (skip concat). Multi-payload: concatenate into single PoolBuffer. `drain()` + measure duration. Pure I/O — no transform, no compose. |
| ThrottleControllerBackend | Dynamic max request size. Static: fixed. PID: latency feedback. |
| Composer | Assemble wire-format payload from pre-transformed pages (NDJSON or JSON array). Called by TransformWorker. |
| Transform | Per-page → `transform_into_pool_buffer(source, output)` — writes directly to output PoolBuffer. |
| Sink | Pure I/O: `drain(payload: PoolBuffer)` → HTTP POST / file write / memory push |
| CancellationToken | Graceful shutdown signal. SourceWorker + SinkWorker: `tokio::select!`. TransformWorker: synchronous `is_cancelled()` check. |

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
- ES/OS sink bulk timeout: 120s (was 30s — too short for large PID batches, caused partial commits + auto-gen _id duplicates on retry)
- ES/OS sink bulk response checking: item-level failure detection — errors no longer silently swallowed
- FileSource page range: `0..` (was `0..=` — off-by-one fence post, emitted one extra empty page)
- Bulk URL routing: index field included in bulk URL for proper shard routing (Bug 6 fix)
- Pre-computed bulk URL: `format!("{url}/{index}/_bulk")` called once in constructor, stored as `String` field — zero per-request alloc
- Pre-computed auth header: `base64::encode("user:pass")` called once, stored as `Authorization: Basic <b64>` header value — zero per-request format!()
- `apply_auth_raw()` removed from `opensearch_sink.rs` — was applying auth per-request via mutable HeaderMap; now stateless
- `sink_config` field removed from ES + OS sinks — sizing fields were never read from sinks directly; ownership was confusing
- Response body skip: `content-length <= 2` guard before `response.bytes()` — avoids alloc + deser on empty bulk responses
- TCP_NODELAY: eliminates Nagle algorithm delay on bulk POSTs — critical for many small-ish requests
- `pool_idle_timeout(30s)`: closes idle keep-alive connections after 30s — prevents stale socket errors on long migrations

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

## Current TOML Config Schema (v22)
```toml
[runtime]
queue_capacity = 8
sink_parallelism = 8
transform_parallelism = 7  # defaults to num_cpus - 1

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

- v18 code review + hardening (117 kvx tests, 9 kvx-cli tests = 126 total):
  - Blanket `#![allow(dead_code, unused_variables, unused_imports)]` removed from both crates
  - 9 unused imports/variables cleaned up (file_sink, file_source, in_mem_sink, composers, supervisors)
  - All 17 clippy warnings resolved: collapsible_if, derivable_impl, needless_borrow, doc formatting, is_multiple_of, empty doc line, large_enum_variant (Box<RunArgs>)
  - `SinkBackend`/`SourceBackend` enums changed from `pub` to `pub(crate)` — fixes 7 private_interfaces warnings
  - CI/CD enhanced: `cargo clippy -- -D warnings`, `cargo fmt --check`, cargo caching, pinned toolchain
  - +16 new unit tests: ES sink (3), ES source (4), File sink (3), File source (3), progress.rs (3)
  - `max_request_size_bytes` param in `start_workers` annotated as unused (TODO: wire to SinkWorker)

- v19 PID duplicate bug fix (117 kvx tests, 7 kvx-cli tests = 124 total):
  - Root cause: NOT in Rust pipeline (proven by 7 new tests targeting every plausible pipeline stage)
  - ES bulk timeout 30s → 120s: large PID batches exceeded 30s → partial commit → auto-gen `_id` on retry = duplicates
  - ES + OS sink: bulk response body now parsed; item-level `error` fields detected and surfaced (were silently dropped)
  - FileSource off-by-one: `0..=page_count` → `0..page_count` (emitted one empty trailing page)
  - Bulk URL routing: index included in URL for proper ES/OS shard routing (Bug 6)
  - Files: `file_source.rs`, `elasticsearch_sink.rs`, `opensearch_sink.rs`, `lib.rs`, `rally_s3_to_es.rs`, `ndjson.rs`, `Cargo.toml`

- v20 allocation optimization + request path hardening (current — 134 kvx tests, 9 kvx-cli tests = 143 total):
  - Phase 1: `escape_json_string` returns `Cow<str>` — Borrowed on fast path (99.9% of IDs, no special chars)
  - Phase 2: `FileSource::pump` reuses `line_buffer` + `page_buffer` fields — was re-allocating per call
  - Phase 3: `transform_single_rally_doc` collapsed from 3 allocs to 1 — single `write!` into output String
  - Phase 4: `serde_json::to_writer` into output buffer via `unsafe as_mut_vec` — same pattern as `es_hit_to_bulk`
  - Phase 5: `Transform::transform_into_ndjson` streaming trait method — `RallyS3ToEs` + `EsHitToBulk` override to skip `Vec<Cow>` entirely
  - Phase 6: `NdjsonComposer` uses `transform_into_ndjson`; size estimate per-doc (+64 × newline_count) vs per-page
  - Pre-computed bulk URL + auth header in ES/OS sinks (zero per-request `format!()`)
  - `apply_auth_raw()` removed from `opensearch_sink.rs`; `sink_config` field removed from both sinks
  - Response body skip when `content-length <= 2`
  - `tcp_nodelay(true)` + `pool_idle_timeout(30s)` on all reqwest clients
  - `base64 = "0.22"` dependency added for header pre-computation
  - dhat profiling baseline: 5.175 GB total allocs in 18s; target 55-70% reduction

- v21 buffer pool + PoolBuffer pipeline (current — 157 tests):
  - `buffer_pool` module: `PoolBuffer` struct (Vec<u8> wrapper), global `BufferPool` with 8 size buckets (64B–8MB)
  - TLS per-thread fast path (zero-contention rent/return), shared `Arc<Mutex>` fallback for cross-thread returns
  - `cache_aligned` module: `CacheAligned<T>` with `#[repr(align(64))]` preventing false sharing
  - Source trait: `pump()` returns `Option<PoolBuffer>` instead of `Option<String>`
  - Sink trait: `drain()` accepts `PoolBuffer` instead of `String`; ES/OS use `into_vec()` for reqwest body
  - Transform trait: `transform_into_pool_buffer()` hot-path method writes directly into output PoolBuffer
  - Composer trait: accepts `&[PoolBuffer]`, returns `PoolBuffer` payload
  - Channel: `async_channel::bounded` now carries `PoolBuffer` instead of `String`
  - SinkWorker: buffers `Vec<PoolBuffer>` instead of `Vec<String>`
  - FileSource: rents PoolBuffer each pump(), eliminates page_buffer clone
  - S3RallySource: rents PoolBuffer each pump(), eliminates String::with_capacity()
  - ES/OS sources: wrap `result.page` String in PoolBuffer via `rent_from_string()`
  - All 4 transforms implement `transform_into_pool_buffer()` override (Passthrough, NdjsonToBulk, EsHitToBulk, RallyS3ToEs)
  - PoolBuffer traits: `Write` (reqwest compat), `PartialEq<str>` (test assertions), `From<String>`, `From<Vec<u8>>`, `Drop` (pool return)

- v22 TransformWorker + two-channel pipeline (current — 161 kvx tests, 9 kvx-cli tests = 170 total):
  - `TransformWorker` module: CPU-bound worker between Source and Sink, owns Transformer + Composer
  - Two-channel architecture: Source → Channel1(raw PoolBuffer) → TransformWorker(s) → Channel2(composed PoolBuffer) → SinkWorker(s)
  - `transform_parallelism` in RuntimeConfig: default `num_cpus - 1` (one core reserved for tokio polling)
  - `--transform-parallelism` CLI arg: optional override, uses RuntimeConfig default when omitted
  - SinkWorker simplified to pure I/O: removed `transformer` and `composer` fields, now only buffers + concatenates + drains
  - SinkWorker `flush_and_measure()`: no longer calls `compose_page()` — concatenates pre-composed PoolBuffers via `extend_from_slice()`
  - Channel close propagation: Source EOF → drops tx1 → Channel1 closes → TransformWorkers drain, drop tx2 → Channel2 closes → SinkWorkers drain, close sinks
  - Cancellation: CancellationToken fires → all 3 worker types see via `select!`, clean exit
  - 4 new TransformWorker tests: transform page, handle channel close, respect cancellation, multi-worker load sharing

- v23 allocation optimization + bulk fixes (current — 161 kvx tests, 9 kvx-cli tests = 170 total):
  - `PoolBuffer` gains `std::fmt::Write` impl — enables `write!()` macro to format directly into buffer (complements existing `std::io::Write` for serde)
  - `write_hit_to_buffer()` in `es_hit_to_bulk.rs`: zero per-doc heap allocs — action line via `fmt::Write`, `_source` via `push_str()` directly into PoolBuffer
  - `write_rally_doc_to_buffer()` in `rally_s3_to_es.rs`: zero per-doc heap allocs — action line via `fmt::Write`, body via `serde_json::to_writer` (`io::Write`) directly into PoolBuffer
  - TransformWorker: entire recv→compose→send loop runs in single `spawn_blocking()` — eliminates per-page `spawn_blocking` hop (was: 2 context switches per page)
  - TransformWorker: `recv_blocking()` / `send_blocking()` from `async_channel` — no async task, no `tokio::select!`, pure blocking thread
  - TransformWorker: `&[raw_page]` stack slice replaces `vec![raw_page]` — eliminates 1 Vec heap alloc per page
  - `compose_page()` gated with `#[cfg(test)]` — no production callers after TransformWorker calls `compose()` directly
  - SinkWorker `flush_and_measure()`: single-payload fast path — when buffer has 1 payload, skip concat entirely (zero memcpy)
  - Per-page savings: 2 context switches + 1 Vec alloc + N×(1 String alloc + 1 String free) where N = docs/page
  - At 10K docs/page: eliminates ~20K allocator calls per page on the hot path

## Available Rally Tracks
big5, clickbench, eventdata, geonames, geopoint, geopointshape, geoshape, http_logs, nested, neural_search, noaa, noaa_semantic_search, nyc_taxis, percolator, pmc, so, treccovid_semantic_search, vectorsearch
