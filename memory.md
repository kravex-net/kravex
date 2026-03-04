# Memory — Kravex Development Session

## Current State (v23)
- Branch: `perf/allocation-optimization-and-bulk-fixes`
- Tests: 161 kvx + 9 kvx-cli = 170 total (all passing)
- Clippy: 2 pre-existing warnings (collapsible if in ES/OS sinks) — not from our changes

## Completed Work This Session

### v21: Buffer Pool + PoolBuffer Pipeline
- `buffer_pool` module: PoolBuffer struct, global BufferPool with 8 size buckets (64B–8MB), TLS fast path
- `cache_aligned` module: CacheAligned<T> with #[repr(align(64))]
- All traits updated: Source→PoolBuffer, Sink←PoolBuffer, Transform→transform_into_pool_buffer(), Composer←&[PoolBuffer]
- Channel carries PoolBuffer instead of String

### v22: TransformWorker + Two-Channel Pipeline
- TransformWorker: CPU-bound worker between Source and Sink
- Two channels: Source → Channel1 → TransformWorker(s) → Channel2 → SinkWorker(s)
- transform_parallelism config (default: num_cpus - 1)
- SinkWorker simplified: removed transformer/composer, pure I/O

### v23: Allocation Optimization (CURRENT)
- `std::fmt::Write` impl on PoolBuffer — enables `write!()` macro into buffer
- `write_hit_to_buffer()` in es_hit_to_bulk: zero per-doc heap allocs
- `write_rally_doc_to_buffer()` in rally_s3_to_es: zero per-doc heap allocs (fmt::Write for action, io::Write for serde body)
- TransformWorker: entire loop in single `spawn_blocking()` — eliminates per-page spawn_blocking hop
- TransformWorker: `recv_blocking()`/`send_blocking()` — pure blocking thread, no async
- TransformWorker: `&[raw_page]` stack slice replaces `vec![raw_page]` — zero Vec alloc
- `compose_page()` gated with `#[cfg(test)]` — dead code in production
- SinkWorker: single-payload fast path skips concat (zero memcpy when buffer has 1 payload)
- Per-page savings: 2 ctx switches + 1 Vec alloc + N×(String alloc+free) where N=docs/page

## Pre-existing Issues
- `the_one_where_s3_rally_config_materializes_from_toml` test: sometimes fails in full suite run (Pmc vs Geonames), passes when run individually — test ordering issue, not our code
- 2 clippy warnings in elasticsearch_sink.rs:299 and opensearch_sink.rs:276 (collapsible if) — pre-existing

## Key Architecture Facts
- Pipeline: Source → Channel1(raw PoolBuffer) → TransformWorker(blocking thread) → Channel2(composed PoolBuffer) → SinkWorker(async) → Sink
- PoolBuffer implements both `io::Write` (serde) and `fmt::Write` (write! macro)
- Zero-sized transform/composer structs: Clone+Copy, owned by TransformWorker
- SinkWorker is pure I/O — no transform, no compose
- Cancellation: SourceWorker+SinkWorker use tokio::select!, TransformWorker uses sync is_cancelled()
