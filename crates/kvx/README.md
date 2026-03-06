# Summary

Core library for kravex — the data migration engine. 3-stage pipeline: Pumper (async I/O) → Joiner (sync CPU, std::thread) → Drainer (async I/O). Cow-powered zero-copy, Manifold-based payload assembly, and a clean config ownership model.

# Description

`kvx` provides the foundational primitives for search migration: throttling, cutover logic, retry/recovery, and adaptive throughput. This crate is consumed by `kvx-cli` and any future integrations.

# Knowledge Graph

- **Workspace member**: `crates/kvx`
- **Dependents**: `kvx-cli`
- **Dependencies**: anyhow, async-channel, figment, memchr, reqwest, serde, serde_json, tokio, tracing, async-trait, futures, indicatif, comfy-table
- **Dev-Dependencies**: wiremock, criterion, tempfile
- **Edition**: 2024
- **Modules**: `app_config`, `backends`, `casts`, `manifolds`, `workers`, `regulators`, `foreman`, `progress`

## Pipeline Architecture (current — 3-stage: Pumper → Joiner → Drainer)
```
Source.next_page() → Option<String> (raw page)
  → ch1 (async_channel, bounded, MPMC)
  → Joiner(s) on std::thread (recv_blocking → buffer → manifold.join(buffer, caster) → send_blocking)
  → ch2 (async_channel, bounded, MPMC)
  → Drainer(s) on tokio (recv → sink.send)
  → Sink (HTTP POST, file write, memory push)
```

### Why 3 stages?
- **Pumper**: async — blocked on I/O (source reads, HTTP, file)
- **Joiner**: sync std::thread — CPU-bound (JSON parsing, casting, manifold join, buffering)
- **Drainer**: async — blocked on I/O (sink writes, HTTP, file)

Separating CPU work onto OS threads prevents starving tokio's async I/O workers.

# Key Concepts

- **Sources return `Option<String>`**: one raw page per call, content uninterpreted. `None` = EOF
- **Sinks are I/O-only**: accept a fully rendered payload `String`, send it
- **Joiner buffers raw pages** by byte size, flushes via Manifold when buffer approaches `max_request_size_bytes`
- **Caster** (`DocumentCaster`): per-page format conversion (NdJsonToBulk, Passthrough)
- **Manifold** (`ManifoldBackend`): cast + assemble in one shot:
  - ES/File → `NdjsonManifold`: items joined with `\n`, trailing `\n`
  - InMemory → `JsonArrayManifold`: `[item,item,item]`, zero serde
- **All abstractions follow the same pattern**: trait → concrete impls → enum dispatcher → from_config resolver
- **Joiner threads**: CPU-bound work on `std::thread`, not tokio. Uses `recv_blocking()`/`send_blocking()` on async_channel
- **Drainer is thin**: just recv from ch2, send to sink. No buffering, no casting, no manifold

# Notes

- POC/MVP stage — API surface is unstable
- Casters and manifolds are zero-sized structs — cloning per-worker is free
- All abstractions follow: trait → concrete impl → enum dispatcher → from_config resolver
- Pipeline cascade: pumper done → ch1 closes → joiners flush+exit → ch2 closes → drainers exit
