

# Casts

Feed format transformation layer. A Caster converts raw feeds from a Source into sink-ready documents.

## Trait

| Trait | Method | Returns | Purpose |
|---|---|---|---|
| `Caster` | `cast(feed)` | `Result<String>` | Transform one feed into sink-ready doc(s) |
| `Caster` | `lines_per_doc()` | `usize` | Number of output lines per logical document (default 1) |

## Dispatcher Enum

`DocumentCaster` — routes to concrete caster based on source/sink config combination.

## Concrete Casters

| Caster | Source → Sink | Transformation |
|---|---|---|
| `Passthrough` | Any → same format | Identity — feed passes through unchanged |
| `NdJsonToBulk` | File → Elasticsearch | Wraps each NDJSON line with a `_bulk` action line |
| `PitToBulk` | Elasticsearch → Elasticsearch | Extracts hits from PIT search response, emits `_bulk` NDJSON |

## Resolution

Caster selection is determined by the **source x sink config** combination at startup via `from_configs()`.

## Key Concepts

- **Stateless**: Casters hold no state — pure transformation
- **Zero-sized**: All casters are zero-sized structs (Clone + Copy for free)
- **lines_per_doc**: Bulk casters return 2 (action line + document line), others return 1

## Knowledge Graph

```
Caster trait → DocumentCaster enum → Passthrough | NdJsonToBulk | PitToBulk
DocumentCaster → resolved by from_configs(SourceConfig, SinkConfig)
Caster → consumed by Manifold during join()
lines_per_doc → used by Manifold for size-aware chunking
```
