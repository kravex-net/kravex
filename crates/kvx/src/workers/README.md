

# Workers

Pipeline execution stages. Three worker types form the data flow pipeline.

## Worker Types

| Worker | Runtime | Role | I/O Model |
|---|---|---|---|
| **Pumper** | tokio (async) | Reads feeds from Source into ch1 | Async I/O bound |
| **Joiner** | std::thread (sync) | Casts + joins feeds into payloads | CPU bound |
| **Drainer** | tokio (async) | Writes payloads from ch2 to Sink | Async I/O bound |

## Pipeline Flow

```
Source → Pumper → [ch1] → Joiner → [ch2] → Drainer → Sink
```

- **ch1**: Bounded async_channel carrying raw feeds (String)
- **ch2**: Bounded async_channel carrying assembled payloads (String)

## Traits

| Trait | Method | Returns | Purpose |
|---|---|---|---|
| `Worker` | `start()` | `JoinHandle<Result<()>>` | Spawn the worker as an async task |

Note: Joiner does NOT implement Worker — it uses std::thread, not tokio tasks.

## Shutdown Cascade

Pumper completes → ch1 closes → Joiners flush and exit → ch2 closes → Drainers exit

## Key Concepts

- **Three-stage separation**: Async I/O (pump) → sync CPU (cast+join) → async I/O (drain)
- **Drainer is thin**: Pure relay — recv from ch2, send to sink. No buffering, no casting.
- **Joiner is stateful**: Buffers feeds by byte count, flushes the Manifold output
- **Signal reporting**: Drainer sends PipelineSignal to regulators (429, timeout, success)

## Knowledge Graph

```
Foreman → spawns Pumper (1) + Joiner (N) + Drainer (N)
Pumper → Source.pump() → ch1
Joiner → ch1 → Caster + Manifold → ch2
Drainer → ch2 → Sink.drain() + PipelineSignal
Joiner parallelism → RuntimeConfig.joiner_parallelism
Drainer parallelism → RuntimeConfig.sink_parallelism
```
