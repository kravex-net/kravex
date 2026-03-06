

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
                                                ↻ retry with backoff
```

- **ch1**: Bounded async_channel carrying raw feeds (String)
- **ch2**: Bounded async_channel carrying assembled payloads (Payload)

## Traits

| Trait | Method | Returns | Purpose |
|---|---|---|---|
| `Worker` | `start()` | `JoinHandle<Result<()>>` | Spawn the worker as an async task |

Note: Joiner does NOT implement Worker — it uses std::thread, not tokio tasks.

## Shutdown Cascade

Pumper completes → ch1 closes → Joiners flush and exit → ch2 closes → Drainers exit

## Retry & Backoff

Drainer retries failed `sink.send()` calls with configurable exponential backoff.

| Config Field | Default | Description |
|---|---|---|
| `max_retries` | 3 | Maximum retry attempts after initial failure |
| `initial_backoff_ms` | 1000 | Base backoff duration in milliseconds |
| `backoff_multiplier` | 2.0 | Exponential multiplier per retry |
| `max_backoff_ms` | 30000 | Ceiling for backoff duration |

TOML section `[drainer]` — optional, defaults apply when absent.

Backoff formula: `min(initial_backoff_ms * multiplier^attempt, max_backoff_ms)`

Total attempts = 1 (initial) + max_retries. All errors are retried; error classification is a future enhancement.

## Key Concepts

- **Three-stage separation**: Async I/O (pump) → sync CPU (cast+join) → async I/O (drain)
- **Drainer is thin + resilient**: Relay with retry — recv from ch2, send to sink with backoff
- **Joiner is stateful**: Buffers feeds by byte count, flushes the Manifold output

## Knowledge Graph

```
Foreman → spawns Pumper (1) + Joiner (N) + Drainer (N)
Pumper → Source.pump() → ch1
Joiner → ch1 → Caster + Manifold → ch2
Drainer → ch2 → Sink.send() with exponential backoff retry
Drainer config → DrainerConfig (workers/config.rs)
Joiner parallelism → RuntimeConfig.joiner_parallelism
Drainer parallelism → RuntimeConfig.sink_parallelism
```
