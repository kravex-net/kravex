

# kvx Source Root

Core library source for kravex — the zero-config search migration engine.

## Module Map

| Module | Purpose |
|---|---|
| `app_config` | Configuration hierarchy — AppConfig, RuntimeConfig, SourceConfig, SinkConfig |
| `backends` | I/O abstraction — Source/Sink traits, backend-specific implementations |
| `casts` | Feed transformation — Caster trait, format conversion between source and sink |
| `manifolds` | Payload assembly — cast feeds into docs, buffer and flush as wire-format payloads |
| `workers` | Pipeline stages — Pumper (async read), Joiner (sync CPU), Drainer (async write) |
| `regulators` | Adaptive throttling — PID controller, pressure gauges, flow control |
| `foreman` | Orchestration — spawns and joins all pipeline workers |
| `progress` | TUI metrics and progress reporting |
| `lib.rs` | Entry point — wires up config, regulators, foreman, shutdown |

## Pipeline Vocabulary

| Term | Definition |
|---|---|
| **Feed** | Raw result page from a Source |
| **Cast** | Transform a feed into sink-ready doc(s) |
| **Payload** | Wire-format string ready for the sink |
| **Pump** | Read the next feed from a source |
| **Drain** | Write a payload to a sink |

## Architecture

```
Source.pump() → ch1 → Joiner(cast+join) → ch2 → Drainer(drain)
```

Three-stage pipeline: async I/O → sync CPU → async I/O. Channels are bounded async_channel (MPMC).

## Knowledge Graph

```
lib.rs → AppConfig → Foreman → Workers (Pumper, Joiner, Drainer)
lib.rs → Regulators → Manometer + FlowMaster → FlowKnob
Foreman → Source (via Pumper), Sink (via Drainer)
Joiner → Caster + Manifold (cast feeds, assemble payloads)
```
