

# Manifolds

## Vocabulary

| Term         | Definition                                                    |
| ------------ | ------------------------------------------------------------- |
| **Feed**     | Raw result page from a Source                                 |
| **Doc**      | Single sink-ready string produced by casting a feed           |
| **Payload**  | Joined docs in wire format, ready for the sink                |
| **Caster**   | Stateless: one feed in, many docs out                         |
| **Manifold** | Stateful: casts feeds → docs, buffers docs, flushes payloads |
| **Joiner**   | Worker: buffers feeds, drives the Manifold, forwards payloads |

## Pipeline

```
[channel 1: feeds] → Joiner → Manifold → [channel 2: payloads]
                        │          │
                        │          ├─ casts feeds → docs (via Caster)
                        │          ├─ lazy iteration over feeds, drops the feed once all docs in feed are cast
                        │          
                        │
                        └─ buffers feeds by byte count
                           sends feeds through Manifold
                           receives docs from Manifold
                           buffers docs by set point
                           extra docs are kept around until the next flush, and not included in the current payload
                           creates payloads from docs
                           forwards payloads to channel 2
```

## How It Works

1. **Joiner** accumulates feeds from channel 1 until a byte threshold is reached
2. **Joiner** passes the buffered feeds to the **Manifold**
3. **Manifold** casts each feed into docs (via the **Caster**), adding them to its doc buffer
4. When the doc buffer reaches the setpoint, the Manifold flushes it as a payload
5. Leftover feeds and docs that didn't reach the setpoint stay in the Manifold (FIFO carry-over)
6. On the next call, carried-over state is processed first, then new feeds
7. When the source is exhausted, the Joiner triggers a final flush — all remaining docs drain as one last payload

## Key Concepts

- **Two-level buffering:** Joiner buffers feeds, Manifold buffers docs
- **Both setpoints are dynamic** — read from FlowKnob, adjusted by backpressure
- **Manifold is stateful** — carries over unconsumed feeds and docs between calls
- **Caster is stateless** — transforms only, no buffering or joining

## Knowledge Graph

```
Joiner ──buffers──→ feeds
Joiner ──flushes──→ feeds into Manifold
Manifold ──casts via──→ Caster (feed → docs)
Manifold ──buffers──→ docs (stateful carry-over)
Manifold ──flushes──→ payload(s) at dynamic setpoint
Joiner ──forwards──→ payloads to channel 2
FlowKnob ──controls──→ both Joiner + Manifold setpoints
FlowMaster ──adjusts──→ FlowKnob (via regulator)
```
