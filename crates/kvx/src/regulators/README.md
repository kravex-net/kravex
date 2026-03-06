

# Regulators

Adaptive throttling via feedback control. Regulators dynamically adjust a set point based on sink pressure signals.

## Vocabulary

| Term | Definition |
|---|---|
| **Regulator** | Controls a value based on feedback signals |
| **FlowKnob** | Shared atomic value read by Joiner/Manifold to size payloads |
| **Manometer** | Background poller that reads sink metrics (CPU pressure) |
| **FlowMaster** | Consumer of pipeline signals — drives the regulator, adjusts the FlowKnob |
| **PipelineSignal** | Event from the pipeline (CPU reading, 429 response, drain result) |

## Trait

| Trait | Method | Returns | Purpose |
|---|---|---|---|
| `Regulate` | `regulate(measurement)` | `usize` | Given a pressure measurement, return the desired payload size |

## Dispatcher Enum

`Regulators` — routes to concrete regulator based on config.

## Concrete Regulators

| Regulator | Behavior |
|---|---|
| `ByteValue` (Static) | Returns a fixed value — no regulation |
| `CpuPressure` (PID) | PID controller targeting a CPU utilization setpoint |

## Signal Flow

```
Manometer (polls sink) → PipelineSignal::CpuReading(cpu_percent : usize) → FlowMaster → Regulator → FlowKnob
Drainer (HTTP result)  → PipelineSignal::TooManyRequests/DrainError(error_code : ErrorCode) → FlowMaster → FlowKnob
Drainer (success)      → PipelineSignal::DrainSuccess(duration : usize) → FlowMaster → FlowKnob
```

## Key Concepts

- **PID Controller**: Proportional-Integral-Derivative feedback loop for smooth convergence
- **429 Backoff**: Immediate halve on rate-limit response, cooldown before resuming PID
- **EMA Smoothing**: Exponential moving average on CPU readings to dampen noise
- **Auto-tuned Gains**: PID gains derived from min/max payload size ratio
- **FlowKnob is atomic**: Lock-free reads from hot-path workers

## Knowledge Graph

```
Regulate trait → Regulators enum → ByteValue | CpuPressure
Manometer → polls _nodes/stats → sends CpuReading via mpsc to Pipeline signal
FlowMaster → receives PipelineSignal → runs Regulator → writes FlowKnob
Drainer → sends DrainSuccess/TooManyRequests/DrainError via mpsc
FlowKnob → read by Joiner which passes to Manifold for dynamic sizing
lib.rs → spawns Manometer + FlowMaster + signal channel
```
