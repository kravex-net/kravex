# Summary

Search migrations for devs with better things to do. No tuning. No babysitting. Just migrate and move on.

# Description

Kravex automatically optimizes search migrations — no knobs, no guesswork. Smart throttling adapts to destination limits (429 backoff/ramp). Smart cutovers provide retries, validation, recovery, pause, and resume.

# Knowledge Graph

- **Workspace root**: Cargo workspace with members `crates/kvx`, `crates/kvx-cli`
- **`kvx`**: Core library — throttling, cutover logic, retry/recovery, adaptive throughput
- **`kvx-cli`**: CLI binary — user-facing entry point wrapping `kvx`
- **Edition**: 2024

# Key Concepts

- Zero-config optimization
- Adaptive throttle (429 backoff/ramp)
- Smart cutovers (retry, validation, recovery, pause, resume)

# Development

## Prerequisites

- Rust toolchain (edition 2024)
- VS Code with [CodeLLDB](https://marketplace.visualstudio.com/items?itemName=vadimcn.vscode-lldb) extension

## VS Code Tasks & Launch

`.vscode/tasks.json` provides:
- `cargo build --workspace` (default build — `Ctrl+Shift+B`)
- `cargo check --workspace`, `cargo test --workspace`, `cargo clippy --workspace`
- `cargo build -p kvx-cli` (targeted build for launch configs)

`.vscode/launch.json` provides:
- **Debug kvx-cli** (`F5`) — LLDB debugger attached
- **Run kvx-cli (no debug)** (`Ctrl+F5`)

# Notes

- POC/MVP stage — API surface is unstable
- Keep dependency footprint minimal
