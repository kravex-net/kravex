# Summary

CLI interface for kravex — run migrations from the command line with clap-powered arg parsing. Now with graceful shutdown and a renewed sense of purpose. (The borrow checker said it was okay to ship this.)

# Description

`kvx-cli` wraps the `kvx` core library and exposes it as a terminal tool. Supports TOML config files, flat CLI arguments, or both (CLI overrides TOML). Includes `list-flows` for discoverability and a Claude Code skill (`/kvx`) for interactive guided setup.

# Knowledge Graph

- **Workspace member**: `crates/kvx-cli`
- **Dependencies**: `kvx` (path = `../kvx`), `clap` (derive-based), `comfy-table`, `tokio-util`
- **Edition**: 2024
- **Binary crate**
- **Subcommands**: `run`, `list-flows`
- **Config resolution**: TOML base → CLI overrides applied on top
- **Source of truth**: `kvx::transforms::supported_flows()` → drives `list-flows` output
- **Drift detection**: test in transforms.rs ensures `supported_flows()` ↔ `from_configs()` parity
- **Test count**: 9 (kvx-cli crate); 126 total across workspace

# Key Concepts

- `list-flows` — prints valid source→sink combos via `supported_flows()`
- `run` — builds `AppConfig` from `--source`/`--sink` flat args or `--config` TOML (or both)
- **TOML `[throttle]` section** now owns all batch/request sizing: `[throttle.source]` + `[throttle.sink]`; source/sink backend configs are pure connection config
- **CLI throttle args** (`--source-max-batch-size-bytes`, `--source-max-batch-size-docs`, `--sink-max-request-size-bytes`) override `[throttle.*]` from TOML
- `kvx::stop()` wired to `Ctrl+C` for graceful shutdown via `CancellationToken`
- InMemory source/sink: hidden from help/list-flows, available via `--source inmemory`
- Claude Code skill: `.claude/commands/kvx.md` — interactive migration setup

## CLI Usage

```bash
# List available migration flows
kvx list-flows

# Run file → elasticsearch
kvx run --source file --source-file-name input.json \
  --sink elasticsearch --sink-url http://localhost:9200 --sink-index my-index \
  --sink-parallelism 8

# Run with explicit throttle overrides
kvx run --source file --source-file-name input.json \
  --sink elasticsearch --sink-url http://localhost:9200 --sink-index my-index \
  --source-max-batch-size-docs 5000 \
  --sink-max-request-size-bytes 5242880

# Run s3-rally → file (download benchmark data)
kvx run --source s3-rally --source-track geonames --source-bucket rally-tracks-bench \
  --sink file --sink-file-name geonames.json

# Run with TOML config
kvx run --config kvx.toml

# TOML base + CLI override
kvx run --config kvx.toml --sink-parallelism 16
```

## TOML Config Schema (v17)

```toml
[runtime]
queue_capacity = 8
sink_parallelism = 8

[source.File]
file_name = "input.json"

[sink.Elasticsearch]
url = "http://localhost:9200"
index = "test-123"

[throttle.source]
max_batch_size_bytes = 131072
max_batch_size_docs = 10000

[throttle.sink]
max_request_size_bytes = 131072
```

## Source Args

| Source | Required Args | Optional Args |
|--------|--------------|---------------|
| `file` | `--source-file-name` | `--source-max-batch-size-bytes`, `--source-max-batch-size-docs` |
| `elasticsearch` | `--source-url` | `--source-username`, `--source-password`, `--source-api-key` |
| `opensearch` | `--source-url` | `--source-username`, `--source-password`, `--source-index` |
| `s3-rally` | `--source-track`, `--source-bucket` | `--source-region` (default: us-east-1), `--source-key` |

## Sink Args

| Sink | Required Args | Optional Args |
|------|--------------|---------------|
| `file` | `--sink-file-name` | `--sink-max-request-size-bytes` |
| `elasticsearch` | `--sink-url` | `--sink-username`, `--sink-password`, `--sink-api-key`, `--sink-index` |
| `opensearch` | `--sink-url` | `--sink-username`, `--sink-password`, `--sink-index`, `--sink-danger-accept-invalid-certs` |

# Notes for future reference

- VS Code launch configs (`F5` / `Ctrl+F5`) target this binary via CodeLLDB
- 9 CLI tests covering parse, config build, error messages
- Error messages are micro-fiction, not stack traces
- `[throttle.source]` / `[throttle.sink]` are the canonical sizing config — never embed sizing in backend configs
- Backend configs (`[source.X]` / `[sink.X]`) are pure connection details: URL, credentials, index name

# Aggregated Context Memory Across Sessions for Current and Future Use

- v0: Placeholder main.rs with raw `std::env::args()` and TOML-only config
- v1: Full clap CLI rewrite. `list-flows` + `run` subcommands. Flat args with `--source-*`/`--sink-*` prefixes. TOML `--config` backwards compat. InMemory hidden. Claude Code skill. 9 CLI tests.
- v2 (6-phase refactor): Updated `AppConfig` field names (`source`/`sink` vs old `source_config`/`sink_config`). Throttle config moved to `[throttle.source]`/`[throttle.sink]` TOML sections. CLI throttle override args added. `kvx::stop()` wired to Ctrl+C for graceful shutdown via `CancellationToken`. 110 total tests (101 kvx + 9 kvx-cli).
- v3 (current — code review hardening): `Commands::Run(Box<RunArgs>)` to fix large_enum_variant (456 byte disparity). Blanket `#![allow]` removed. 126 total tests (117 kvx + 9 kvx-cli).
