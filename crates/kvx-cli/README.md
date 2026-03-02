# Summary

CLI interface for kravex — run migrations from the command line with clap-powered arg parsing.

# Description

`kvx-cli` wraps the `kvx` core library and exposes it as a terminal tool. Supports both TOML config files and flat CLI arguments. Includes `list-flows` for discoverability and a Claude Code skill (`/kvx`) for interactive guided setup.

# Knowledge Graph

- **Workspace member**: `crates/kvx-cli`
- **Dependencies**: `kvx` (path = `../kvx`), `clap` (derive-based), `comfy-table`
- **Edition**: 2024
- **Binary crate**
- **Subcommands**: `run`, `list-flows`
- **Config resolution**: CLI args override TOML values; either or both accepted
- **Source of truth**: `kvx::transforms::supported_flows()` → drives `list-flows` output
- **Drift detection**: test in transforms.rs ensures `supported_flows()` ↔ `from_configs()` parity

# Key Concepts

- `list-flows` — prints valid source→sink combos using `supported_flows()`
- `run` — builds `AppConfig` from `--source`/`--sink` flat args or `--config` TOML
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

# Run s3-rally → file (download benchmark data)
kvx run --source s3-rally --source-track geonames --source-bucket rally-tracks-bench \
  --sink file --sink-file-name geonames.json

# Run with TOML config (backwards compatible)
kvx run --config kvx.toml

# TOML base + CLI override
kvx run --config kvx.toml --sink-parallelism 16
```

## Source Args

| Source | Required Args | Optional Args |
|--------|--------------|---------------|
| `file` | `--source-file-name` | batch size overrides |
| `elasticsearch` | `--source-url` | `--source-username`, `--source-password`, `--source-api-key` |
| `s3-rally` | `--source-track`, `--source-bucket` | `--source-region` (default: us-east-1), `--source-key` |

## Sink Args

| Sink | Required Args | Optional Args |
|------|--------------|---------------|
| `file` | `--sink-file-name` | request size override |
| `elasticsearch` | `--sink-url` | `--sink-username`, `--sink-password`, `--sink-api-key`, `--sink-index` |

# Notes for future reference

- VS Code launch configs (`F5` / `Ctrl+F5`) target this binary via CodeLLDB
- 9 CLI tests covering parse, config build, error messages
- Error messages are micro-fiction, not stack traces

# Aggregated Context Memory Across Sessions for Current and Future Use

- v0: Placeholder main.rs with raw `std::env::args()` and TOML-only config
- v1 (current): Full clap CLI rewrite. `list-flows` + `run` subcommands. Flat args with `--source-*`/`--sink-*` prefixes. TOML `--config` backwards compat. InMemory hidden. Claude Code skill at `.claude/commands/kvx.md`. Drift-detection test ensures supported_flows() ↔ from_configs() parity. 9 CLI tests + 45 kvx tests passing.
