# ai

# Summary
Shared Python utilities for Kravex demo and benchmark scripts.

# Description
The Switzerland of the Kravex repo. Common functions extracted from `demo/scripts/` and `benchmark/scripts/` to eliminate duplication. One library to rule them all.

# Knowledge Graph
- `kvx_utils.py` → used by `demo/scripts/*.py` + `benchmark/scripts/*.py`
- Index ops: `create_index`, `delete_index`, `get_doc_count`, `refresh_index`, `force_merge`, `reset_index`
- Binary discovery: `find_kvx_binary`, `build_kvx_binary`, `find_or_build_kvx_binary`
- Cluster health: `check_cluster_health` (polling with timeout)
- Metrics: `MetricsSampler` (background thread, CPU/RSS via `ps`)
- Result recording: `record_result` (JSON output for Jupyter notebook)
- URL helpers: `extract_host_port`, `engine_from_url`, `get_engine_url`
- Timing: `generate_run_id`

# Key Concepts
- **No rich/console dependency**: Shared utils use plain print/exceptions — callers add their own UI
- **macOS-first metrics**: `ps -p PID -o %cpu=,rss=` — RSS in KB, converted to MB
- **Idempotent index ops**: `reset_index` = delete + create, `create_index` handles settings
- **Benchmark-optimal defaults**: 1 shard, 0 replicas, refresh=-1 for max ingest throughput

# Notes for future reference
- Import pattern: `sys.path.insert(0, str(REPO_ROOT))` then `from shared.kvx_utils import ...`
- `_guess_project_root()` walks up from `shared/` to find `Cargo.toml`
- `MetricsSampler` uses daemon threads — no zombie cleanup needed

# Aggregated Context Memory Across Sessions for Current and Future Use
- Created 2026-03-03 during demo+benchmark infrastructure consolidation
- Extracted from: `demo/scripts/seed.py` (index ops), `demo/scripts/migrate.py` (binary finder), `benchmark/scripts/helpers/*.sh` (metrics, index ops)
- Eliminated: 7 bash scripts, 1 docker-compose, duplicated index ops code
