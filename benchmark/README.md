# ai
# Kravex Benchmark Suite

# Summary
Sales demo and benchmark harness comparing Kravex vs esrally vs elasticdump for search indexing throughput.

# Description
Three tools. Three datasets. One Jupyter notebook full of charts that make Kravex look good (because it is good).

**Datasets:** geonames (11M docs), noaa (33M docs), pmc (574K docs) — standard Elasticsearch Rally tracks.

**Engines:** Elasticsearch 8.15 + OpenSearch 2.19 via unified Docker Compose profiles, plus AWS OpenSearch managed configs.

**Charts produced:**
- Docs/min throughput (grouped bar)
- Total duration (horizontal bar)
- CPU/memory usage comparison
- PID vs Static throttle hero chart (the money shot)

# Knowledge Graph
- `benchmark/` → sales material, cold outreach demo
- Root `docker-compose.yml` → `--profile bench` for ES 8.15 + OS 2.19
- `shared/kvx_utils.py` → shared index ops, metrics, binary finder (used by demo + benchmark)
- `configs/local/` → 12 TOML configs (3 datasets × 2 throttle modes × 2 engines)
- `configs/aws/` → 6 TOML configs (3 datasets × 2 throttle modes, OpenSearch managed)
- `scripts/setup.py` → downloads Rally data from data.elastic.co, installs deps
- `scripts/run_benchmarks.py` → orchestrates all benchmark runs (replaces 7 bash scripts)
- `notebook/kravex_benchmarks.ipynb` → Jupyter notebook with plotly charts
- PID controller → BSL/EE licensed, adaptive throttle, the premium value prop

# Key Concepts
- **Fair comparison**: All tools load same NDJSON files into same freshly-reset index (1 shard, 0 replicas, refresh=-1)
- **Metrics**: Wall clock timing + MetricsSampler (CPU/RSS via `ps` at 500ms intervals)
- **PID value prop**: Adaptive batch sizing → no manual tuning, prevents 429 cascades, smooth throughput
- **Python-only**: All scripts are Python (bash eliminated), shared utils via `shared/kvx_utils.py`

# Quick Start
```bash
# 1. Start local clusters (bench profile = ES 8.15 + OS 2.19, 2GB heap)
OS_JAVA_OPTS="-Xms2g -Xmx2g" docker compose --profile bench up -d

# 2. Install deps and download data (~45GB total)
python benchmark/scripts/setup.py

# 3. Run all benchmarks (or filter: --dataset=pmc --skip-esrally)
python benchmark/scripts/run_benchmarks.py

# 4. Open notebook
jupyter notebook benchmark/notebook/kravex_benchmarks.ipynb
```

# Notes for future reference
- AWS configs need endpoint/password filled in before use
- esrally needs custom track configs in esrally_tracks/ (TODO)
- For PID time-series charts, kvx needs a `--metrics-file` flag (future Rust work)
- Data files are gitignored (multi-GB), must run setup.py first
- OpenSearch heap configurable via `OS_JAVA_OPTS` env var in docker-compose

# Aggregated Context Memory Across Sessions for Current and Future Use
- Benchmark suite created 2026-03-02 for sales cold outreach
- 2026-03-03: Consolidated bash→Python, shared infra with demo via `shared/kvx_utils.py`
- 7 bash scripts replaced by 2 Python scripts + 1 shared library
- Docker Compose unified at repo root with profiles (demo/bench)
- First target: demonstrate PID controller value to justify premium pricing
- Horizontal scaling orchestration deferred until first paid engagement
