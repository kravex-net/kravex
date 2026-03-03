# ai

# Summary
Kravex demo pipeline: self-contained ES 7.17 → OpenSearch 2.19 migration demo. 190k docs, 5 indices, zero config.

# Description
End-to-end sales demo showing Kravex migrating realistic HR/talent management data from Elasticsearch to OpenSearch. Runs locally via Docker Compose. Python scripts handle seeding, analysis, migration (via kvx-cli subprocess), and validation. Uses `shared/kvx_utils.py` for common index ops and binary discovery.

## Quick Start
```bash
pip install -r demo/requirements.txt
python demo/scripts/run_demo.py    # runs everything (starts docker, seeds, migrates, validates)
docker compose --profile demo down -v   # teardown
```

## Manual Steps
```bash
docker compose --profile demo up -d       # start ES 7.17 + OS 2.19
python demo/scripts/seed.py               # seed 190k docs into ES
python demo/scripts/analyze.py            # compatibility analysis
python demo/scripts/migrate.py            # run kvx-cli per index
python demo/scripts/validate.py           # verify migration
docker compose --profile demo down -v     # teardown
```

## Indices
| Index | Docs | Description |
|-------|------|-------------|
| employees | 10,000 | Employee profiles with nested location, skills array |
| learning_records | 50,000 | LMS completion records |
| performance_reviews | 25,000 | Review cycles with ratings |
| job_postings | 5,000 | Internal job board |
| search_analytics | 100,000 | Search query telemetry |

# Knowledge Graph
- `run_demo.py` → orchestrates → `seed.py` → `analyze.py` → `migrate.py` → `validate.py`
- `migrate.py` → subprocess → `kvx-cli run --config <toml>` (uses `shared.find_or_build_kvx_binary`)
- `seed.py` → REST API → Elasticsearch (bulk load) + OpenSearch (empty index creation)
- `validate.py` → REST API → both clusters (count, mapping, sample, match_all)
- Root `docker-compose.yml` → `--profile demo` for ES 7.17.25 (:9200) + OS 2.19.0 (:9201)
- `shared/kvx_utils.py` → shared index ops, cluster health, binary finder
- All configs in `configs/*.toml` follow `kvx.toml` pattern from repo root

# Key Concepts
- **Zero external deps**: Data generation is pure Python (no faker), deterministic via md5 hashing
- **Security disabled**: Both clusters run plain HTTP — `plugins.security.disabled=true` for OS
- **512MB JVM**: Lighter than benchmark's 2GB — sufficient for demo volumes
- **Idempotent seeding**: `seed.py` deletes + recreates indices on each run
- **Deterministic validation**: Sample docs fetched by `_id` sort for reproducible comparison
- **Shared utils**: `shared/kvx_utils.py` provides index ops, binary finder, cluster health

# Notes for future reference
- ES image: `docker.elastic.co/elasticsearch/elasticsearch:7.17.25`
- OS image: `opensearchproject/opensearch:2.19.0`
- Env vars: `ES_URL`, `OS_URL` override defaults (localhost:9200/9201)
- `migrate.py` auto-builds kvx-cli if no binary found in target/
- Bulk batch size: 1000 docs per `_bulk` request in seed.py

# Aggregated Context Memory Across Sessions for Current and Future Use
- Created 2026-03-02 in worktree `demo-pipeline`
- 2026-03-03: Consolidated infra — docker-compose moved to repo root with profiles, shared utils extracted
- TOML config fields: `[runtime]`, `[source.Elasticsearch]`, `[sink.OpenSearch]`, `[throttle.source]`, `[throttle.sink]`
- No changes to existing crates — kvx-cli invoked as subprocess only
