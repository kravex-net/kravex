# ai
# Benchmark Suite — Improvements & Findings

# Summary
Issues discovered and manual steps needed during the first full benchmark run (2026-03-03).
Goal: make `setup.py` and `run_benchmarks.py` fully self-contained so a single `./run_all.sh` reproduces everything.

# Bugs Fixed This Session

## Bug 1: Config directory path (run_benchmarks.py)
- `CONFIGS_DIR` pointed to `benchmark/configs` but local configs live in `benchmark/configs/local/`
- Fix: Changed to `BENCHMARK_DIR / "configs" / "local"`

## Bug 2: Elasticsearch config naming convention (run_benchmarks.py)
- Code built filenames as `{dataset}_elasticsearch_static.toml` but actual files are `{dataset}_static.toml` (no engine prefix for ES)
- Fix: Added `get_config_path()` helper that handles the naming asymmetry

## Bug 3: Data file extension mismatch (all 12 TOML configs)
- TOML configs referenced `.ndjson` but setup.py downloads files as `.json`
- Fix: Updated all 12 configs in `configs/local/` from `.ndjson` to `.json`

## Bug 4: Index name mismatch (run_benchmarks.py)
- Script used `{dataset}_benchmark` but TOML configs use `benchmark_{dataset}`
- Fix: Changed `run_kravex()` and `run_elasticdump()` to use `f"benchmark_{dataset}"`

## Bug 5: Download URLs dead (setup.py)
- `data.elastic.co` domain does not resolve (DNS failure)
- Correct base URL: `rally-tracks.elastic.co`
- Geonames uses `documents-2.json.bz2` (not `documents.json.bz2`)
- Fix: Updated all 3 URLs in setup.py DATASETS dict

## Bug 6: ES/OS Bulk URL missing index path (kvx Rust crate)
- `ElasticsearchSink` and `OpenSearchSink` posted bulk requests to `{url}/_bulk` without the target index
- The `RallyS3ToEs` transform generates `{"index":{}}` (no `_index` field in action line)
- ES 8.x requires index in either the action line OR the URL path → 400 Bad Request for every doc
- Symptom: kvx process hangs at 0% CPU, 0 docs indexed (the error is returned but retried forever on the large dataset)
- Fix: Changed both sinks to use `{url}/{index}/_bulk` when index is configured
- Files: `elasticsearch_sink.rs`, `opensearch_sink.rs`
- Added 4 new tests, all 121 tests pass

## Bug 7: PID throttle TOML format wrong (all 6 PID configs)
- `[throttle.sink]` had `mode = "Pid"` as a flat string, but `ThrottleConfig` is `#[serde(tag = "mode")]` internally tagged enum
- Serde expects a sub-table `[throttle.sink.mode]` with `mode = "Pid"` and the PID fields inside it
- Fix: Moved PID fields into `[throttle.sink.mode]` sub-table in all 6 local PID configs
- AWS PID configs have same bug (not fixed, out of scope for local benchmarks)

## Bug 8: esrally track configs missing
- `benchmark/esrally_tracks/{geonames,noaa,pmc}/` directories existed but were empty
- esrally needs `track.json` + `index.json` + data file per track
- Fix: Created track.json, index.json, and symlinks for all 3 datasets

# Manual Steps Performed (To Automate)

1. `OS_JAVA_OPTS="-Xms2g -Xmx2g" docker compose --profile bench up -d` — start clusters
2. `pip3 install esrally` — install esrally (setup.py attempts this but may fail silently)
3. `npm install -g elasticdump` — install elasticdump (same)
4. `cargo build --release -p kvx-cli` — build kvx binary (setup.py does this via find_or_build)
5. Download + decompress data files separately (setup.py handles this, but URLs were wrong)
6. Create esrally track configs (track.json, index.json, symlinks) — setup.py does NOT do this
7. Verify clusters healthy before running benchmarks

# Improvements Needed

## P0 — Script Won't Run Without These
- [ ] `setup.py` should create esrally track configs automatically (track.json, index.json, symlinks)
- [ ] `setup.py` should verify/create symlinks in esrally_tracks dirs
- [ ] Download URLs must stay current — add fallback URL logic or version pinning

## P1 — Reliability
- [ ] Add a `--dataset` flag to setup.py so you can download just one dataset for quick iteration
- [ ] `run_benchmarks.py` should handle esrally not installed gracefully (currently checks `which` then tries to construct Path from empty string)
- [ ] Decompression step should show progress (multi-GB bz2 decompress takes minutes, no output)
- [ ] Consider parallel downloads in setup.py (currently sequential, 3 files totaling ~7GB)
- [ ] esrally produces no stdout during run — add a progress monitor polling ES doc count (like kvx benchmarks)
- [ ] esrally first run on a fresh install takes extra time for internal setup (~60s before indexing starts)
- [ ] elasticdump OOMs on large files (>2GB) — document this or add `--fileSize` limit warning

## P2 — Developer Experience
- [ ] Create `run_all.sh` master script that chains: docker up → setup → benchmarks → notebook
- [ ] Add `--quick` mode: only run pmc (smallest? actually geonames is smallest) for fast iteration
- [ ] Add elapsed time to setup.py download progress
- [ ] Document heap requirements: 2GB ES + 2GB OS = 4GB just for clusters + data decompression

## P3 — Data Quality
- [ ] geonames Rally track uses `documents-2.json.bz2` (v2) — document in README
- [ ] PMC is 23GB uncompressed — warn user about disk space requirements
- [ ] Total disk: ~38GB for all datasets (compressed + uncompressed) — document this

# Findings & Observations

## Data Sizes (Actual)
| Dataset | Compressed | Uncompressed | Docs |
|---------|-----------|--------------|------|
| geonames | 253 MB | 3.3 GB | 11,396,503 |
| noaa | 995 MB | 9.0 GB | 33,659,481 |
| pmc | 5.9 GB | 21.7 GB | 574,199 |

## Infrastructure
- ES 8.15.0 + OS 2.19.0 via Docker Compose bench profile
- Both clusters healthy within ~30s of docker compose up
- kvx-cli release build: ~90s on Apple Silicon

# Benchmark Results — geonames × elasticsearch (11.4M docs, 3.3GB)

| Tool | Docs Indexed | Duration | Docs/Min | Peak Mem | Exit | Notes |
|------|-------------|----------|----------|----------|------|-------|
| kravex-static | 11,396,503 | 479s | 1,428,391 | 135 MB | 0 | ✅ Perfect — all docs indexed |
| kravex-pid | 19,324,678 | 634s | 1,830,344 | 148 MB | 0 | ⚠️ Indexed 70% more docs than exist — PID duplicate bug |
| elasticdump | 901,003 | 302s | 178,872 | 2,700 MB | -6 | 💀 OOM crash (SIGABRT) — can't handle 3.3GB file |
| esrally | 0 | 60s | 0 | 59 MB | 64 | 💀 Track config issue — needs investigation |

## Key Takeaways So Far
- **kravex-static is 8x faster than elasticdump** (before elasticdump crashed) at 1/20th the memory
- **kravex PID controller has a duplicate document bug** — indexed 19.3M docs from an 11.4M doc file
  - Throughput number (1.83M/min) is inflated; can't trust PID results until bug is fixed
  - Bug is in the Rust crate, not the benchmark scripts
- **elasticdump can't handle multi-GB files** — it loads the entire file into memory
  - Crashed at 901K docs / 2.7GB RAM, confirming the memory hog reputation
- **esrally track config needs fixing** — exit code 64, likely the custom track format is wrong
  - TODO: run esrally manually with `--laps=1` and debug the track.json

## Observations on kvx Runtime Behavior
- First ~30s shows 0 docs because SinkWorker buffers to 50MB before first flush
  - This is the `max_request_size_bytes = 52428800` setting
  - After first flush, steady ~1.5M docs/min throughput
- Memory usage is very lean: 135MB peak for 11.4M docs (vs 2.7GB for elasticdump's 901K)
- CPU averaging 7% with peaks to 86% — efficient, not CPU-bound

## Remaining Benchmarks To Run
- [ ] geonames × opensearch (kravex-static, kravex-pid, elasticdump, esrally)
- [ ] noaa × elasticsearch (33.6M docs — will take ~20-30 min per tool)
- [ ] noaa × opensearch
- [ ] pmc × elasticsearch (574K docs — quick, but 22GB file)
- [ ] pmc × opensearch

## Kvx Crate Bugs Found During Benchmarking
1. **Bulk URL missing index** (Bug 6 above) — Fixed
2. **PID controller duplicate documents** — NOT FIXED, needs Rust investigation
   - Hypothesis: PID controller's adaptive batch sizing may cause the FileSource to re-read pages
   - Or the SinkWorker retry logic resends successful batches
   - Impact: PID benchmark numbers are unreliable until this is resolved
