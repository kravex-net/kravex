#!/usr/bin/env python3
# ai
"""
🚀 Kravex Benchmark Suite — Run Benchmarks 🚀📊🧪
It was a dark and stormy deploy. The clusters were up. The data was downloaded.
And somewhere, deep in the benchmark suite, an orchestrator was about to do
something profoundly boring but technically necessary: run every tool against
every dataset and write the receipts.

Replaces: run_all.sh, run_kravex.sh, run_esrally.sh, run_elasticdump.sh
Uses shared/kvx_utils.py for index ops, metrics, and result recording.

Usage:
  python benchmark/scripts/run_benchmarks.py
  python benchmark/scripts/run_benchmarks.py --dataset=pmc
  python benchmark/scripts/run_benchmarks.py --engine=opensearch --skip-esrally
  python benchmark/scripts/run_benchmarks.py --help

🦆 The singularity will happen before we finish benchmarking noaa (33.6M docs).
"""

import argparse
import glob
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

# -- 🔧 Add repo root to path so we can import shared utils
SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
REPO_ROOT = BENCHMARK_DIR.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import (  # noqa: E402
    MetricsSampler,
    check_cluster_health,
    extract_host_port,
    find_or_build_kvx_binary,
    generate_run_id,
    get_doc_count,
    get_engine_url,
    record_result,
    refresh_index,
    reset_index,
)


# ============================================================================
#  📦 Configuration Constants
# ============================================================================

CONFIGS_DIR = BENCHMARK_DIR / "configs" / "local"
DATA_DIR = BENCHMARK_DIR / "data"
RESULTS_DIR = BENCHMARK_DIR / "results"
ESRALLY_TRACKS_DIR = BENCHMARK_DIR / "esrally_tracks"

# -- 🎯 Datasets and their expected doc counts. Official Rally track counts as of 2024.
DATASETS = {
    "geonames": 11_396_503,
    "noaa": 33_659_481,
    "pmc": 574_199,
}

ENGINES = ["elasticsearch", "opensearch"]


# ============================================================================
#  📢 Logging — consistent emoji-prefixed output
# ============================================================================

def log_info(msg: str):
    print(f"🚀 {msg}")


def log_warn(msg: str):
    print(f"⚠️  {msg}")


def log_error(msg: str):
    print(f"💀 {msg}", file=sys.stderr)


def log_section(title: str):
    """Print a section banner. Big. Bold. Theatrical."""
    print()
    print("═" * 60)
    print(f"  {title}")
    print("═" * 60)
    print()


# ============================================================================
#  🔧 CLI Argument Parsing
# ============================================================================

def parse_args() -> argparse.Namespace:
    """
    Parse CLI flags. getopts in Python is slightly less cursed than in bash.
    TODO: win the lottery, retire, rewrite this with clap. Not today.
    """
    parser = argparse.ArgumentParser(
        description="🚀 Kravex Benchmark Suite — run all benchmark tools against all datasets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python run_benchmarks.py --dataset=pmc\n"
            "  python run_benchmarks.py --engine=opensearch --skip-esrally\n"
            "  python run_benchmarks.py --skip-esrally --skip-elasticdump\n"
            "\n"
            "env overrides: ES_URL, OS_URL (defaults: localhost:9200, localhost:9201)\n"
            "\n"
            "Ancient proverb: 'He who benchmarks without a notebook, presents results in a terminal forever.'"
        ),
    )
    parser.add_argument("--dataset", help="Run only this dataset (geonames, noaa, pmc)")
    parser.add_argument("--engine", help="Run only this engine (elasticsearch, opensearch)")
    parser.add_argument("--skip-esrally", action="store_true", help="Skip esrally benchmark runs")
    parser.add_argument("--skip-elasticdump", action="store_true", help="Skip elasticdump benchmark runs")
    return parser.parse_args()


# ============================================================================
#  📡 Cluster Health Checks
# ============================================================================

def check_all_clusters(es_url: str, os_url: str, filter_engine: str = None):
    """
    Verify clusters are healthy before wasting benchmark time.
    We accept yellow — this is benchmarking, not production. Low standards, but standards.
    """
    log_section("Cluster Health Checks")

    engines_to_check = []
    if not filter_engine or filter_engine == "elasticsearch":
        engines_to_check.append(("Elasticsearch", es_url))
    if not filter_engine or filter_engine == "opensearch":
        engines_to_check.append(("OpenSearch", os_url))

    for label, url in engines_to_check:
        try:
            status = check_cluster_health(url, timeout=30)
            log_info(f"{label} health: {status} ✅ ({url})")
        except TimeoutError:
            log_error(
                f"{label} at {url} is not healthy — the cluster is in distress. "
                f"Like the dog. In the fire. 'This is fine.'"
            )
            log_error("Start clusters with: docker compose --profile bench up -d")
            sys.exit(1)

    log_info("All required clusters are healthy ✅")


def check_data_files(filter_dataset: str = None):
    """
    Verify NDJSON data files exist for datasets that need them.
    Geonames is skipped — it uses esrally-seeded ES index, not a file.
    """
    log_section("Data File Verification")

    # -- 🎯 Geonames uses esrally→ES→OS migration flow, no NDJSON file needed
    datasets_needing_files = {k: v for k, v in DATASETS.items() if k != "geonames"}

    all_present = True
    for dataset in datasets_needing_files:
        if filter_dataset and dataset != filter_dataset:
            continue
        ndjson_path = DATA_DIR / f"{dataset}.json"
        if ndjson_path.exists():
            log_info(f"Data file exists: {ndjson_path} ✅")
        else:
            log_error(f"Missing data file: {ndjson_path} — run: python benchmark/scripts/setup.py")
            all_present = False

    if filter_dataset == "geonames":
        log_info("Geonames uses esrally-seeded ES index — no NDJSON file needed ✅")

    if not all_present:
        log_error("Some data files are missing. Run: python benchmark/scripts/setup.py")
        sys.exit(1)


# ============================================================================
#  🚀 Tool Runners — kravex, esrally, elasticdump
# ============================================================================

def run_tool_with_metrics(
    cmd: list[str],
    tool_label: str,
    dataset: str,
    engine: str,
    expected_docs: int,
    cluster_url: str,
    index_name: str,
    log_prefix: str = "",
    refresh_after: bool = False,
) -> dict:
    """
    Generic tool runner: launch a subprocess, sample metrics, record result.
    Returns the result dict. Used by all three tool runners.

    Knowledge: All benchmark tools (kvx, esrally, elasticdump) follow the same
    pattern: start process → sample CPU/RSS → wait → count docs → record.
    This function consolidates that pattern.
    """
    prefix = f"[{log_prefix or tool_label}] "
    run_id = generate_run_id(tool_label, dataset, engine)

    log_info(f"{prefix}Launching: {' '.join(str(c) for c in cmd[:5])}...")
    log_info(f"{prefix}Expected docs: {expected_docs:,}")

    start_time = time.monotonic()

    # -- 🚀 Launch the process (cwd=REPO_ROOT so relative TOML paths resolve correctly)
    # 🧠 RUST_LOG must be set or tracing_subscriber's EnvFilter::from_default_env()
    # defaults to ERROR-only — silencing all info!/warn! output. Ghost process vibes.
    env = os.environ.copy()
    env.setdefault("RUST_LOG", "info")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=REPO_ROOT,
        env=env,
    )

    # -- 🧵 Start metrics sampling
    sampler = MetricsSampler(proc.pid, interval=0.5)
    sampler.start()

    # -- 🧵 Background thread drains stdout to prevent pipe buffer deadlock.
    # Without this, kravex blocks on write() once the OS pipe buffer fills (~64KB on macOS).
    # The child process goes to sleep. Docker shows 0% CPU. You stare into the void. The void stares back.
    stdout_lines = []

    def _drain_stdout():
        for line in iter(proc.stdout.readline, b""):
            decoded = line.decode("utf-8", errors="replace").rstrip()
            stdout_lines.append(decoded)
            print(f"  {decoded}")
        proc.stdout.close()

    reader_thread = threading.Thread(target=_drain_stdout, daemon=True)
    reader_thread.start()

    # -- ⏳ Poll doc count while the process runs — live progress instead of staring
    # into the void. Like watching a loading bar, but with numbers that mean something.
    POLL_INTERVAL = 15  # seconds — same cadence as migration_loop.py
    while proc.poll() is None:
        time.sleep(POLL_INTERVAL)
        if proc.poll() is not None:
            break
        elapsed = time.monotonic() - start_time
        docs = get_doc_count(cluster_url, index_name)
        if docs > 0:
            rate = f"{docs / elapsed * 60:,.0f}" if elapsed > 0 else "∞"
            log_info(f"{prefix}⏱️  {elapsed:.0f}s | {docs:,} docs | {rate} docs/min")

    proc.wait()
    reader_thread.join(timeout=5)
    stdout_data = "\n".join(stdout_lines).encode("utf-8")
    exit_code = proc.returncode

    # -- 🧵 Stop sampler and get stats
    metrics = sampler.stop()
    duration = time.monotonic() - start_time

    log_info(f"{prefix}Finished in {duration:.1f}s with exit_code={exit_code}")

    # -- 🔄 Some tools (elasticdump) don't flush before exiting
    if refresh_after:
        refresh_index(cluster_url, index_name)

    # -- 🎯 Count indexed docs
    indexed_docs = get_doc_count(cluster_url, index_name)
    if indexed_docs < 0:
        indexed_docs = 0

    log_info(f"{prefix}Indexed docs in '{index_name}': {indexed_docs:,} (expected: {expected_docs:,})")

    # -- 📝 Record result
    result_path = record_result(
        run_id=run_id,
        tool=tool_label,
        dataset=dataset,
        engine=engine,
        expected_docs=expected_docs,
        indexed_docs=indexed_docs,
        duration_sec=duration,
        exit_code=exit_code,
        metrics=metrics,
        output_dir=RESULTS_DIR,
    )
    log_info(f"{prefix}Result recorded: {result_path} ✅")

    # -- 📝 Save tool log
    log_path = Path(f"/tmp/{tool_label}_{run_id}.log")
    if stdout_data:
        log_path.write_bytes(stdout_data)

    if exit_code != 0:
        log_error(
            f"{prefix}Exited with code {exit_code} — "
            f"'Config not found: We looked everywhere. Under the couch. Behind the fridge. Nothing.'"
        )
        if stdout_data:
            # -- 💀 Show last 10 lines for debugging
            last_lines = stdout_data.decode("utf-8", errors="replace").strip().split("\n")[-10:]
            for line in last_lines:
                print(f"  {line}")

    return {
        "run_id": run_id,
        "tool": tool_label,
        "exit_code": exit_code,
        "duration": duration,
        "indexed_docs": indexed_docs,
    }


def run_kravex(
    config_path: Path,
    tool_label: str,
    dataset: str,
    engine: str,
    expected_docs: int,
    cluster_url: str,
    kvx_binary: Path,
):
    """Run kvx-cli with a TOML config. Resets the target index first."""
    index_name = f"benchmark_{dataset}"
    reset_index(cluster_url, index_name)

    return run_tool_with_metrics(
        cmd=[str(kvx_binary), "run", "--config", str(config_path)],
        tool_label=tool_label,
        dataset=dataset,
        engine=engine,
        expected_docs=expected_docs,
        cluster_url=cluster_url,
        index_name=index_name,
        log_prefix=f"run_kravex/{tool_label}",
    )


def run_esrally(
    dataset: str,
    target_url: str,
    expected_docs: int,
    engine: str,
):
    """
    Run esrally race with --pipeline=benchmark-only against a local track.
    esrally manages its own process; we wrap with metrics for fair comparison.
    """
    track_path = ESRALLY_TRACKS_DIR / dataset
    if not track_path.is_dir():
        log_error(
            f"esrally track not found at {track_path} — "
            f"'We looked everywhere. Under the couch. Behind the fridge. In the junk drawer. Nothing.'"
        )
        return None

    if not Path(subprocess.run(["which", "esrally"], capture_output=True, text=True).stdout.strip()).exists():
        log_error("esrally not found. Run setup.py or: pip3 install esrally")
        return None

    host_port = extract_host_port(target_url)
    index_name = dataset  # -- esrally uses dataset name as index by default

    reset_index(target_url, index_name)

    return run_tool_with_metrics(
        cmd=[
            "esrally", "race",
            "--pipeline=benchmark-only",
            f"--track-path={track_path}",
            f"--target-hosts={host_port}",
            "--on-error=abort",
            "--kill-running-processes",
        ],
        tool_label="esrally",
        dataset=dataset,
        engine=engine,
        expected_docs=expected_docs,
        cluster_url=target_url,
        index_name=index_name,
        log_prefix="run_esrally",
    )


def run_elasticdump(
    dataset: str,
    target_url: str,
    expected_docs: int,
    engine: str,
):
    """
    Run elasticdump with fixed settings: --limit=5000, --concurrency=4.
    These provide a reproducible baseline. The plumbing metaphor writes itself.
    """
    if not Path(subprocess.run(["which", "elasticdump"], capture_output=True, text=True).stdout.strip()).exists():
        log_error("elasticdump not found. Run: npm install -g elasticdump")
        return None

    input_file = DATA_DIR / f"{dataset}.json"
    if not input_file.exists():
        log_error(f"Input file not found: {input_file}")
        return None

    index_name = f"benchmark_{dataset}"
    full_target = f"{target_url}/{index_name}"

    reset_index(target_url, index_name)

    return run_tool_with_metrics(
        cmd=[
            "elasticdump",
            f"--input={input_file}",
            f"--output={full_target}",
            "--type=data",
            "--limit=5000",
            "--concurrency=4",
        ],
        tool_label="elasticdump",
        dataset=dataset,
        engine=engine,
        expected_docs=expected_docs,
        cluster_url=target_url,
        index_name=index_name,
        log_prefix="run_elasticdump",
        refresh_after=True,
    )


# ============================================================================
#  🔄 Orchestration — the nested loop of destiny
# ============================================================================

def get_config_path(dataset: str, engine: str, throttle_mode: str) -> Path:
    """
    Build the correct config filename based on engine.
    ES configs: {dataset}_{mode}.toml (e.g. geonames_static.toml)
    OS configs: {dataset}_opensearch_{mode}.toml (e.g. geonames_opensearch_static.toml)
    """
    if engine == "opensearch":
        return CONFIGS_DIR / f"{dataset}_opensearch_{throttle_mode}.toml"
    else:
        return CONFIGS_DIR / f"{dataset}_{throttle_mode}.toml"


def run_dataset_engine(
    dataset: str,
    engine: str,
    expected_docs: int,
    kvx_binary: Path,
    es_url: str,
    os_url: str,
    skip_esrally: bool,
    skip_elasticdump: bool,
):
    """
    Run all 3 tools for a single dataset × engine pair (file-based ingest).
    Used for noaa/pmc. Geonames uses run_geonames_migration() instead.
    'Knock knock. Who's there? Nested loop. Nested loop wh— O(n²) has entered the chat.'
    """
    cluster_url = get_engine_url(engine, es_url, os_url)

    log_section(f"{dataset} × {engine} (expected: {expected_docs:,} docs)")

    # -- 1. kravex-static (file → engine ingest)
    log_info("--- Tool 1/3: kravex-static ---")
    static_config = get_config_path(dataset, engine, "static")
    if static_config.exists():
        run_kravex(static_config, "kravex-static", dataset, engine, expected_docs, cluster_url, kvx_binary)
    else:
        log_warn(f"Config not found: {static_config} — skipping static run")

    # -- 2. esrally
    if not skip_esrally:
        log_info("--- Tool 2/3: esrally ---")
        run_esrally(dataset, cluster_url, expected_docs, engine)
    else:
        log_warn(f"Skipping esrally for {dataset}/{engine} (--skip-esrally)")

    # -- 3. elasticdump
    if not skip_elasticdump:
        log_info("--- Tool 3/3: elasticdump ---")
        run_elasticdump(dataset, cluster_url, expected_docs, engine)
    else:
        log_warn(f"Skipping elasticdump for {dataset}/{engine} (--skip-elasticdump)")

    log_info(f"Completed all tools for {dataset} × {engine} ✅")


def run_geonames_migration(
    expected_docs: int,
    kvx_binary: Path,
    es_url: str,
    os_url: str,
    skip_esrally: bool,
    skip_elasticdump: bool,
):
    """
    🔄 Geonames migration flow: esrally seeds ES, kravex migrates ES → OS.
    This demonstrates kravex's actual migration capability — not file ingest.

    The pitch: 'In a world where 11.4M docs must cross the search engine divide...
    one tool dared to migrate them. Without config. Without babysitting.'
    """
    log_section(f"geonames — ES → OS migration (expected: {expected_docs:,} docs)")

    # -- 🎯 Step 1: Check if ES already has geonames data (idempotent seeding)
    es_doc_count = get_doc_count(es_url, "geonames")

    if es_doc_count >= expected_docs:
        log_info(
            f"ES 'geonames' index already has {es_doc_count:,} docs "
            f"(expected: {expected_docs:,}) — skipping esrally seed ✅"
        )
    elif not skip_esrally:
        log_info("--- Step 1: Seed ES via esrally ---")
        log_info(f"ES 'geonames' has {max(es_doc_count, 0):,} docs — seeding required")
        seed_result = run_esrally("geonames", es_url, expected_docs, "elasticsearch")
        if seed_result and seed_result.get("exit_code") != 0:
            log_error(
                "esrally seed failed — cannot proceed with migration. "
                "The foundation crumbled. The house of cards collapsed. "
                "Like my fantasy football team in week 3."
            )
            return
    else:
        log_warn(
            f"ES 'geonames' has {max(es_doc_count, 0):,} docs but --skip-esrally is set. "
            f"Migration may produce incomplete results. You've been warned. 🎲"
        )

    # -- 🚀 Step 2: Migrate ES → OS via kravex
    log_info("--- Step 2: Migrate ES → OS via kravex-static ---")
    migration_config = CONFIGS_DIR / "geonames_es_to_os_static.toml"
    if not migration_config.exists():
        log_error(
            f"Migration config not found: {migration_config} — "
            f"'We looked everywhere. Under the couch. Behind the fridge. Nothing.'"
        )
        return

    # -- 🔄 Reset OS target index before migration
    reset_index(os_url, "benchmark_geonames")

    run_tool_with_metrics(
        cmd=[str(kvx_binary), "run", "--config", str(migration_config)],
        tool_label="kravex-migration",
        dataset="geonames",
        engine="opensearch",
        expected_docs=expected_docs,
        cluster_url=os_url,
        index_name="benchmark_geonames",
        log_prefix="run_geonames_migration/kravex",
    )

    # -- 📊 Step 3: Run comparison tools against ES (file-based) if requested
    if not skip_elasticdump:
        log_info("--- Step 3: elasticdump baseline (file → ES) ---")
        run_elasticdump("geonames", es_url, expected_docs, "elasticsearch")
    else:
        log_warn("Skipping elasticdump for geonames (--skip-elasticdump)")

    log_info("Completed geonames migration flow ✅")


# ============================================================================
#  📊 Summary Table — terminal-formatted results
# ============================================================================

def print_summary_table():
    """
    Print a summary table of all result JSON files.
    'Boomer tech confusion: this is like a spreadsheet but for the terminal.'
    """
    log_section("Benchmark Results Summary")

    result_files = sorted(glob.glob(str(RESULTS_DIR / "*.json")))
    if not result_files:
        log_warn("No result files found — did anything actually run?")
        return

    records = []
    for path in result_files:
        try:
            with open(path) as f:
                records.append(json.load(f))
        except Exception as e:
            log_warn(f"Could not parse {path}: {e}")

    if not records:
        return

    # -- 📊 Sort by dataset, engine, tool for readable ordering
    records.sort(key=lambda r: (r.get("dataset", ""), r.get("engine", ""), r.get("tool", "")))

    # -- 📊 Column formatting
    headers = ["tool", "dataset", "engine", "indexed_docs", "duration_sec", "docs/min", "peak_mem_mb", "exit"]

    def fmt_row(r):
        return [
            r.get("tool", "?"),
            r.get("dataset", "?"),
            r.get("engine", "?"),
            f"{r.get('indexed_docs', 0):,}",
            f"{r.get('duration_sec', 0):.1f}",
            f"{r.get('docs_per_min', 0):,.0f}",
            f"{r.get('peak_mem_mb', 0):.1f}",
            str(r.get("exit_code", "?")),
        ]

    rows = [fmt_row(r) for r in records]
    col_widths = [
        max(len(h), max((len(row[i]) for row in rows), default=0))
        for i, h in enumerate(headers)
    ]

    def pad(s, w):
        return str(s).ljust(w)

    sep = "─" * (sum(col_widths) + len(col_widths) * 3 + 1)
    print(sep)
    print("  " + " │ ".join(pad(h, col_widths[i]) for i, h in enumerate(headers)))
    print(sep)

    for r, row in zip(records, rows):
        exit_icon = "✅" if r.get("exit_code", 1) == 0 else "💀"
        print(f"  {' │ '.join(pad(v, col_widths[i]) for i, v in enumerate(row))} {exit_icon}")

    print(sep)
    print(f"\n  Total runs: {len(records)} | Results dir: {RESULTS_DIR}")


# ============================================================================
#  🚀 Main — parse args, check health, check data, run benchmarks, summarize
# ============================================================================

def main():
    args = parse_args()

    es_url = os.environ.get("ES_URL", "http://localhost:9200")
    os_url = os.environ.get("OS_URL", "http://localhost:9201")

    log_section("🚀 Kravex Benchmark Suite — run_benchmarks.py")
    log_info(f"BENCHMARK_DIR: {BENCHMARK_DIR}")
    log_info(f"REPO_ROOT: {REPO_ROOT}")
    log_info(f"RESULTS_DIR: {RESULTS_DIR}")
    log_info(f"ES_URL: {es_url}")
    log_info(f"OS_URL: {os_url}")

    if args.dataset:
        log_info(f"Filtering to dataset: {args.dataset}")
    if args.engine:
        log_info(f"Filtering to engine: {args.engine}")

    # -- 📡 Health checks
    check_all_clusters(es_url, os_url, args.engine)

    # -- 🎯 Data file checks
    check_data_files(args.dataset)

    # -- 🔧 Ensure results directory exists
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # -- 🔧 Find or build kvx-cli
    log_info("Locating kvx-cli binary...")
    kvx_binary = find_or_build_kvx_binary(REPO_ROOT)
    log_info(f"kvx-cli binary: {kvx_binary} ✅")

    # -- 🔄 Outer loop: datasets × engines
    for dataset, expected_docs in DATASETS.items():
        if args.dataset and dataset != args.dataset:
            log_info(f"Skipping dataset '{dataset}' (--dataset={args.dataset})")
            continue

        # -- 🔄 Geonames gets the migration flow (ES → OS), not file-based ingest
        if dataset == "geonames":
            if args.engine == "elasticsearch":
                log_warn(
                    "Geonames migration targets OpenSearch — skipping "
                    "(--engine=elasticsearch filters out the OS target)"
                )
                continue
            run_geonames_migration(
                expected_docs=expected_docs,
                kvx_binary=kvx_binary,
                es_url=es_url,
                os_url=os_url,
                skip_esrally=args.skip_esrally,
                skip_elasticdump=args.skip_elasticdump,
            )
            continue

        # -- 📦 noaa/pmc: file-based ingest across engines
        for engine in ENGINES:
            if args.engine and engine != args.engine:
                log_info(f"Skipping engine '{engine}' (--engine={args.engine})")
                continue

            run_dataset_engine(
                dataset=dataset,
                engine=engine,
                expected_docs=expected_docs,
                kvx_binary=kvx_binary,
                es_url=es_url,
                os_url=os_url,
                skip_esrally=args.skip_esrally,
                skip_elasticdump=args.skip_elasticdump,
            )

    # -- 📊 Summary
    print_summary_table()

    log_section("✅ All benchmark runs complete")
    log_info(f"Result files written to: {RESULTS_DIR}")
    log_info("To regenerate charts: open benchmark/notebook/kravex_benchmarks.ipynb")
    log_info("Ancient proverb: 'He who benchmarks without a notebook, presents results in a terminal forever.'")


if __name__ == "__main__":
    main()
