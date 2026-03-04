# ai
"""
🚀 Migration Loop Benchmark — One script, three tools, zero notebooks.

The pitch: esrally seeds ES, kravex migrates ES→OS, elasticdump migrates OS→ES.
Then we check the doc counts and print a table. That's it. That's the whole thing.

"In a world where search indices must migrate... one script dared to benchmark them all."
"""

import subprocess
import sys
import time
import shutil
from pathlib import Path

# 🦆 — mandatory duck, no contextual sense whatsoever

# ── paths ────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
REPO_ROOT = BENCHMARK_DIR.parent
CONFIGS_DIR = BENCHMARK_DIR / "configs" / "local"
ESRALLY_TRACK = BENCHMARK_DIR / "esrally_tracks" / "geonames"

sys.path.insert(0, str(REPO_ROOT / "shared"))
from kvx_utils import (
    check_cluster_health,
    reset_index,
    get_doc_count,
    refresh_index,
    MetricsSampler,
    find_or_build_kvx_binary,
    extract_host_port,
)

# ── hardcoded constants (no config matrix, no env vars, just vibes) ──────
ES_URL = "http://localhost:9200"
OS_URL = "http://localhost:9201"
EXPECTED_DOCS = 11_396_503
DATASET = "geonames"
POLL_INTERVAL_SEC = 15

# ── formatting helpers ───────────────────────────────────────────────────

def _banner(text: str):
    """Print a double-line box because we have standards."""
    width = 57
    print(f"\n{'═' * width}")
    print(f"  {text}")
    print(f"{'═' * width}\n")


def _fmt_docs(n: int) -> str:
    return f"{n:,}"


def _fmt_time(secs: float) -> str:
    return f"{secs:.0f}s"


def _fmt_rate(docs: int, secs: float) -> str:
    if secs <= 0:
        return "∞"
    return f"{docs / secs * 60:,.0f}"


def _fmt_mem(mb: float) -> str:
    return f"{mb:.0f} MB"


def _print_progress(label: str, elapsed: float, docs: int):
    """Live progress line — overwrites previous line."""
    rate = _fmt_rate(docs, elapsed)
    print(f"  ⏱️  {_fmt_time(elapsed)} | {_fmt_docs(docs)} docs | {rate} docs/min")


def _print_result(docs: int, secs: float, peak_mem: float):
    rate = _fmt_rate(docs, secs)
    print(f"  ✅ {_fmt_docs(docs)} docs in {_fmt_time(secs)} = {rate} docs/min | {_fmt_mem(peak_mem)} peak\n")


# ── progress monitor ────────────────────────────────────────────────────

def _poll_progress(label: str, proc: subprocess.Popen, cluster_url: str,
                   index_name: str, start_time: float):
    """Poll doc count every POLL_INTERVAL_SEC while process runs.

    Returns (stdout_bytes, returncode) — same contract as proc.communicate().
    """
    stdout_chunks = []
    while proc.poll() is None:
        time.sleep(POLL_INTERVAL_SEC)
        if proc.poll() is not None:
            break
        elapsed = time.time() - start_time
        docs = get_doc_count(cluster_url, index_name)
        if docs > 0:
            _print_progress(label, elapsed, docs)

    remaining_out, _ = proc.communicate()
    if remaining_out:
        stdout_chunks.append(remaining_out)
    return b"".join(stdout_chunks), proc.returncode


# ── leg runners ──────────────────────────────────────────────────────────

def run_esrally() -> dict:
    """Leg 1: esrally seeds geonames into Elasticsearch from the Rally track.

    "You shall not pass... without bulk indexing 11M docs first." — Gandalf, probably
    """
    print("Leg 1: esrally → Elasticsearch (seed)")
    reset_index(ES_URL, DATASET, shards=1, replicas=0)

    host_port = extract_host_port(ES_URL)
    cmd = [
        "esrally", "race",
        "--pipeline=benchmark-only",
        f"--track-path={ESRALLY_TRACK}",
        f"--target-hosts={host_port}",
        "--on-error=abort",
        "--kill-running-processes",
    ]

    start = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sampler = MetricsSampler(proc.pid)
    sampler.start()

    _poll_progress("esrally", proc, ES_URL, DATASET, start)

    duration = time.time() - start
    metrics = sampler.stop()

    refresh_index(ES_URL, DATASET)
    docs = get_doc_count(ES_URL, DATASET)

    _print_result(docs, duration, metrics["peak_mem_mb"])

    return {
        "tool": "esrally",
        "direction": "file→ES",
        "docs": docs,
        "duration": duration,
        "peak_mem": metrics["peak_mem_mb"],
        "exit_code": proc.returncode,
    }


def run_kravex(kvx_binary: Path) -> dict:
    """Leg 2: kravex migrates geonames from Elasticsearch → OpenSearch.

    "I'm gonna make him an offer he can't refuse" — The Godfather,
    but about sub-second bulk request latency.
    """
    print("Leg 2: kravex — Elasticsearch → OpenSearch (migrate)")
    os_index = "benchmark_geonames"
    reset_index(OS_URL, os_index, shards=1, replicas=0)

    config_path = CONFIGS_DIR / "geonames_es_to_os_static.toml"
    cmd = [str(kvx_binary), "run", "--config", str(config_path)]

    start = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sampler = MetricsSampler(proc.pid)
    sampler.start()

    _poll_progress("kravex", proc, OS_URL, os_index, start)

    duration = time.time() - start
    metrics = sampler.stop()

    refresh_index(OS_URL, os_index)
    docs = get_doc_count(OS_URL, os_index)

    _print_result(docs, duration, metrics["peak_mem_mb"])

    return {
        "tool": "kravex",
        "direction": "ES→OS",
        "docs": docs,
        "duration": duration,
        "peak_mem": metrics["peak_mem_mb"],
        "exit_code": proc.returncode,
    }


def run_elasticdump() -> dict:
    """Leg 3: elasticdump migrates geonames from OpenSearch → Elasticsearch.

    "Here's looking at you, kid" — Casablanca, but it's elasticdump
    looking at 11M docs and sweating profusely.
    """
    print("Leg 3: elasticdump — OpenSearch → Elasticsearch (migrate back)")
    es_index = "benchmark_geonames"
    os_index = "benchmark_geonames"
    reset_index(ES_URL, es_index, shards=1, replicas=0)

    cmd = [
        "elasticdump",
        f"--input=http://localhost:9201/{os_index}",
        f"--output=http://localhost:9200/{es_index}",
        "--type=data",
        "--limit=5000",
        "--concurrency=4",
    ]

    start = time.time()
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    sampler = MetricsSampler(proc.pid)
    sampler.start()

    _poll_progress("elasticdump", proc, ES_URL, es_index, start)

    duration = time.time() - start
    metrics = sampler.stop()

    refresh_index(ES_URL, es_index)
    docs = get_doc_count(ES_URL, es_index)

    _print_result(docs, duration, metrics["peak_mem_mb"])

    return {
        "tool": "elasticdump",
        "direction": "OS→ES",
        "docs": docs,
        "duration": duration,
        "peak_mem": metrics["peak_mem_mb"],
        "exit_code": proc.returncode,
    }


# ── preflight ────────────────────────────────────────────────────────────

def preflight() -> Path:
    """Check everything is ready before we start burning CPU cycles.

    Returns the kvx binary path. Exits on any failure because life is too
    short to debug missing prerequisites at minute 45 of a benchmark run.
    """
    print("Preflight checks...")
    failures = []

    # Docker clusters
    for label, url in [("Elasticsearch", ES_URL), ("OpenSearch", OS_URL)]:
        try:
            status = check_cluster_health(url, timeout=10)
            print(f"  ✅ {label} ({url}) — {status}")
        except Exception as e:
            print(f"  💀 {label} ({url}) — {e}")
            failures.append(label)

    # kvx binary
    try:
        kvx_binary = find_or_build_kvx_binary(REPO_ROOT)
        print(f"  ✅ kvx-cli — {kvx_binary}")
    except Exception as e:
        print(f"  💀 kvx-cli — {e}")
        failures.append("kvx-cli")
        kvx_binary = None

    # esrally
    if shutil.which("esrally"):
        print(f"  ✅ esrally — {shutil.which('esrally')}")
    else:
        print("  💀 esrally — not found in PATH")
        failures.append("esrally")

    # elasticdump
    if shutil.which("elasticdump"):
        print(f"  ✅ elasticdump — {shutil.which('elasticdump')}")
    else:
        print("  💀 elasticdump — not found in PATH")
        failures.append("elasticdump")

    # esrally track
    track_json = ESRALLY_TRACK / "track.json"
    if track_json.exists():
        print(f"  ✅ geonames track — {track_json}")
    else:
        print(f"  💀 geonames track — {track_json} not found")
        failures.append("geonames track")

    if failures:
        print(f"\n💀 Preflight failed: {', '.join(failures)}")
        print("Fix the issues above and try again. The benchmarks will wait.")
        sys.exit(1)

    print()
    return kvx_binary


# ── summary ──────────────────────────────────────────────────────────────

def print_summary(results: list[dict]):
    """Print the final summary table and doc integrity check.

    "After all, tomorrow is another day" — but today we print numbers.
    """
    _banner(f"Summary")

    # Header
    print(f"  {'Tool':<15} {'Direction':<12} {'Docs':<14} {'Time':<8} {'Docs/Min':<12} {'Mem':<8}")

    for r in results:
        rate = _fmt_rate(r["docs"], r["duration"])
        print(
            f"  {r['tool']:<15} {r['direction']:<12} "
            f"{_fmt_docs(r['docs']):<14} {_fmt_time(r['duration']):<8} "
            f"{rate:<12} {_fmt_mem(r['peak_mem']):<8}"
        )

    # Doc integrity check
    print()
    es_geonames = get_doc_count(ES_URL, DATASET)
    es_benchmark = get_doc_count(ES_URL, "benchmark_geonames")
    os_benchmark = get_doc_count(OS_URL, "benchmark_geonames")

    # After the loop: ES:geonames has esrally data, OS:benchmark_geonames has kravex data,
    # ES:benchmark_geonames has elasticdump data
    all_match = (es_geonames == EXPECTED_DOCS and
                 os_benchmark == EXPECTED_DOCS and
                 es_benchmark == EXPECTED_DOCS)

    status = "✅" if all_match else "💀"
    print(f"  Doc integrity: "
          f"ES:geonames={_fmt_docs(es_geonames)} "
          f"OS:benchmark={_fmt_docs(os_benchmark)} "
          f"ES:benchmark={_fmt_docs(es_benchmark)} "
          f"{status}")

    if not all_match:
        print(f"  ⚠️  Expected {_fmt_docs(EXPECTED_DOCS)} everywhere")

    print()


# ── main ─────────────────────────────────────────────────────────────────

def main():
    _banner(f"Migration Loop Benchmark — {DATASET} ({_fmt_docs(EXPECTED_DOCS)} docs)")

    kvx_binary = preflight()

    results = []

    # Leg 1: esrally seeds ES from Rally track
    results.append(run_esrally())

    # Leg 2: kravex migrates ES → OS
    results.append(run_kravex(kvx_binary))

    # Leg 3: elasticdump migrates OS → ES
    results.append(run_elasticdump())

    print_summary(results)

    # Exit with failure if any leg had a non-zero exit code
    bad_legs = [r for r in results if r["exit_code"] != 0]
    if bad_legs:
        print(f"💀 {len(bad_legs)} leg(s) exited non-zero. Check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
