#!/usr/bin/env python3
# ai
"""
🔧 Kravex Shared Utilities — kvx_utils.py 🔧🚀📦
It was 3am. The demo scripts and benchmark scripts both needed index ops.
They both needed to find the binary. They both needed cluster health checks.
And yet, they lived apart — duplicating code like estranged twins.
This module reunites them. Group therapy for Python scripts. 🦆

The singularity will happen before we stop adding functions to this file.
"""

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests


# ============================================================================
#  📡 Cluster Health — "Are you alive? Blink twice if the JVM is eating you."
# ============================================================================

def check_cluster_health(url: str, timeout: int = 120) -> str:
    """
    Wait for a cluster to reach green/yellow health.
    Returns the health status string, or raises if timeout.

    Rationale: Both demo and benchmark need to wait for clusters
    post-docker-compose. This consolidates the polling loop.
    ES/OS clusters can take 30-60s to initialize on cold start,
    hence the generous default timeout.
    """
    # -- 💤 Poll every 2s, up to timeout. Like waiting for your Rust project to compile.
    waited = 0
    while waited < timeout:
        try:
            resp = requests.get(f"{url}/_cluster/health", timeout=5)
            if resp.status_code == 200:
                status = resp.json().get("status", "red")
                if status != "red":
                    return status
        except Exception:
            pass
        time.sleep(2)
        waited += 2

    raise TimeoutError(
        f"💀 Cluster at {url} not healthy after {timeout}s. "
        f"We waited. And waited. Like a dog at the window. But the cluster never came home."
    )


# ============================================================================
#  📦 Index Operations — CRUD for indices, benchmark-style
# ============================================================================

def create_index(
    url: str,
    name: str,
    mapping: Optional[dict] = None,
    shards: int = 1,
    replicas: int = 0,
    refresh: str = "-1",
) -> bool:
    """
    Create an index with optional mapping and benchmark-optimal settings.
    Returns True on success, False on failure.

    Knowledge: shards=1, replicas=0, refresh=-1 maximizes ingest throughput
    for single-node benchmarks. Refresh is disabled because ES refreshes
    every 1s by default, which wastes IOPS during bulk ingest.
    """
    body = mapping or {}
    if "settings" not in body:
        body["settings"] = {}
    body["settings"]["number_of_shards"] = shards
    body["settings"]["number_of_replicas"] = replicas
    body["settings"]["refresh_interval"] = refresh

    # -- 🚀 PUT the index. If it already exists, ES returns 400. We don't judge.
    resp = requests.put(
        f"{url}/{name}",
        json=body,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    return resp.status_code in (200, 201)


def delete_index(url: str, name: str) -> bool:
    """
    Delete an index. Returns True if deleted or didn't exist (idempotent).
    The emotional equivalent of 'unsubscribe from all'.
    """
    resp = requests.delete(f"{url}/{name}", timeout=10)
    # -- 🗑️ 200 = deleted, 404 = already gone. Both are fine. Like letting go.
    return resp.status_code in (200, 404)


def get_doc_count(url: str, name: str) -> int:
    """
    Refresh + count. The eternal dance of eventual consistency.

    Tribal knowledge: ES/OS are eventually consistent. Without a refresh,
    recently indexed docs may not appear in _count. We always refresh first
    to get an accurate count, even though it adds ~100ms of latency.
    """
    try:
        requests.post(f"{url}/{name}/_refresh", timeout=10)
        resp = requests.get(f"{url}/{name}/_count", timeout=10)
        if resp.status_code == 200:
            return resp.json().get("count", 0)
    except Exception:
        pass
    return -1


def refresh_index(url: str, name: str) -> bool:
    """
    POST /_refresh — makes newly indexed docs searchable.
    Returns True on success. Like waking documents from their indexing slumber.
    """
    try:
        resp = requests.post(f"{url}/{name}/_refresh", timeout=10)
        return resp.status_code == 200
    except Exception:
        return False


def force_merge(url: str, name: str, max_segments: int = 1) -> bool:
    """
    POST /_forcemerge — collapse segments for optimal read performance.
    Heavy operation. Do not run mid-ingest unless you enjoy suffering.
    """
    try:
        resp = requests.post(
            f"{url}/{name}/_forcemerge?max_num_segments={max_segments}",
            timeout=300,
        )
        return resp.status_code == 200
    except Exception:
        return False


def reset_index(
    url: str,
    name: str,
    shards: int = 1,
    replicas: int = 0,
    refresh: str = "-1",
) -> bool:
    """
    Delete + recreate with benchmark-optimal settings.
    The scorched earth + rebuild approach. Therapy would call this 'healthy detachment'.
    """
    delete_index(url, name)
    return create_index(url, name, shards=shards, replicas=replicas, refresh=refresh)


# ============================================================================
#  🔧 Binary Discovery — "Where is kvx-cli? Have you tried looking in target/?"
# ============================================================================

def find_kvx_binary(project_root: Optional[Path] = None) -> Optional[Path]:
    """
    Locate an existing kvx-cli binary. Returns Path or None.

    Search order: release first (faster binary), then debug.
    Does NOT build — use build_kvx_binary() for that.
    """
    root = project_root or _guess_project_root()
    # -- 🔍 Check release first because release builds are what we benchmark with
    release_path = root / "target" / "release" / "kvx-cli"
    if release_path.exists():
        return release_path

    debug_path = root / "target" / "debug" / "kvx-cli"
    if debug_path.exists():
        return debug_path

    return None


def build_kvx_binary(project_root: Optional[Path] = None) -> Path:
    """
    Build kvx-cli in release mode. Returns path to binary.
    Raises RuntimeError if build fails.

    'cargo build --release: the sound of the future being compiled'
    """
    root = project_root or _guess_project_root()
    result = subprocess.run(
        ["cargo", "build", "--release", "-p", "kvx-cli"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"💀 Build failed. Cargo said: {result.stderr[:500]}\n"
            f"Against all odds, cargo did NOT succeed. The engineers wept."
        )
    binary = root / "target" / "release" / "kvx-cli"
    if not binary.exists():
        raise RuntimeError(
            f"💀 Build appeared to succeed but binary is missing at {binary}. "
            f"Schrödinger's build."
        )
    return binary


def find_or_build_kvx_binary(project_root: Optional[Path] = None) -> Path:
    """
    Find existing release binary or build one. Debug binaries are forbidden —
    benchmarking a debug build is like timing a sprint in ski boots.
    """
    root = project_root or _guess_project_root()
    release_path = root / "target" / "release" / "kvx-cli"
    if release_path.exists():
        return release_path
    # -- 🚀 No release binary found — build one. No debug fallback allowed.
    return build_kvx_binary(root)


def _guess_project_root() -> Path:
    """
    Walk up from this file to find the repo root (where Cargo.toml lives).
    shared/ is at repo_root/shared/, so parent.parent gets us there.
    """
    # -- 🔧 shared/kvx_utils.py → shared/ → repo_root/
    candidate = Path(__file__).resolve().parent.parent
    if (candidate / "Cargo.toml").exists():
        return candidate
    # -- 🐛 Fallback: cwd. Hope for the best, prepare for the worst.
    return Path.cwd()


# ============================================================================
#  📊 Metrics Sampling — CPU/RSS via ps, because observability matters
# ============================================================================

class MetricsSampler:
    """
    Background thread that samples CPU% and RSS for a given PID.
    Uses `ps -p PID -o %cpu=,rss=` every interval seconds.

    Knowledge: On macOS, rss from ps is in KB. We convert to MB in results.
    The sampler runs in a daemon thread so it dies with the parent process.
    No zombie processes. Unlike my TODO list, which will live forever.
    """

    def __init__(self, pid: int, interval: float = 0.5):
        self._pid = pid
        self._interval = interval
        self._cpu_samples: list[float] = []
        self._rss_samples: list[float] = []
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        """Launch the sampling thread. Like mission control, but for one process."""
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> dict:
        """
        Stop sampling and return aggregate stats.
        Returns dict with avg_cpu_pct, peak_cpu_pct, avg_mem_mb, peak_mem_mb.
        """
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

        def safe_avg(vals):
            return round(sum(vals) / len(vals), 2) if vals else 0.0

        def safe_peak(vals):
            return round(max(vals), 2) if vals else 0.0

        return {
            "avg_cpu_pct": safe_avg(self._cpu_samples),
            "peak_cpu_pct": safe_peak(self._cpu_samples),
            "avg_mem_mb": safe_avg(self._rss_samples),
            "peak_mem_mb": safe_peak(self._rss_samples),
        }

    def _sample_loop(self):
        """The inner loop. Runs until stopped. Eats samples like pac-man eats dots."""
        while not self._stop_event.is_set():
            try:
                result = subprocess.run(
                    ["ps", "-p", str(self._pid), "-o", "%cpu=,rss="],
                    capture_output=True, text=True, timeout=2,
                )
                if result.returncode == 0 and result.stdout.strip():
                    parts = result.stdout.strip().split()
                    if len(parts) >= 2:
                        self._cpu_samples.append(float(parts[0]))
                        # -- 📊 rss from ps on macOS is KB → convert to MB
                        self._rss_samples.append(float(parts[1]) / 1024.0)
            except Exception:
                pass
            self._stop_event.wait(self._interval)


# ============================================================================
#  📝 Result Recording — JSON benchmark results for the notebook
# ============================================================================

def record_result(
    run_id: str,
    tool: str,
    dataset: str,
    engine: str,
    expected_docs: int,
    indexed_docs: int,
    duration_sec: float,
    exit_code: int,
    metrics: dict,
    output_dir: Path,
) -> Path:
    """
    Write a single JSON benchmark result to output_dir/run_id.json.
    Returns the path to the written file.

    The JSON schema matches what the Jupyter notebook expects for chart generation.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    docs_per_min = round((indexed_docs / duration_sec * 60), 0) if duration_sec > 0 else 0

    record = {
        "run_id": run_id,
        "tool": tool,
        "dataset": dataset,
        "engine": engine,
        "expected_docs": expected_docs,
        "indexed_docs": indexed_docs,
        "duration_sec": round(duration_sec, 3),
        "docs_per_min": docs_per_min,
        "peak_mem_mb": metrics.get("peak_mem_mb", 0),
        "avg_cpu_pct": metrics.get("avg_cpu_pct", 0),
        "peak_cpu_pct": metrics.get("peak_cpu_pct", 0),
        "avg_mem_mb": metrics.get("avg_mem_mb", 0),
        "exit_code": exit_code,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    output_path = output_dir / f"{run_id}.json"
    with open(output_path, "w") as f:
        json.dump(record, f, indent=2)

    return output_path


# ============================================================================
#  🔧 URL Helpers — because esrally wants host:port, not a full URL
# ============================================================================

def extract_host_port(url: str) -> str:
    """
    Extract host:port from a full URL for tools that want bare host:port.
    esrally's --target-hosts flag demands this format. Because of course it does.
    """
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return f"{parsed.hostname}:{port}"


def engine_from_url(url: str) -> str:
    """
    Heuristic: port 9201 = opensearch, else elasticsearch.
    Not bulletproof, but works for our benchmark/demo docker setups.
    """
    return "opensearch" if ":9201" in url else "elasticsearch"


def get_engine_url(engine: str, es_url: str = "http://localhost:9200", os_url: str = "http://localhost:9201") -> str:
    """Map engine name to cluster URL."""
    if engine == "opensearch":
        return os_url
    return es_url


# ============================================================================
#  ⏱️  Timing Helpers — wall clock, no quantum uncertainty
# ============================================================================

def generate_run_id(tool_label: str, dataset: str, engine: str) -> str:
    """Generate a unique run ID with timestamp. Results never collide (unlike opinions)."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{tool_label}__{dataset}__{engine}__{ts}"


# -- 🦆 This duck is here for emotional support. It has no other purpose.
