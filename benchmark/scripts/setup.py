#!/usr/bin/env python3
# ai
"""
🔧 Kravex Benchmark Suite — Setup Script 🔧🚀📦
It was a dark and stormy deploy. The benchmarks hadn't run in weeks.
The data directory was empty. The dependencies were missing.
And then... setup.py arrived. With downloads. And hope.
This script installs tools, downloads Rally track datasets,
and prepares the benchmark environment. 🦆

The singularity will arrive before these datasets finish downloading.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

# -- 🔧 Add repo root to path so we can import shared utils
SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_DIR = SCRIPT_DIR.parent
REPO_ROOT = BENCHMARK_DIR.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import find_or_build_kvx_binary  # noqa: E402


# ============================================================================
#  📡 Dataset Configuration — Rally track data from Elastic's public S3
# ============================================================================

DATASETS = {
    "geonames": {
        "url": "https://rally-tracks.elastic.co/geonames/documents-2.json.bz2",
        "expected_docs": 11_396_503,
    },
    "noaa": {
        "url": "https://rally-tracks.elastic.co/noaa/documents.json.bz2",
        "expected_docs": 33_659_481,
    },
    "pmc": {
        "url": "https://rally-tracks.elastic.co/pmc/documents.json.bz2",
        "expected_docs": 574_199,
    },
}

# -- 📢 Doc count tolerance: 1% deviation allowed because sometimes downloads are vibes-based
DOC_COUNT_TOLERANCE_PCT = 1

DATA_DIR = BENCHMARK_DIR / "data"
RESULTS_DIR = BENCHMARK_DIR / "results"


# ============================================================================
#  📢 Logging — consistent emoji-prefixed output
# ============================================================================

def log_info(msg: str):
    print(f"🚀 {msg}")


def log_warn(msg: str):
    print(f"⚠️  {msg}")


def log_error(msg: str):
    print(f"💀 {msg}", file=sys.stderr)


# ============================================================================
#  🔧 Dependency Checks — "You can't handle the truth about your missing binaries"
# ============================================================================

def check_command(name: str) -> bool:
    """Check if a command exists on PATH. Returns True if found, False if lost in the void."""
    return shutil.which(name) is not None


def check_and_install_deps():
    """
    Verify all required dependencies exist or attempt to install them.
    'Against all odds, we have tools.'
    """
    log_info("Checking dependencies — 'You can't handle the truth... about your missing binaries.'")

    # -- 🔧 Hard requirements — abort if missing
    hard_deps = ["python3", "curl", "bunzip2"]
    for dep in hard_deps:
        if check_command(dep):
            log_info(f"{dep} is present and accounted for ✅")
        else:
            log_error(f"{dep} not found. On macOS. How. How did you do this.")
            sys.exit(1)

    # -- 📦 esrally: pip3 install. Optional — benchmark can skip it.
    if not check_command("esrally"):
        log_warn("esrally not found — attempting pip3 install")
        try:
            # -- 🎬 "In a world where benchmarks matter... one tool dared to measure them."
            subprocess.run(
                ["pip3", "install", "esrally", "--quiet"],
                check=True, capture_output=True, text=True, timeout=120,
            )
            log_info("esrally installed ✅")
        except (subprocess.CalledProcessError, FileNotFoundError):
            log_warn("esrally install failed — skipping esrally benchmarks will be required")
    else:
        log_info("esrally found ✅")

    # -- 📦 elasticdump: npm install -g. Optional — benchmark can skip it.
    if not check_command("elasticdump"):
        log_warn("elasticdump not found — attempting npm install -g")
        try:
            # -- 🔧 "No cap, this tool slaps fr fr (for real for real)"
            subprocess.run(
                ["npm", "install", "-g", "elasticdump", "--quiet"],
                check=True, capture_output=True, text=True, timeout=120,
            )
            log_info("elasticdump installed ✅")
        except (subprocess.CalledProcessError, FileNotFoundError):
            log_warn("elasticdump install failed — elasticdump benchmarks will be skipped")
    else:
        log_info("elasticdump found ✅")

    log_info("Dependency check complete — 'Against all odds, we have tools.'")


# ============================================================================
#  📡 Dataset Download + Decompression
# ============================================================================

def download_dataset(name: str, url: str) -> bool:
    """
    Download a .bz2 dataset if the decompressed NDJSON does not already exist.
    Uses curl with -L (follow redirects) and -# (progress bar).
    Returns True on success.

    '(This may take a while. Go touch grass. Or don't. I'm a script, not your therapist.)'
    """
    bz2_path = DATA_DIR / f"{name}.json.bz2"
    ndjson_path = DATA_DIR / f"{name}.json"

    if ndjson_path.exists():
        log_info(f"{name} already decompressed at {ndjson_path} — skipping download ✅")
        return True

    if not bz2_path.exists():
        log_info(f"Downloading {name} from {url}")
        log_info("(This may take a while. Go touch grass. Or don't. I'm a script, not your therapist.)")
        try:
            subprocess.run(
                ["curl", "-L", "-#", "-o", str(bz2_path), url],
                check=True, timeout=3600,
            )
            log_info(f"Download complete for {name} ✅")
        except subprocess.CalledProcessError:
            log_error(f"Download of {name} failed. Check your internet. Or Elastic's S3. Or the stars.")
            bz2_path.unlink(missing_ok=True)
            return False
    else:
        log_info(f"{name}.json.bz2 already present — skipping download")

    # -- 🔧 Decompress — 'Resistance is futile. bunzip2 always wins.'
    log_info(f"Decompressing {name}.json.bz2 — 'Resistance is futile. bunzip2 always wins.'")
    try:
        subprocess.run(
            ["bunzip2", "-k", str(bz2_path)],
            check=True, timeout=600,
        )
        log_info(f"{name} decompressed to {ndjson_path} ✅")
        return True
    except subprocess.CalledProcessError:
        log_error(f"bunzip2 failed on {name}. The archive is corrupted, or bunzip2 is having a moment.")
        ndjson_path.unlink(missing_ok=True)
        return False


def verify_doc_count(name: str, expected: int):
    """
    Verify NDJSON line count is within tolerance of expected doc count.
    'Ancient proverb: He who skips verification, ships production incidents.'
    """
    ndjson_path = DATA_DIR / f"{name}.json"
    if not ndjson_path.exists():
        log_warn(f"{name} file not found at {ndjson_path} — skipping count verification")
        return

    log_info(f"Counting lines in {name} — 'This is fine.' (reading {ndjson_path})")

    # -- 📊 wc -l is faster than reading in Python for multi-GB files
    result = subprocess.run(
        ["wc", "-l"], stdin=open(ndjson_path, "rb"),
        capture_output=True, text=True,
    )
    actual = int(result.stdout.strip())

    lower = expected - (expected * DOC_COUNT_TOLERANCE_PCT // 100)
    upper = expected + (expected * DOC_COUNT_TOLERANCE_PCT // 100)

    if lower <= actual <= upper:
        log_info(f"{name}: {actual:,} lines (expected ~{expected:,}) ✅")
    else:
        log_warn(f"{name}: {actual:,} lines but expected ~{expected:,} — ⚠️  this is a vibe check failure")
        log_warn(f"Lower bound: {lower:,}, Upper bound: {upper:,}. Actual: {actual:,}.")


# ============================================================================
#  🚀 Main — checks deps, creates dirs, downloads datasets, verifies counts
# ============================================================================

def main():
    log_info("🚀 Kravex Benchmark Suite — Setup")
    log_info(f"BENCHMARK_DIR: {BENCHMARK_DIR}")
    print()

    check_and_install_deps()

    # -- 📁 Create directories. mkdir -p: the unsung hero of setup scripts worldwide.
    log_info("Creating benchmark directory structure — building the scaffolding before the circus arrives")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    log_info(f"Directories ready: data={DATA_DIR}, results={RESULTS_DIR} ✅")

    # -- 📡 Download Rally track datasets — 'You can't stop the signal, Mal.'
    print()
    log_info("Downloading Rally track datasets — 'You can't stop the signal, Mal.'")

    for name, info in DATASETS.items():
        if not download_dataset(name, info["url"]):
            log_warn(f"{name} download failed — continuing anyway, you optimist")

    # -- 🎯 Verify doc counts
    print()
    log_info("Verifying doc counts — Ancient proverb: 'He who skips verification, ships production incidents'")

    for name, info in DATASETS.items():
        verify_doc_count(name, info["expected_docs"])

    # -- 🔧 Pre-build kvx-cli binary so benchmark runs don't include compile time
    print()
    log_info("Pre-building kvx-cli release binary...")
    try:
        binary = find_or_build_kvx_binary(REPO_ROOT)
        log_info(f"kvx-cli binary ready at {binary} ✅")
    except RuntimeError as e:
        log_warn(f"kvx-cli build failed: {e}")
        log_warn("Benchmark will attempt to build again at runtime")

    print()
    log_info("✅ Setup complete. May your benchmarks be fast and your baselines be honest.")


if __name__ == "__main__":
    main()
