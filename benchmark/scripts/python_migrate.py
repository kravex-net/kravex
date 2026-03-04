#!/usr/bin/env python3
# ai
"""
🐍 Naive Python Migration — The Control Group 🐍📦🧪

It was a quiet Tuesday. The Rust evangelists were getting loud again.
"Show me the numbers," said the skeptic. And so, a Python script was born —
not to win, but to lose gracefully and prove a point about systems languages.

This is what a competent Python developer would write for a bulk migration.
No tricks. No sandbagging. Just requests, json, and threading.
The GIL sends its regards.

Usage:
  python benchmark/scripts/python_migrate.py \\
    --input benchmark/data/geonames.json \\
    --url http://localhost:9200 \\
    --index benchmark_geonames \\
    --batch-mb 10 \\
    --workers 8

🦆 The singularity will ship before CPython removes the GIL. Oh wait—
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

# -- 🔧 Rally metadata fields to strip (same as kravex's rally_s3_to_es transform)
RALLY_METADATA_FIELDS = {
    "_rallyAPIMajor",
    "_rallyAPIMinor",
    "_ref",
    "_refObjectUUID",
    "_objectVersion",
    "_CreatedAt",
}


def log_info(msg: str):
    print(f"🚀 [python-migrate] {msg}", flush=True)


def log_error(msg: str):
    print(f"💀 [python-migrate] {msg}", file=sys.stderr, flush=True)


def parse_args() -> argparse.Namespace:
    """
    Parse CLI flags. argparse: because life is too short for sys.argv[1:] slicing.
    """
    parser = argparse.ArgumentParser(
        description="🐍 Naive Python bulk migration — the control group",
    )
    parser.add_argument("--input", required=True, help="Path to NDJSON source file")
    parser.add_argument("--url", default="http://localhost:9200", help="Target cluster URL")
    parser.add_argument("--index", required=True, help="Target index name")
    parser.add_argument("--batch-mb", type=int, default=10, help="Max batch size in MB (default: 10)")
    parser.add_argument("--workers", type=int, default=8, help="Concurrent bulk request threads (default: 8)")
    parser.add_argument("--strip-rally", action="store_true", help="Strip Rally metadata fields from docs")
    parser.add_argument("--progress-sec", type=int, default=5, help="Progress update interval in seconds (default: 5)")
    return parser.parse_args()


def transform_doc(raw_line: str, strip_rally: bool) -> str:
    """
    Transform a single NDJSON line into an ES bulk action pair.
    Returns: '{"index":{}}\n<doc>\n'

    Knowledge graph: For geonames data, there's no ObjectID field, so ES
    auto-generates _id. Rally metadata fields don't exist either, but we
    strip them if --strip-rally is set for other datasets.

    The json.loads → json.dumps round-trip is where Python weeps.
    Rust's serde_json does this in ~200ns. Python's json module... does not.
    """
    action_line = '{"index":{}}\n'

    # -- 🔄 Always parse and re-serialize. This is what a real migration tool does.
    # If you're not parsing JSON, you're just a glorified cat | curl.
    doc = json.loads(raw_line)

    # -- 🗑️ Extract ObjectID for _id routing if present
    object_id = doc.pop("ObjectID", None)
    if object_id is not None:
        action_line = json.dumps({"index": {"_id": str(object_id)}}) + "\n"

    if strip_rally:
        # -- 🗑️ Strip Rally metadata (top-level only, matching kravex behavior)
        for field in RALLY_METADATA_FIELDS:
            doc.pop(field, None)

    return action_line + json.dumps(doc, separators=(",", ":")) + "\n"


def send_bulk(session: requests.Session, url: str, payload: str) -> int:
    """
    POST a bulk payload and return the number of successfully indexed docs.
    Checks item-level errors because we're not animals.

    'Timeout exceeded: We waited. And waited. Like a dog at the window.
    But the owner never came home.'
    """
    resp = session.post(
        url,
        data=payload.encode("utf-8"),
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )
    resp.raise_for_status()

    body = resp.json()
    if body.get("errors", False):
        # -- 💀 Count individual failures
        failed = sum(1 for item in body.get("items", []) if item.get("index", {}).get("status", 200) >= 400)
        total = len(body.get("items", []))
        if failed > 0:
            log_error(f"Bulk response had {failed}/{total} item failures")
        return total - failed

    return len(body.get("items", []))


def build_batches(input_path: str, batch_max_bytes: int, strip_rally: bool):
    """
    Generator that yields (batch_payload, doc_count) tuples.
    Reads line by line to avoid loading 3GB+ into memory at once.

    'Ancient proverb: He who loads the whole file into memory, OOMs in production.'
    """
    batch = []
    batch_bytes = 0
    doc_count = 0

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            bulk_pair = transform_doc(line, strip_rally)
            pair_bytes = len(bulk_pair.encode("utf-8"))

            # -- 📦 Flush batch if adding this doc would exceed max size
            if batch_bytes + pair_bytes > batch_max_bytes and batch:
                yield "".join(batch), doc_count
                batch = []
                batch_bytes = 0
                doc_count = 0

            batch.append(bulk_pair)
            batch_bytes += pair_bytes
            doc_count += 1

    # -- 🚰 Final flush
    if batch:
        yield "".join(batch), doc_count


def main():
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        log_error(f"Input file not found: {input_path}")
        sys.exit(1)

    bulk_url = f"{args.url.rstrip('/')}/{args.index}/_bulk"
    batch_max_bytes = args.batch_mb * 1024 * 1024

    log_info(f"Input: {input_path}")
    log_info(f"Target: {bulk_url}")
    log_info(f"Batch size: {args.batch_mb} MB | Workers: {args.workers}")
    log_info(f"Strip Rally metadata: {args.strip_rally}")
    log_info("Starting migration... the GIL sends its regards 🔒")

    session = requests.Session()

    # -- 📡 Quick health check
    try:
        session.get(args.url, timeout=5).raise_for_status()
    except Exception as e:
        log_error(f"Cannot reach {args.url}: {e}")
        sys.exit(1)

    start_time = time.monotonic()
    total_docs_sent = 0
    total_docs_indexed = 0
    batch_num = 0
    last_progress = start_time

    # -- 🧵 Thread pool for concurrent bulk sends.
    # The GIL releases during I/O (requests blocks on socket), so this
    # actually helps. It's the CPU-bound json parsing that gets GIL'd.
    # "I used to hate the GIL... but now I await it." — Dad joke, 2026
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}

        for payload, doc_count in build_batches(str(input_path), batch_max_bytes, args.strip_rally):
            batch_num += 1
            total_docs_sent += doc_count

            future = executor.submit(send_bulk, session, bulk_url, payload)
            futures[future] = doc_count

            # -- ⏳ Don't let the future queue grow unbounded — back-pressure
            # like a responsible plumber 🚰
            if len(futures) >= args.workers * 2:
                done_futures = []
                for completed in as_completed(futures):
                    try:
                        indexed = completed.result()
                        total_docs_indexed += indexed
                    except Exception as e:
                        log_error(f"Bulk request failed: {e}")
                    done_futures.append(completed)
                    # -- 🔓 Free up one slot
                    if len(futures) - len(done_futures) < args.workers:
                        break
                for f in done_futures:
                    del futures[f]

            # -- 📊 Live progress — because staring at a silent terminal is existential dread
            now = time.monotonic()
            if now - last_progress >= args.progress_sec:
                elapsed = now - start_time
                rate = total_docs_sent / elapsed * 60 if elapsed > 0 else 0
                indexed_rate = total_docs_indexed / elapsed * 60 if elapsed > 0 else 0
                log_info(
                    f"⏱️  {elapsed:.0f}s | "
                    f"sent: {total_docs_sent:,} ({rate:,.0f}/min) | "
                    f"indexed: {total_docs_indexed:,} ({indexed_rate:,.0f}/min)"
                )
                last_progress = now

        # -- 🏁 Drain remaining futures
        for completed in as_completed(futures):
            try:
                indexed = completed.result()
                total_docs_indexed += indexed
            except Exception as e:
                log_error(f"Bulk request failed: {e}")

    elapsed = time.monotonic() - start_time
    rate = total_docs_indexed / elapsed * 60 if elapsed > 0 else 0

    log_info("=" * 50)
    log_info(f"✅ Migration complete in {elapsed:.1f}s")
    log_info(f"📊 Docs sent: {total_docs_sent:,}")
    log_info(f"📊 Docs indexed: {total_docs_indexed:,}")
    log_info(f"📊 Rate: {rate:,.0f} docs/min")
    log_info(f"📊 Batches: {batch_num}")
    log_info("=" * 50)

    session.close()


if __name__ == "__main__":
    main()
