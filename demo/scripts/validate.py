#!/usr/bin/env python3
# ai
"""
🧪 Kravex Demo — Post-Migration Validation 🧪
Trust, but verify. Then verify again. Then diff a random sample
just to make sure you're not living in a simulation.
Spoiler: you might be. 🦆
"""

import json
import os
import sys
import hashlib
from pathlib import Path

import requests
from rich.console import Console
from rich.table import Table

# -- 🔧 Add repo root to path for shared imports
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import get_doc_count as shared_get_doc_count  # noqa: E402

console = Console()

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
OS_URL = os.environ.get("OS_URL", "http://localhost:9201")

DEMO_INDICES = [
    "employees",
    "learning_records",
    "performance_reviews",
    "job_postings",
    "search_analytics",
]


def get_doc_count(url: str, index_name: str) -> int:
    """Delegates to shared.kvx_utils — the eternal dance of eventual consistency."""
    return shared_get_doc_count(url, index_name)


def get_mapping_fields(url: str, index_name: str) -> dict:
    """Get mapping field names and types as a flat dict."""
    resp = requests.get(f"{url}/{index_name}/_mapping", timeout=10)
    if resp.status_code != 200:
        return {}
    data = resp.json()
    properties = data.get(index_name, {}).get("mappings", {}).get("properties", {})

    fields = {}
    def flatten(props, prefix=""):
        for name, defn in props.items():
            full = f"{prefix}{name}" if not prefix else f"{prefix}.{name}"
            fields[full] = defn.get("type", "object")
            if "properties" in defn:
                flatten(defn["properties"], full)
    flatten(properties)
    return fields


def sample_docs(url: str, index_name: str, size: int = 5) -> list[dict]:
    """Fetch a deterministic sample of docs by sorting on _id."""
    resp = requests.post(
        f"{url}/{index_name}/_search",
        json={
            "size": size,
            "sort": [{"_id": "asc"}],
            "query": {"match_all": {}},
        },
        timeout=10,
    )
    if resp.status_code != 200:
        return []
    hits = resp.json().get("hits", {}).get("hits", [])
    return hits


def compare_docs(es_docs: list[dict], os_docs: list[dict]) -> list[dict]:
    """Compare sampled docs by _id. Returns list of mismatches."""
    es_by_id = {d["_id"]: d["_source"] for d in es_docs}
    os_by_id = {d["_id"]: d["_source"] for d in os_docs}

    mismatches = []
    for doc_id in es_by_id:
        if doc_id not in os_by_id:
            mismatches.append({"id": doc_id, "issue": "missing in OpenSearch"})
        elif es_by_id[doc_id] != os_by_id[doc_id]:
            mismatches.append({"id": doc_id, "issue": "content differs"})
    return mismatches


def validate_index(index_name: str) -> dict:
    """Run all validation checks for a single index."""
    checks = {}

    # Check 1: Doc count comparison
    es_count = get_doc_count(ES_URL, index_name)
    os_count = get_doc_count(OS_URL, index_name)
    count_match = es_count == os_count and es_count > 0
    checks["doc_count"] = {
        "pass": count_match,
        "detail": f"ES={es_count:,} OS={os_count:,}",
    }

    # Check 2: Mapping schema comparison
    es_fields = get_mapping_fields(ES_URL, index_name)
    os_fields = get_mapping_fields(OS_URL, index_name)

    missing_in_os = set(es_fields.keys()) - set(os_fields.keys())
    type_mismatches = []
    for field in set(es_fields.keys()) & set(os_fields.keys()):
        if es_fields[field] != os_fields[field]:
            type_mismatches.append(f"{field}: ES={es_fields[field]} OS={os_fields[field]}")

    mapping_ok = len(missing_in_os) == 0 and len(type_mismatches) == 0
    mapping_detail = "schemas match"
    if missing_in_os:
        mapping_detail = f"missing fields in OS: {missing_in_os}"
    elif type_mismatches:
        mapping_detail = f"type mismatches: {type_mismatches}"
    checks["mapping"] = {"pass": mapping_ok, "detail": mapping_detail}

    # Check 3: Sample doc comparison (5 docs by _id sort)
    es_sample = sample_docs(ES_URL, index_name, 5)
    os_sample = sample_docs(OS_URL, index_name, 5)
    mismatches = compare_docs(es_sample, os_sample)
    sample_ok = len(mismatches) == 0 and len(es_sample) > 0
    sample_detail = f"{len(es_sample)} docs compared, {len(mismatches)} mismatches"
    checks["sample_docs"] = {"pass": sample_ok, "detail": sample_detail}

    # Check 4: match_all hit count comparison
    try:
        es_resp = requests.post(
            f"{ES_URL}/{index_name}/_search",
            json={"size": 0, "query": {"match_all": {}}},
            timeout=10,
        )
        os_resp = requests.post(
            f"{OS_URL}/{index_name}/_search",
            json={"size": 0, "query": {"match_all": {}}},
            timeout=10,
        )
        es_hits = es_resp.json().get("hits", {}).get("total", {})
        os_hits = os_resp.json().get("hits", {}).get("total", {})

        # ES 7.x returns {"value": N, "relation": "eq"}, OS does too
        es_total = es_hits.get("value", 0) if isinstance(es_hits, dict) else es_hits
        os_total = os_hits.get("value", 0) if isinstance(os_hits, dict) else os_hits
        query_ok = es_total == os_total
        checks["match_all"] = {
            "pass": query_ok,
            "detail": f"ES={es_total:,} OS={os_total:,}",
        }
    except Exception as e:
        checks["match_all"] = {"pass": False, "detail": str(e)}

    all_pass = all(c["pass"] for c in checks.values())
    return {"index": index_name, "checks": checks, "pass": all_pass}


def main():
    console.print("\n[bold cyan]🧪 Kravex Demo — Post-Migration Validation[/bold cyan]")
    console.print(f"[dim]ES: {ES_URL} | OS: {OS_URL}[/dim]\n")

    results = []
    for index_name in DEMO_INDICES:
        result = validate_index(index_name)
        results.append(result)

    # Results table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Index", style="cyan")
    table.add_column("Doc Count", justify="center")
    table.add_column("Mapping", justify="center")
    table.add_column("Sample Docs", justify="center")
    table.add_column("match_all", justify="center")
    table.add_column("Verdict", justify="center")

    all_pass = True
    for r in results:
        checks = r["checks"]
        verdict = "[green]✅ PASS[/green]" if r["pass"] else "[red]💀 FAIL[/red]"
        if not r["pass"]:
            all_pass = False

        table.add_row(
            r["index"],
            "✅" if checks["doc_count"]["pass"] else "💀",
            "✅" if checks["mapping"]["pass"] else "💀",
            "✅" if checks["sample_docs"]["pass"] else "💀",
            "✅" if checks["match_all"]["pass"] else "💀",
            verdict,
        )
    console.print(table)

    # Detail on failures
    for r in results:
        if not r["pass"]:
            console.print(f"\n[red]💀 {r['index']} failures:[/red]")
            for check_name, check in r["checks"].items():
                if not check["pass"]:
                    console.print(f"  [red]{check_name}[/red]: {check['detail']}")

    console.print()
    if all_pass:
        console.print("[bold green]✅ All validations passed! Migration verified.[/bold green]\n")
    else:
        console.print("[bold red]💀 Validation failures detected — investigate above.[/bold red]\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
