#!/usr/bin/env python3
# ai
"""
🔍 Kravex Demo — Pre-Migration Analysis 🔍
The analyst stared at the mappings. The mappings stared back.
"Are you compatible?" they whispered. The field types said nothing.
This script breaks the silence. 🦆
"""

import os
import sys
import requests
from rich.console import Console
from rich.table import Table

console = Console()

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")

# 🚫 Field types that OpenSearch 2.x doesn't support or changed semantics
BREAKING_FIELD_TYPES = {
    "string": "Removed in ES 5.x — should be 'text' or 'keyword'",
}

# ⚠️ Field types that might need attention but aren't blockers
WARN_FIELD_TYPES = {
    "flattened": "Supported in OS 2.x but behavior may differ slightly",
    "rank_feature": "Supported but verify query compatibility",
    "shape": "Verify spatial query compatibility",
}

# Known compatible types — no issues
COMPATIBLE_TYPES = {
    "text", "keyword", "long", "integer", "short", "byte", "double", "float",
    "half_float", "scaled_float", "date", "boolean", "binary", "integer_range",
    "float_range", "long_range", "double_range", "date_range", "ip",
    "completion", "geo_point", "geo_shape", "nested", "object", "token_count",
    "percolator", "join", "alias", "search_as_you_type", "histogram",
}


def get_indices() -> list[dict]:
    """Fetch all demo indices from ES."""
    resp = requests.get(
        f"{ES_URL}/_cat/indices?format=json&h=index,health,status,docs.count,store.size",
        timeout=10,
    )
    resp.raise_for_status()
    # Filter to demo indices only
    demo_indices = [
        "employees", "learning_records", "performance_reviews",
        "job_postings", "search_analytics"
    ]
    return [idx for idx in resp.json() if idx.get("index") in demo_indices]


def get_mapping(index_name: str) -> dict:
    """Fetch mapping for an index."""
    resp = requests.get(f"{ES_URL}/{index_name}/_mapping", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get(index_name, {}).get("mappings", {}).get("properties", {})


def get_settings(index_name: str) -> dict:
    """Fetch settings for an index."""
    resp = requests.get(f"{ES_URL}/{index_name}/_settings", timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get(index_name, {}).get("settings", {}).get("index", {})


def analyze_field_types(properties: dict, prefix: str = "") -> list[dict]:
    """Recursively check field types for compatibility. Returns list of findings."""
    findings = []
    for field_name, field_def in properties.items():
        full_name = f"{prefix}{field_name}" if not prefix else f"{prefix}.{field_name}"
        field_type = field_def.get("type", "object")

        if field_type in BREAKING_FIELD_TYPES:
            findings.append({
                "field": full_name,
                "type": field_type,
                "level": "FAIL",
                "message": BREAKING_FIELD_TYPES[field_type],
            })
        elif field_type in WARN_FIELD_TYPES:
            findings.append({
                "field": full_name,
                "type": field_type,
                "level": "WARN",
                "message": WARN_FIELD_TYPES[field_type],
            })
        elif field_type in COMPATIBLE_TYPES:
            findings.append({
                "field": full_name,
                "type": field_type,
                "level": "PASS",
                "message": "Compatible",
            })
        else:
            findings.append({
                "field": full_name,
                "type": field_type,
                "level": "WARN",
                "message": f"Unknown type '{field_type}' — verify manually",
            })

        # Recurse into nested/object properties
        if "properties" in field_def:
            findings.extend(analyze_field_types(field_def["properties"], full_name))

    return findings


def check_for_type_field(index_name: str) -> bool:
    """Check if the index uses the deprecated _type field (ES 6.x/7.x removal)."""
    resp = requests.get(f"{ES_URL}/{index_name}/_mapping", timeout=10)
    raw = resp.text
    # ES 7.x still has _doc type internally but it's transparent
    return "_type" in raw and '"_doc"' not in raw


def estimate_migration_time(doc_count: int) -> str:
    """Rough estimate based on ~20k docs/sec throughput assumption."""
    if doc_count <= 0:
        return "N/A"
    seconds = max(1, doc_count / 20000)
    if seconds < 60:
        return f"~{seconds:.0f}s"
    return f"~{seconds/60:.1f}min"


def main():
    console.print("\n[bold cyan]🔍 Kravex Demo — Pre-Migration Analysis[/bold cyan]")
    console.print(f"[dim]Source: {ES_URL}[/dim]\n")

    try:
        indices = get_indices()
    except Exception as e:
        console.print(f"[red]💀 Cannot reach Elasticsearch: {e}[/red]")
        sys.exit(1)

    if not indices:
        console.print("[yellow]⚠️  No demo indices found. Run seed.py first.[/yellow]")
        sys.exit(1)

    # Sort by index name for consistent output
    indices.sort(key=lambda x: x.get("index", ""))

    # Index overview table
    console.print("[bold]📊 Index Overview[/bold]\n")
    overview = Table(show_header=True, header_style="bold magenta")
    overview.add_column("Index", style="cyan")
    overview.add_column("Health", justify="center")
    overview.add_column("Docs", justify="right")
    overview.add_column("Size", justify="right")
    overview.add_column("Est. Migration", justify="right")

    for idx in indices:
        health = idx.get("health", "?")
        health_color = {"green": "green", "yellow": "yellow", "red": "red"}.get(health, "white")
        doc_count = int(idx.get("docs.count", 0))
        overview.add_row(
            idx["index"],
            f"[{health_color}]{health}[/{health_color}]",
            f"{doc_count:,}",
            idx.get("store.size", "?"),
            estimate_migration_time(doc_count),
        )
    console.print(overview)

    # Compatibility analysis per index
    console.print("\n[bold]🧪 Compatibility Analysis[/bold]\n")

    total_pass = 0
    total_warn = 0
    total_fail = 0

    for idx in indices:
        index_name = idx["index"]
        properties = get_mapping(index_name)
        findings = analyze_field_types(properties)
        has_type = check_for_type_field(index_name)

        pass_count = sum(1 for f in findings if f["level"] == "PASS")
        warn_count = sum(1 for f in findings if f["level"] == "WARN")
        fail_count = sum(1 for f in findings if f["level"] == "FAIL")

        if has_type:
            fail_count += 1

        total_pass += pass_count
        total_warn += warn_count
        total_fail += fail_count

        # Per-index verdict
        if fail_count > 0:
            verdict = "[red]FAIL 💀[/red]"
        elif warn_count > 0:
            verdict = "[yellow]WARN ⚠️[/yellow]"
        else:
            verdict = "[green]PASS ✅[/green]"

        console.print(f"  {verdict}  [cyan]{index_name}[/cyan] — "
                      f"{pass_count} pass, {warn_count} warn, {fail_count} fail")

        # Show non-PASS findings
        for f in findings:
            if f["level"] != "PASS":
                level_color = "yellow" if f["level"] == "WARN" else "red"
                console.print(f"        [{level_color}]{f['level']}[/{level_color}] "
                              f"{f['field']} ({f['type']}): {f['message']}")

        if has_type:
            console.print(f"        [red]FAIL[/red] _type field detected — "
                          f"removed in OpenSearch")

    # Summary
    console.print(f"\n[bold]📋 Summary[/bold]")
    console.print(f"  Fields analyzed: {total_pass + total_warn + total_fail}")
    console.print(f"  [green]✅ Pass: {total_pass}[/green]")
    console.print(f"  [yellow]⚠️  Warn: {total_warn}[/yellow]")
    console.print(f"  [red]💀 Fail: {total_fail}[/red]")

    if total_fail > 0:
        console.print("\n[red]💀 Migration blocked — fix FAIL items before proceeding[/red]")
        sys.exit(1)
    elif total_warn > 0:
        console.print("\n[yellow]⚠️  Migration can proceed — review warnings[/yellow]\n")
    else:
        console.print("\n[green]✅ All clear — ready to migrate![/green]\n")


if __name__ == "__main__":
    main()
