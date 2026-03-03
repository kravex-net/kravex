#!/usr/bin/env python3
# ai
"""
🚀 Kravex Demo — Migration Runner 🚀
The hour has come. The configs are loaded. The binary is built.
Five indices stand between us and the promised land of OpenSearch.
"Run," whispered the subprocess. And so we did. 🦆
"""

import os
import subprocess
import sys
import time
from pathlib import Path

import requests
from rich.console import Console
from rich.table import Table

# -- 🔧 Add repo root to path for shared imports
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import (  # noqa: E402
    find_or_build_kvx_binary,
    get_doc_count as shared_get_doc_count,
)

console = Console()

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
OS_URL = os.environ.get("OS_URL", "http://localhost:9201")
CONFIGS_DIR = Path(__file__).parent.parent / "configs"
PROJECT_ROOT = REPO_ROOT

# Index order: smallest first for quick wins, biggest last for drama
MIGRATION_ORDER = [
    "job_postings",
    "employees",
    "performance_reviews",
    "learning_records",
    "search_analytics",
]


def get_doc_count(url: str, index_name: str) -> int:
    """Delegates to shared.kvx_utils — eventual consistency is a lifestyle."""
    return shared_get_doc_count(url, index_name)


def find_kvx_binary() -> Path:
    """Locate or build the kvx-cli binary via shared utils."""
    console.print("[dim]  Looking for kvx-cli binary...[/dim]")
    binary = find_or_build_kvx_binary(PROJECT_ROOT)
    console.print(f"[dim]  Found binary: {binary}[/dim]")
    return binary


def migrate_index(binary: Path, config_path: Path, index_name: str) -> dict:
    """Run kvx-cli for a single index. Returns timing and result info."""
    source_count = get_doc_count(ES_URL, index_name)

    console.print(f"\n[bold cyan]  🚀 Migrating: {index_name}[/bold cyan] "
                  f"({source_count:,} docs)")

    start = time.time()
    result = subprocess.run(
        [str(binary), "run", "--config", str(config_path)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    elapsed = time.time() - start

    if result.returncode != 0:
        console.print(f"[red]  💀 Migration failed for {index_name}[/red]")
        if result.stderr:
            # Show last 10 lines of stderr
            stderr_lines = result.stderr.strip().split("\n")
            for line in stderr_lines[-10:]:
                console.print(f"[dim red]    {line}[/dim red]")
        return {
            "index": index_name,
            "status": "failed",
            "elapsed": elapsed,
            "source_docs": source_count,
            "dest_docs": 0,
            "throughput": 0,
        }

    # Check dest doc count
    dest_count = get_doc_count(OS_URL, index_name)
    throughput = dest_count / elapsed if elapsed > 0 else 0

    status = "ok" if dest_count == source_count else "partial"
    status_icon = "✅" if status == "ok" else "⚠️"

    console.print(f"  {status_icon} {index_name}: {dest_count:,}/{source_count:,} docs "
                  f"in {elapsed:.1f}s ({throughput:,.0f} docs/sec)")

    return {
        "index": index_name,
        "status": status,
        "elapsed": elapsed,
        "source_docs": source_count,
        "dest_docs": dest_count,
        "throughput": throughput,
    }


def main():
    console.print("\n[bold cyan]🚀 Kravex Demo — Migration Phase[/bold cyan]")
    console.print(f"[dim]ES: {ES_URL} → OS: {OS_URL}[/dim]\n")

    # Find or build binary
    binary = find_kvx_binary()

    # Run migrations
    results = []
    total_start = time.time()

    for index_name in MIGRATION_ORDER:
        config_path = CONFIGS_DIR / f"{index_name}.toml"
        if not config_path.exists():
            console.print(f"[red]💀 Config not found: {config_path}[/red]")
            continue
        result = migrate_index(binary, config_path, index_name)
        results.append(result)

    total_elapsed = time.time() - total_start

    # Summary table
    console.print("\n[bold]📊 Migration Summary[/bold]\n")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Index", style="cyan")
    table.add_column("Source Docs", justify="right")
    table.add_column("Dest Docs", justify="right")
    table.add_column("Time", justify="right")
    table.add_column("Throughput", justify="right")
    table.add_column("Status", justify="center")

    total_docs = 0
    all_ok = True
    for r in results:
        status_icon = "✅" if r["status"] == "ok" else ("💀" if r["status"] == "failed" else "⚠️")
        if r["status"] != "ok":
            all_ok = False
        total_docs += r.get("dest_docs", 0)
        table.add_row(
            r["index"],
            f"{r['source_docs']:,}",
            f"{r['dest_docs']:,}",
            f"{r['elapsed']:.1f}s",
            f"{r['throughput']:,.0f} docs/s",
            status_icon,
        )
    console.print(table)

    overall_throughput = total_docs / total_elapsed if total_elapsed > 0 else 0
    console.print(f"\n[bold]Total: {total_docs:,} docs in {total_elapsed:.1f}s "
                  f"({overall_throughput:,.0f} docs/sec overall)[/bold]")

    if not all_ok:
        console.print("[yellow]⚠️  Some migrations had issues[/yellow]")
        sys.exit(1)
    else:
        console.print("[green]✅ All migrations completed successfully![/green]\n")


if __name__ == "__main__":
    main()
