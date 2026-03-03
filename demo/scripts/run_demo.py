#!/usr/bin/env python3
# ai
"""
🎬 Kravex Demo — Full Pipeline Orchestrator 🎬
Lights. Camera. Migration.
This script runs the entire demo end-to-end:
seed → analyze → migrate → validate.
Like a movie director, but for data. And with more error handling. 🦆
"""

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# -- 🔧 Add repo root to path for shared imports
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import check_cluster_health  # noqa: E402

console = Console()

DEMO_DIR = Path(__file__).parent.parent
SCRIPTS_DIR = Path(__file__).parent
# -- 🐳 Uses the unified root-level docker-compose with --profile demo
DOCKER_COMPOSE = REPO_ROOT / "docker-compose.yml"

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
OS_URL = os.environ.get("OS_URL", "http://localhost:9201")


def check_prerequisites() -> bool:
    """Verify all required tools are installed. Like a preflight checklist, but nerdier."""
    console.print("[bold]🔧 Checking prerequisites...[/bold]\n")
    all_good = True

    checks = [
        ("docker", ["docker", "--version"]),
        ("docker compose", ["docker", "compose", "version"]),
        ("cargo", ["cargo", "--version"]),
        ("python3", ["python3", "--version"]),
    ]

    for name, cmd in checks:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                version = result.stdout.strip().split("\n")[0]
                console.print(f"  [green]✅ {name}[/green]: {version}")
            else:
                console.print(f"  [red]💀 {name}[/red]: not working")
                all_good = False
        except FileNotFoundError:
            console.print(f"  [red]💀 {name}[/red]: not found")
            all_good = False

    # Check Python packages
    try:
        import requests  # noqa: F401
        console.print("  [green]✅ requests[/green]: installed")
    except ImportError:
        console.print("  [red]💀 requests[/red]: not installed — run: pip install -r requirements.txt")
        all_good = False

    try:
        import rich  # noqa: F401
        console.print("  [green]✅ rich[/green]: installed")
    except ImportError:
        console.print("  [red]💀 rich[/red]: not installed — run: pip install -r requirements.txt")
        all_good = False

    console.print()
    return all_good


def start_docker() -> bool:
    """Start Docker Compose services with --profile demo and wait for healthy clusters."""
    console.print("[bold]🐳 Starting Docker services (--profile demo)...[/bold]\n")

    result = subprocess.run(
        ["docker", "compose", "-f", str(DOCKER_COMPOSE), "--profile", "demo", "up", "-d"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        console.print(f"[red]💀 Docker Compose failed:[/red]\n{result.stderr}")
        return False

    console.print("  [dim]Waiting for clusters to be ready...[/dim]")

    # -- 💤 Wait for both clusters using shared health check
    for name, url in [("Elasticsearch", ES_URL), ("OpenSearch", OS_URL)]:
        try:
            status = check_cluster_health(url, timeout=120)
            console.print(f"  [green]✅ {name} ready[/green] (status: {status})")
        except TimeoutError:
            console.print(f"  [red]💀 {name} not ready after 120s[/red]")
            return False

    console.print()
    return True


def run_phase(name: str, script: str, phase_num: int, total_phases: int) -> bool:
    """Run a demo phase script. Returns True on success."""
    console.print(Panel(
        f"[bold cyan]Phase {phase_num}/{total_phases}: {name}[/bold cyan]",
        border_style="cyan",
    ))

    script_path = SCRIPTS_DIR / script
    env = os.environ.copy()
    env["ES_URL"] = ES_URL
    env["OS_URL"] = OS_URL

    result = subprocess.run(
        [sys.executable, str(script_path)],
        env=env,
        timeout=900,  # 15 min max per phase
    )

    if result.returncode != 0:
        console.print(f"[red]💀 Phase {phase_num} ({name}) failed![/red]\n")
        return False
    return True


def main():
    total_start = time.time()

    console.print(Panel(
        "[bold white]🚀 Kravex Demo Pipeline 🚀[/bold white]\n"
        "[dim]Elasticsearch 7.17 → OpenSearch 2.19 Migration Demo[/dim]\n"
        "[dim]190,000 documents across 5 indices[/dim]",
        border_style="bold cyan",
        padding=(1, 2),
    ))

    # Phase 0: Prerequisites
    if not check_prerequisites():
        console.print("[red]💀 Prerequisites check failed. Install missing tools and retry.[/red]")
        sys.exit(1)

    # Phase 1: Docker
    if not start_docker():
        console.print("[red]💀 Docker startup failed. Check docker logs.[/red]")
        sys.exit(1)

    # Phase 2-5: Demo scripts
    phases = [
        ("Seed Data", "seed.py"),
        ("Pre-Migration Analysis", "analyze.py"),
        ("Migration", "migrate.py"),
        ("Post-Migration Validation", "validate.py"),
    ]

    for i, (name, script) in enumerate(phases, start=1):
        if not run_phase(name, script, i, len(phases)):
            console.print(f"\n[red]💀 Demo pipeline failed at phase {i}: {name}[/red]")
            console.print("[dim]Containers are still running for debugging.[/dim]")
            console.print(f"[dim]Teardown: docker compose -f {DOCKER_COMPOSE} --profile demo down -v[/dim]")
            sys.exit(1)

    total_elapsed = time.time() - total_start

    # Final summary
    console.print(Panel(
        f"[bold green]✅ Demo Complete![/bold green]\n\n"
        f"[white]Total time: {total_elapsed:.1f}s[/white]\n"
        f"[white]Documents: 190,000 across 5 indices[/white]\n"
        f"[white]Source: Elasticsearch 7.17 (localhost:9200)[/white]\n"
        f"[white]Destination: OpenSearch 2.19 (localhost:9201)[/white]\n\n"
        f"[dim]Explore the data:[/dim]\n"
        f"  curl http://localhost:9200/_cat/indices?v\n"
        f"  curl http://localhost:9201/_cat/indices?v\n\n"
        f"[dim]Teardown:[/dim]\n"
        f"  docker compose -f {DOCKER_COMPOSE} --profile demo down -v",
        border_style="bold green",
        padding=(1, 2),
    ))


if __name__ == "__main__":
    main()
