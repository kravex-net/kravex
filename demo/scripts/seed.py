#!/usr/bin/env python3
# ai
"""
🌱 Kravex Demo — Seed Script 🌱
It was a quiet Tuesday. The indices were empty. The clusters were lonely.
And somewhere, a Python script was about to generate 190,000 documents
of fake corporate data that would never be read by a real human.
This is that script. 🦆
"""

import json
import os
import sys
import time
import hashlib
from pathlib import Path

import requests
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# -- 🔧 Add repo root to path for shared imports
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from shared.kvx_utils import (  # noqa: E402
    create_index as shared_create_index,
    get_doc_count as shared_get_doc_count,
)

console = Console()

# 🎲 Deterministic data pools — no faker needed, no randomness, just vibes
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mona", "Nick", "Olivia", "Pat",
    "Quinn", "Ray", "Sara", "Tom", "Uma", "Vic", "Wendy", "Xander",
    "Yara", "Zane", "Aria", "Blake", "Cleo", "Dante"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson"
]

DEPARTMENTS = [
    "Engineering", "Product", "Design", "Marketing", "Sales",
    "HR", "Finance", "Legal", "Operations", "Support",
    "Data Science", "Security", "DevOps", "QA", "Research"
]

TITLES = [
    "Software Engineer", "Senior Engineer", "Staff Engineer", "Principal Engineer",
    "Engineering Manager", "Product Manager", "Designer", "Data Analyst",
    "Marketing Specialist", "Sales Representative", "HR Coordinator",
    "Financial Analyst", "Legal Counsel", "Operations Manager", "Support Lead",
    "Data Scientist", "Security Engineer", "DevOps Engineer", "QA Engineer",
    "Research Scientist"
]

SKILLS = [
    "Python", "Rust", "JavaScript", "TypeScript", "Go", "Java", "C++",
    "SQL", "PostgreSQL", "Elasticsearch", "OpenSearch", "Kubernetes",
    "Docker", "AWS", "GCP", "Azure", "React", "Vue", "Angular",
    "Machine Learning", "Data Engineering", "CI/CD", "Terraform",
    "GraphQL", "REST", "gRPC", "Redis", "Kafka", "Spark", "Airflow"
]

CITIES = [
    ("San Francisco", "CA", "US"), ("New York", "NY", "US"),
    ("Austin", "TX", "US"), ("Seattle", "WA", "US"),
    ("Denver", "CO", "US"), ("Chicago", "IL", "US"),
    ("Boston", "MA", "US"), ("Portland", "OR", "US"),
    ("London", "England", "UK"), ("Berlin", "Brandenburg", "DE"),
    ("Toronto", "Ontario", "CA"), ("Sydney", "NSW", "AU"),
    ("Tokyo", "Tokyo", "JP"), ("Singapore", "Singapore", "SG"),
    ("Amsterdam", "North Holland", "NL")
]

COURSES = [
    "Introduction to Machine Learning", "Advanced Kubernetes",
    "Rust for Systems Programming", "Leadership Fundamentals",
    "Effective Communication", "Data Privacy & Compliance",
    "Cloud Architecture Patterns", "Agile Project Management",
    "Security Best Practices", "Technical Writing",
    "Python for Data Science", "Elasticsearch Deep Dive",
    "Conflict Resolution", "Time Management Mastery",
    "Public Speaking Workshop", "Financial Literacy for Engineers",
    "Diversity & Inclusion Training", "Ethics in AI",
    "Microservices Architecture", "Performance Optimization"
]

COURSE_CATEGORIES = [
    "Technical", "Leadership", "Compliance", "Soft Skills", "Data Science"
]

COURSE_PROVIDERS = [
    "Internal", "Coursera", "Udemy", "LinkedIn Learning", "O'Reilly"
]

REVIEW_COMMENTS = [
    "Consistently delivers high-quality work ahead of schedule.",
    "Shows strong initiative and takes ownership of complex problems.",
    "Excellent collaboration skills across cross-functional teams.",
    "Needs improvement in documentation and knowledge sharing.",
    "Demonstrates strong technical leadership and mentoring abilities.",
    "Proactive in identifying and resolving technical debt.",
    "Could benefit from more structured approach to project planning.",
    "Outstanding contribution to team culture and morale.",
    "Solid performer who meets expectations consistently.",
    "Shows great potential for growth into senior roles."
]

JOB_DESCRIPTIONS = [
    "Join our team to build next-generation search infrastructure.",
    "We are looking for a passionate engineer to scale our platform.",
    "Help us transform how enterprises manage their workforce data.",
    "Drive innovation in our core data pipeline architecture.",
    "Lead the development of our real-time analytics platform."
]

SEARCH_QUERIES = [
    "employee handbook", "vacation policy", "benefits enrollment",
    "performance review", "salary bands", "org chart",
    "onboarding checklist", "remote work policy", "expense report",
    "training catalog", "career development", "mentorship program",
    "diversity report", "company values", "holiday calendar",
    "parking policy", "IT helpdesk", "security training",
    "compliance certification", "team directory"
]

SEARCH_TYPES = ["keyword", "phrase", "fuzzy", "prefix", "wildcard"]

# 📊 Index configurations: (name, doc_count)
INDICES = [
    ("employees", 10_000),
    ("learning_records", 50_000),
    ("performance_reviews", 25_000),
    ("job_postings", 5_000),
    ("search_analytics", 100_000),
]

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
OS_URL = os.environ.get("OS_URL", "http://localhost:9201")
BULK_BATCH_SIZE = 1000
MAPPINGS_DIR = Path(__file__).parent.parent / "mappings"


def deterministic_hash(seed: str, mod: int) -> int:
    """Pure deterministic pseudo-random via md5. No secrets, just vibes."""
    return int(hashlib.md5(seed.encode()).hexdigest(), 16) % mod


def generate_date(seed: int, year_start: int = 2020, year_end: int = 2025) -> str:
    """Generate a deterministic ISO date string."""
    year = year_start + (seed % (year_end - year_start + 1))
    month = 1 + (seed // 7 % 12)
    day = 1 + (seed // 13 % 28)
    return f"{year:04d}-{month:02d}-{day:02d}"


def generate_employee(i: int) -> dict:
    """Generate one employee doc. Each one is a unique, deterministic snowflake."""
    h = deterministic_hash(f"emp-{i}", 10**9)
    first = FIRST_NAMES[i % len(FIRST_NAMES)]
    last = LAST_NAMES[(i * 7 + 3) % len(LAST_NAMES)]
    dept = DEPARTMENTS[i % len(DEPARTMENTS)]
    city, state, country = CITIES[i % len(CITIES)]
    num_skills = 2 + (i % 5)
    skills = [SKILLS[(i + s * 3) % len(SKILLS)] for s in range(num_skills)]

    return {
        "employee_id": f"EMP-{i:06d}",
        "first_name": first,
        "last_name": last,
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "department": dept,
        "title": TITLES[i % len(TITLES)],
        "hire_date": generate_date(i + 1000, 2015, 2025),
        "salary": 50000 + (h % 150000),
        "skills": skills,
        "location": {"city": city, "state": state, "country": country},
        "is_active": (i % 10) != 0,
    }


def generate_learning_record(i: int) -> dict:
    """Generate one learning record. Mandatory compliance training, naturally."""
    h = deterministic_hash(f"learn-{i}", 10**9)
    return {
        "record_id": f"LRN-{i:06d}",
        "employee_id": f"EMP-{i % 10000:06d}",
        "course_name": COURSES[i % len(COURSES)],
        "category": COURSE_CATEGORIES[i % len(COURSE_CATEGORIES)],
        "score": round(60.0 + (h % 4000) / 100.0, 2),
        "completion_date": generate_date(i + 2000),
        "duration_minutes": 15 + (h % 480),
        "status": ["completed", "in_progress", "not_started"][i % 3],
        "provider": COURSE_PROVIDERS[i % len(COURSE_PROVIDERS)],
    }


def generate_performance_review(i: int) -> dict:
    """Generate one performance review. Everyone 'meets expectations'."""
    h = deterministic_hash(f"review-{i}", 10**9)
    emp_id = i % 10000
    reviewer_id = (emp_id + 1 + (i // 10000)) % 10000
    periods = ["2023-H1", "2023-H2", "2024-H1", "2024-H2", "2025-H1"]
    return {
        "review_id": f"REV-{i:06d}",
        "employee_id": f"EMP-{emp_id:06d}",
        "reviewer_id": f"EMP-{reviewer_id:06d}",
        "review_period": periods[i % len(periods)],
        "rating": round(1.0 + (h % 400) / 100.0, 2),
        "goals_met": h % 6,
        "goals_total": 5,
        "comments": REVIEW_COMMENTS[i % len(REVIEW_COMMENTS)],
        "review_date": generate_date(i + 3000),
        "status": ["draft", "submitted", "acknowledged"][i % 3],
    }


def generate_job_posting(i: int) -> dict:
    """Generate one job posting. Requirements: 10 years in a 3-year-old framework."""
    h = deterministic_hash(f"job-{i}", 10**9)
    salary_min = 60000 + (h % 100000)
    return {
        "posting_id": f"JOB-{i:06d}",
        "title": TITLES[i % len(TITLES)],
        "department": DEPARTMENTS[i % len(DEPARTMENTS)],
        "salary_min": salary_min,
        "salary_max": salary_min + 20000 + (h % 30000),
        "posted_date": generate_date(i + 4000, 2024, 2025),
        "closing_date": generate_date(i + 4100, 2025, 2026),
        "status": ["open", "closed", "draft", "filled"][i % 4],
        "location": CITIES[i % len(CITIES)][0],
        "description": JOB_DESCRIPTIONS[i % len(JOB_DESCRIPTIONS)],
        "requirements": f"Required: {SKILLS[i % len(SKILLS)]}, {SKILLS[(i+1) % len(SKILLS)]}. "
                        f"Nice to have: {SKILLS[(i+2) % len(SKILLS)]}. "
                        f"Minimum {3 + (i % 8)} years experience.",
    }


def generate_search_event(i: int) -> dict:
    """Generate one search analytics event. Most popular query: 'how to quit'."""
    h = deterministic_hash(f"search-{i}", 10**9)
    ts_base = 1704067200  # 2024-01-01 UTC
    return {
        "event_id": f"EVT-{i:07d}",
        "query_text": SEARCH_QUERIES[i % len(SEARCH_QUERIES)],
        "user_id": f"USR-{i % 10000:06d}",
        "timestamp": f"{generate_date(i + 5000, 2024, 2025)}T{(i % 24):02d}:{(i*7 % 60):02d}:00Z",
        "result_count": h % 500,
        "response_time_ms": round(5.0 + (h % 5000) / 10.0, 1),
        "clicked_result": f"doc-{h % 1000}" if i % 3 != 0 else None,
        "search_type": SEARCH_TYPES[i % len(SEARCH_TYPES)],
        "filters_used": [DEPARTMENTS[i % len(DEPARTMENTS)]] if i % 4 == 0 else [],
    }


# Generator dispatch table
GENERATORS = {
    "employees": generate_employee,
    "learning_records": generate_learning_record,
    "performance_reviews": generate_performance_review,
    "job_postings": generate_job_posting,
    "search_analytics": generate_search_event,
}


def create_index(url: str, index_name: str, mapping_path: Path) -> bool:
    """Create an index with mapping. Deletes first for idempotent seeding. Uses shared utils."""
    # -- 🗑️ Delete if exists (idempotent seeding — like Ctrl+Z for your index)
    requests.delete(f"{url}/{index_name}", timeout=10)

    with open(mapping_path) as f:
        mapping = json.load(f)

    if not shared_create_index(url, index_name, mapping=mapping, refresh="1s"):
        console.print(f"[red]💀 Failed to create {index_name} on {url}[/red]")
        return False
    return True


def bulk_load(url: str, index_name: str, docs: list[dict]) -> int:
    """Bulk-load docs into an index. Returns number of successful docs."""
    if not docs:
        return 0

    # Build NDJSON bulk payload
    lines = []
    for doc in docs:
        lines.append(json.dumps({"index": {"_index": index_name}}))
        lines.append(json.dumps(doc))
    body = "\n".join(lines) + "\n"

    resp = requests.post(
        f"{url}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )

    if resp.status_code != 200:
        console.print(f"[red]💀 Bulk load failed for {index_name}: {resp.status_code}[/red]")
        return 0

    result = resp.json()
    if result.get("errors"):
        error_count = sum(1 for item in result["items"] if "error" in item.get("index", {}))
        return len(docs) - error_count
    return len(docs)


def get_doc_count(url: str, index_name: str) -> int:
    """Delegates to shared.kvx_utils — the eternal dance of eventual consistency."""
    return shared_get_doc_count(url, index_name)


def seed_index(index_name: str, doc_count: int, progress: Progress, task_id) -> dict:
    """Seed a single index on ES and create empty target on OS."""
    mapping_path = MAPPINGS_DIR / f"{index_name}.json"
    generator = GENERATORS[index_name]

    # Create index on ES (source) and OS (empty target)
    if not create_index(ES_URL, index_name, mapping_path):
        return {"index": index_name, "status": "failed", "docs": 0}

    if not create_index(OS_URL, index_name, mapping_path):
        return {"index": index_name, "status": "failed", "docs": 0}

    # Generate and bulk-load in batches
    total_loaded = 0
    for batch_start in range(0, doc_count, BULK_BATCH_SIZE):
        batch_end = min(batch_start + BULK_BATCH_SIZE, doc_count)
        batch = [generator(i) for i in range(batch_start, batch_end)]
        loaded = bulk_load(ES_URL, index_name, batch)
        total_loaded += loaded
        progress.update(task_id, advance=len(batch))

    # Verify
    actual_count = get_doc_count(ES_URL, index_name)
    os_count = get_doc_count(OS_URL, index_name)

    return {
        "index": index_name,
        "status": "ok" if actual_count == doc_count else "partial",
        "expected": doc_count,
        "es_actual": actual_count,
        "os_actual": os_count,
    }


def main():
    console.print("\n[bold cyan]🌱 Kravex Demo — Seeding Phase[/bold cyan]")
    console.print(f"[dim]ES: {ES_URL} | OS: {OS_URL}[/dim]\n")

    # Verify cluster connectivity
    for name, url in [("Elasticsearch", ES_URL), ("OpenSearch", OS_URL)]:
        try:
            resp = requests.get(f"{url}/_cluster/health", timeout=5)
            status = resp.json().get("status", "unknown")
            color = {"green": "green", "yellow": "yellow", "red": "red"}.get(status, "white")
            console.print(f"  [{color}]✅ {name}: cluster status = {status}[/{color}]")
        except Exception as e:
            console.print(f"  [red]💀 {name} not reachable: {e}[/red]")
            sys.exit(1)

    console.print()

    results = []
    total_docs = sum(count for _, count in INDICES)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for index_name, doc_count in INDICES:
            task_id = progress.add_task(
                f"[cyan]{index_name}[/cyan] ({doc_count:,} docs)",
                total=doc_count,
            )
            result = seed_index(index_name, doc_count, progress, task_id)
            results.append(result)

    # Summary table
    console.print("\n[bold]📊 Seeding Summary[/bold]\n")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Index", style="cyan")
    table.add_column("Expected", justify="right")
    table.add_column("ES Actual", justify="right")
    table.add_column("OS (empty)", justify="right")
    table.add_column("Status", justify="center")

    all_ok = True
    for r in results:
        status_icon = "✅" if r.get("status") == "ok" else "⚠️"
        if r.get("status") != "ok":
            all_ok = False
        table.add_row(
            r["index"],
            f"{r.get('expected', 0):,}",
            f"{r.get('es_actual', 0):,}",
            f"{r.get('os_actual', 0):,}",
            status_icon,
        )

    console.print(table)
    console.print(f"\n[bold]Total docs seeded: {total_docs:,}[/bold]")

    if not all_ok:
        console.print("[yellow]⚠️  Some indices had issues — check above for details[/yellow]")
        sys.exit(1)
    else:
        console.print("[green]✅ All indices seeded successfully![/green]\n")


if __name__ == "__main__":
    main()
