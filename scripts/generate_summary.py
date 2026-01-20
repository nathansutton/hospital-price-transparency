#!/usr/bin/env python3
"""Generate summary from per-state status files.

Aggregates all status/{STATE}.csv files into summary statistics:
- status/summary.csv: Per-state aggregate statistics
- status/badge.json: Shields.io endpoint badge data

Usage:
    python scripts/generate_summary.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import click

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def parse_status_files(status_dir: Path) -> dict[str, list[dict]]:
    """Read all state status files and return data grouped by state.

    Args:
        status_dir: Path to status directory

    Returns:
        Dict mapping state code to list of row dicts
    """
    data_by_state: dict[str, list[dict]] = {}

    for status_file in status_dir.glob("*.csv"):
        # Skip summary file
        if status_file.stem.lower() == "summary":
            continue

        state = status_file.stem.upper()

        try:
            with open(status_file, newline="") as f:
                reader = csv.DictReader(f)
                data_by_state[state] = list(reader)
        except Exception as e:
            click.echo(f"Warning: Could not read {status_file}: {e}")
            continue

    return data_by_state


def compute_state_summary(state: str, rows: list[dict]) -> dict:
    """Compute summary statistics for a single state.

    Args:
        state: Two-letter state code
        rows: List of status row dicts

    Returns:
        Summary dict with aggregate statistics
    """
    total = len(rows)
    success = sum(1 for r in rows if r.get("status") == "SUCCESS")
    failed = sum(1 for r in rows if r.get("status") == "FAILURE")
    skipped = sum(1 for r in rows if r.get("status") == "SKIPPED")

    # Sum records (handle empty strings)
    records = sum(int(r.get("records") or 0) for r in rows)

    # Find most recent date
    dates = [r.get("date") for r in rows if r.get("date")]
    last_run = max(dates) if dates else ""

    # Calculate success rate
    success_rate = (success / total * 100) if total > 0 else 0.0

    return {
        "state": state,
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": skipped,
        "success_rate": f"{success_rate:.1f}%",
        "records": records,
        "last_run": last_run,
    }


def write_summary_csv(status_dir: Path, summaries: list[dict]) -> Path:
    """Write summary.csv with per-state statistics.

    Args:
        status_dir: Path to status directory
        summaries: List of state summary dicts

    Returns:
        Path to written file
    """
    summary_file = status_dir / "summary.csv"

    # Sort by state code
    summaries = sorted(summaries, key=lambda s: s["state"])

    fieldnames = ["state", "total", "success", "failed", "skipped", "success_rate", "records", "last_run"]

    with open(summary_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summaries)

    return summary_file


def write_badge_json(status_dir: Path, summaries: list[dict]) -> Path:
    """Write badge.json for shields.io endpoint badge.

    Args:
        status_dir: Path to status directory
        summaries: List of state summary dicts

    Returns:
        Path to written file
    """
    badge_file = status_dir / "badge.json"

    # Compute totals
    total_hospitals = sum(s["total"] for s in summaries)
    total_success = sum(s["success"] for s in summaries)
    total_records = sum(s["records"] for s in summaries)

    overall_rate = (total_success / total_hospitals * 100) if total_hospitals > 0 else 0.0

    # Determine color based on success rate
    if overall_rate >= 90:
        color = "brightgreen"
    elif overall_rate >= 75:
        color = "green"
    elif overall_rate >= 50:
        color = "yellow"
    else:
        color = "red"

    badge_data = {
        "schemaVersion": 1,
        "label": "hospitals scraped",
        "message": f"{total_success}/{total_hospitals} ({overall_rate:.0f}%)",
        "color": color,
        "namedLogo": "data",
        "cacheSeconds": 3600,
    }

    with open(badge_file, "w") as f:
        json.dump(badge_data, f, indent=2)

    return badge_file


@click.command()
@click.option(
    "--status-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to status directory (default: PROJECT_ROOT/status)",
)
def main(status_dir: Path | None) -> None:
    """Generate summary from per-state status files."""
    if status_dir is None:
        status_dir = project_root / "status"

    if not status_dir.exists():
        click.echo(f"Error: Status directory not found: {status_dir}")
        click.echo("Run the scraper first to generate status files.")
        sys.exit(1)

    click.echo(f"Reading status files from: {status_dir}")

    # Parse all state status files
    data_by_state = parse_status_files(status_dir)

    if not data_by_state:
        click.echo("Error: No state status files found.")
        sys.exit(1)

    click.echo(f"Found {len(data_by_state)} states")

    # Compute summaries
    summaries = [compute_state_summary(state, rows) for state, rows in data_by_state.items()]

    # Write summary.csv
    summary_file = write_summary_csv(status_dir, summaries)
    click.echo(f"Wrote: {summary_file}")

    # Write badge.json
    badge_file = write_badge_json(status_dir, summaries)
    click.echo(f"Wrote: {badge_file}")

    # Print summary table
    click.echo("\n" + "=" * 70)
    click.echo("Summary by State:")
    click.echo("-" * 70)
    click.echo(f"{'State':<8} {'Total':>8} {'Success':>8} {'Failed':>8} {'Rate':>10} {'Records':>12}")
    click.echo("-" * 70)

    total_hospitals = 0
    total_success = 0
    total_failed = 0
    total_records = 0

    for s in sorted(summaries, key=lambda x: x["state"]):
        click.echo(
            f"{s['state']:<8} {s['total']:>8} {s['success']:>8} {s['failed']:>8} "
            f"{s['success_rate']:>10} {s['records']:>12,}"
        )
        total_hospitals += s["total"]
        total_success += s["success"]
        total_failed += s["failed"]
        total_records += s["records"]

    click.echo("-" * 70)
    overall_rate = (total_success / total_hospitals * 100) if total_hospitals > 0 else 0.0
    click.echo(
        f"{'TOTAL':<8} {total_hospitals:>8} {total_success:>8} {total_failed:>8} "
        f"{overall_rate:>9.1f}% {total_records:>12,}"
    )
    click.echo("=" * 70)


if __name__ == "__main__":
    main()
