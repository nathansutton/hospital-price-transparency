#!/usr/bin/env python3
"""Generate summary by scanning actual data files on disk.

Compares hospital URL configs against data/{STATE}/{CCN}.jsonl files
to compute accurate success/failure counts.

Usage:
    python scripts/generate_summary.py
"""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import click

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# Valid US state codes
VALID_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "PR",
    "VI",
    "GU",
    "AS",
    "MP",  # Territories
}


def load_url_configs(urls_dir: Path) -> dict[str, list[dict]]:
    """Load all hospital URL configs grouped by state.

    Args:
        urls_dir: Path to dim/urls directory

    Returns:
        Dict mapping state code (uppercase) to list of hospital configs
    """
    configs_by_state: dict[str, list[dict]] = {}

    for json_file in urls_dir.glob("*.json"):
        state = json_file.stem.upper()
        # Skip non-state files (e.g., needs_review.json)
        if state not in VALID_STATES:
            continue
        try:
            with open(json_file) as f:
                hospitals = json.load(f)
                configs_by_state[state] = hospitals
        except Exception as e:
            click.echo(f"Warning: Could not read {json_file}: {e}")
            continue

    return configs_by_state


def count_jsonl_records(file_path: Path) -> int:
    """Count number of records in a JSONL file.

    Args:
        file_path: Path to .jsonl file

    Returns:
        Number of lines (records) in the file
    """
    try:
        with open(file_path) as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


def scan_data_files(data_dir: Path, state: str) -> dict[str, dict]:
    """Scan data directory for a state and return file info by CCN.

    Args:
        data_dir: Path to data directory
        state: Two-letter state code

    Returns:
        Dict mapping CCN to {path, records, mtime}
    """
    state_dir = data_dir / state.upper()
    files_by_ccn: dict[str, dict] = {}

    if not state_dir.exists():
        return files_by_ccn

    for jsonl_file in state_dir.glob("*.jsonl"):
        ccn = jsonl_file.stem
        records = count_jsonl_records(jsonl_file)
        mtime = datetime.fromtimestamp(jsonl_file.stat().st_mtime, tz=UTC)

        files_by_ccn[ccn] = {
            "path": jsonl_file,
            "records": records,
            "mtime": mtime,
        }

    return files_by_ccn


def compute_state_status(
    state: str,
    url_configs: list[dict],
    data_files: dict[str, dict],
) -> tuple[dict, list[dict]]:
    """Compute status for a state by comparing configs to data files.

    Args:
        state: Two-letter state code
        url_configs: List of hospital URL configs
        data_files: Dict of CCN -> file info from scan_data_files

    Returns:
        Tuple of (summary_dict, list of per-hospital status dicts)
    """
    rows = []
    success = 0
    failed = 0
    total_records = 0

    for config in url_configs:
        ccn = config.get("ccn", "")
        hospital_name = config.get("hospital_name", "")
        file_url = config.get("file_url", "")

        if ccn in data_files:
            file_info = data_files[ccn]
            records = file_info["records"]

            if records > 0:
                status = "SUCCESS"
                success += 1
                total_records += records
            else:
                # File exists but empty
                status = "FAILURE"
                failed += 1
        else:
            # No data file found
            status = "FAILURE"
            failed += 1
            records = 0

        rows.append(
            {
                "ccn": ccn,
                "hospital": hospital_name,
                "status": status,
                "file_url": file_url,
                "records": records if records > 0 else "",
            }
        )

    total = len(url_configs)
    success_rate = (success / total * 100) if total > 0 else 0.0

    # Find most recent file modification
    mtimes = [f["mtime"] for f in data_files.values()]
    last_updated = max(mtimes).isoformat() if mtimes else ""

    summary = {
        "state": state,
        "total": total,
        "success": success,
        "failed": failed,
        "skipped": 0,  # No skipped in data-driven status
        "success_rate": f"{success_rate:.1f}%",
        "records": total_records,
        "last_updated": last_updated,
    }

    return summary, rows


def write_state_csv(status_dir: Path, state: str, rows: list[dict]) -> Path:
    """Write per-state status CSV file.

    Args:
        status_dir: Path to status directory
        state: Two-letter state code
        rows: List of per-hospital status dicts

    Returns:
        Path to written file
    """
    import csv

    status_file = status_dir / f"{state.upper()}.csv"

    fieldnames = ["ccn", "hospital", "status", "file_url", "records"]

    with open(status_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return status_file


def write_summary_csv(status_dir: Path, summaries: list[dict]) -> Path:
    """Write summary.csv with per-state statistics.

    Args:
        status_dir: Path to status directory
        summaries: List of state summary dicts

    Returns:
        Path to written file
    """
    import csv

    summary_file = status_dir / "summary.csv"

    # Sort by state code
    summaries = sorted(summaries, key=lambda s: s["state"])

    fieldnames = [
        "state",
        "total",
        "success",
        "failed",
        "skipped",
        "success_rate",
        "records",
        "last_updated",
    ]

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
    "--urls-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to URL configs directory (default: PROJECT_ROOT/dim/urls)",
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Path to data directory (default: PROJECT_ROOT/data)",
)
@click.option(
    "--status-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to status directory (default: PROJECT_ROOT/status)",
)
@click.option(
    "--write-state-files",
    is_flag=True,
    help="Also write per-state status CSV files",
)
def main(
    urls_dir: Path | None,
    data_dir: Path | None,
    status_dir: Path | None,
    write_state_files: bool,
) -> None:
    """Generate summary by scanning data files on disk.

    Compares hospital URL configs against actual data files to compute
    accurate success/failure statistics.
    """
    if urls_dir is None:
        urls_dir = project_root / "dim" / "urls"
    if data_dir is None:
        data_dir = project_root / "data"
    if status_dir is None:
        status_dir = project_root / "status"

    status_dir.mkdir(parents=True, exist_ok=True)

    click.echo(f"URL configs: {urls_dir}")
    click.echo(f"Data files:  {data_dir}")
    click.echo(f"Status dir:  {status_dir}")
    click.echo()

    # Load all URL configs
    configs_by_state = load_url_configs(urls_dir)

    if not configs_by_state:
        click.echo("Error: No URL config files found.")
        sys.exit(1)

    click.echo(f"Found {len(configs_by_state)} states with URL configs")

    # Compute status for each state
    summaries = []
    all_rows_by_state: dict[str, list[dict]] = {}

    for state, url_configs in sorted(configs_by_state.items()):
        data_files = scan_data_files(data_dir, state)
        summary, rows = compute_state_status(state, url_configs, data_files)
        summaries.append(summary)
        all_rows_by_state[state] = rows

    # Write summary.csv
    summary_file = write_summary_csv(status_dir, summaries)
    click.echo(f"Wrote: {summary_file}")

    # Write badge.json
    badge_file = write_badge_json(status_dir, summaries)
    click.echo(f"Wrote: {badge_file}")

    # Optionally write per-state files
    if write_state_files:
        click.echo("\nWriting per-state status files...")
        for state, rows in all_rows_by_state.items():
            state_file = write_state_csv(status_dir, state, rows)
            click.echo(f"  Wrote: {state_file}")

    # Print summary table
    click.echo("\n" + "=" * 70)
    click.echo("Summary by State:")
    click.echo("-" * 70)
    click.echo(
        f"{'State':<8} {'Total':>8} {'Success':>8} {'Failed':>8} {'Rate':>10} {'Records':>12}"
    )
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
