#!/usr/bin/env python3
"""
Analyze scrape failures from status CSV files.

Groups failures by error type and URL domain to enable smart issue creation
and batch remediation.
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterator
from urllib.parse import urlparse


@dataclass
class Failure:
    """Individual scrape failure record."""
    ccn: str
    hospital: str
    state: str
    file_url: str
    error_type: str
    error_message: str
    domain: str


@dataclass
class FailureGroup:
    """Group of related failures for issue creation."""
    group_id: str
    error_type: str
    domain: str
    category: str  # url-update, url-blocked, registry-update, encoding-fix, needs-human
    confidence: float
    failures: list[Failure]

    @property
    def ccns(self) -> list[str]:
        return [f.ccn for f in self.failures]

    @property
    def states(self) -> set[str]:
        return {f.state for f in self.failures}

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "group_id": self.group_id,
            "error_type": self.error_type,
            "domain": self.domain,
            "category": self.category,
            "confidence": self.confidence,
            "count": len(self.failures),
            "ccns": self.ccns,
            "states": list(self.states),
            "failures": [asdict(f) for f in self.failures],
        }


def extract_domain(url: str) -> str:
    """Extract domain from URL, handling edge cases."""
    if not url:
        return "unknown"
    try:
        parsed = urlparse(url)
        return parsed.netloc or "unknown"
    except Exception:
        return "unknown"


def categorize_failure(error_type: str, error_message: str, file_url: str) -> tuple[str, float]:
    """
    Categorize a failure and assign fix confidence.

    Returns:
        (category, confidence) tuple
    """
    error_type = error_type or ""
    error_message = error_message or ""

    # HTTP 404 - URL moved, high confidence for auto-fix
    if "404" in error_type or "404" in error_message:
        return ("url-update", 0.8)

    # HTTP 403 - Access blocked, medium confidence (may need headers/auth)
    if "403" in error_type or "403" in error_message:
        return ("url-blocked", 0.5)

    # No scraper registered - need registry update
    if "No scraper" in error_message or error_type == "SKIPPED":
        # Check if URL pattern suggests a known format
        if any(pattern in file_url for pattern in [".csv", ".json", ".xlsx", ".zip"]):
            return ("registry-update", 0.7)
        return ("registry-update", 0.4)

    # Encoding issues - often fixable
    if "UnicodeDecodeError" in error_type or "UnicodeDecodeError" in error_message:
        return ("encoding-fix", 0.6)

    # Empty data - provider issue
    if "EmptyDataError" in error_type:
        return ("needs-human", 0.3)

    # SSL errors - sometimes fixable with verify=False
    if "SSL" in error_message or "certificate" in error_message.lower():
        return ("url-blocked", 0.4)

    # Connection errors - transient or provider issue
    if "Connection" in error_message:
        return ("needs-human", 0.2)

    # Default to needs-human for unknown errors
    return ("needs-human", 0.3)


def parse_status_csv(csv_path: Path) -> Iterator[Failure]:
    """Parse a status CSV file and yield failure records."""
    state = csv_path.stem.upper()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            status = row.get("status", "")
            if status not in ("FAILURE", "SKIPPED"):
                continue

            file_url = row.get("file_url", "")
            yield Failure(
                ccn=row.get("ccn", ""),
                hospital=row.get("hospital", ""),
                state=state,
                file_url=file_url,
                error_type=row.get("error_type", ""),
                error_message=row.get("error_message", ""),
                domain=extract_domain(file_url),
            )


def group_failures(failures: list[Failure]) -> list[FailureGroup]:
    """Group failures by error type and domain."""
    # Group by (error_type, domain)
    groups: dict[tuple[str, str], list[Failure]] = defaultdict(list)

    for failure in failures:
        # Normalize error type for grouping
        error_type = failure.error_type or "UNKNOWN"
        if "404" in error_type:
            error_type = "HTTP_404"
        elif "403" in error_type:
            error_type = "HTTP_403"

        key = (error_type, failure.domain)
        groups[key].append(failure)

    # Convert to FailureGroup objects
    result = []
    for (error_type, domain), failures in groups.items():
        # Use first failure to determine category
        sample = failures[0]
        category, confidence = categorize_failure(
            sample.error_type,
            sample.error_message,
            sample.file_url
        )

        group_id = f"{error_type}:{domain}".replace(".", "-").replace("/", "-")

        result.append(FailureGroup(
            group_id=group_id,
            error_type=error_type,
            domain=domain,
            category=category,
            confidence=confidence,
            failures=failures,
        ))

    # Sort by count (descending) then category
    result.sort(key=lambda g: (-len(g.failures), g.category))

    return result


def analyze_all_states(status_dir: Path) -> list[FailureGroup]:
    """Analyze all status CSV files in the directory."""
    all_failures = []

    for csv_path in status_dir.glob("*.csv"):
        # Skip summary.csv
        if csv_path.name == "summary.csv":
            continue

        for failure in parse_status_csv(csv_path):
            all_failures.append(failure)

    return group_failures(all_failures)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze scrape failures from status CSV files"
    )
    parser.add_argument(
        "--status-dir",
        type=Path,
        default=Path("status"),
        help="Directory containing status CSV files",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Output JSON file (default: stdout)",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence threshold for output",
    )
    parser.add_argument(
        "--category",
        choices=["url-update", "url-blocked", "registry-update", "encoding-fix", "needs-human"],
        help="Filter by category",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print summary statistics",
    )

    args = parser.parse_args()

    if not args.status_dir.exists():
        print(f"Error: Status directory not found: {args.status_dir}", file=sys.stderr)
        sys.exit(1)

    groups = analyze_all_states(args.status_dir)

    # Apply filters
    if args.min_confidence > 0:
        groups = [g for g in groups if g.confidence >= args.min_confidence]

    if args.category:
        groups = [g for g in groups if g.category == args.category]

    if args.summary:
        # Print summary statistics
        total_failures = sum(len(g.failures) for g in groups)
        by_category = defaultdict(int)
        for g in groups:
            by_category[g.category] += len(g.failures)

        print("Failure Analysis Summary")
        print("=" * 40)
        print(f"Total failure groups: {len(groups)}")
        print(f"Total individual failures: {total_failures}")
        print()
        print("By Category:")
        for cat, count in sorted(by_category.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}")
        print()
        print("Top 10 Failure Groups:")
        for g in groups[:10]:
            print(f"  [{g.category}] {g.error_type} @ {g.domain}: {len(g.failures)} failures")
        return

    # Output JSON
    output_data = {
        "total_groups": len(groups),
        "total_failures": sum(len(g.failures) for g in groups),
        "groups": [g.to_dict() for g in groups],
    }

    if args.output:
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"Wrote {len(groups)} failure groups to {args.output}", file=sys.stderr)
    else:
        print(json.dumps(output_data, indent=2))


if __name__ == "__main__":
    main()
