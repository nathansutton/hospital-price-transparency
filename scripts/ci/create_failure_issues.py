#!/usr/bin/env python3
"""
Create GitHub issues for scrape failures.

Uses gh CLI to create/update issues based on failure analysis.
Handles deduplication by searching for existing issues with matching group_id.
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def run_gh_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a gh CLI command."""
    cmd = ["gh"] + args
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def search_existing_issue(group_id: str, repo: str | None = None) -> int | None:
    """Search for an existing open issue with the given group_id."""
    search_query = f'is:issue is:open "[{group_id}]" in:title'
    args = ["issue", "list", "--search", search_query, "--json", "number", "--limit", "1"]
    if repo:
        args.extend(["--repo", repo])

    result = run_gh_command(args, check=False)
    if result.returncode != 0:
        return None

    try:
        issues = json.loads(result.stdout)
        if issues:
            return issues[0]["number"]
    except (json.JSONDecodeError, KeyError, IndexError):
        pass

    return None


def create_issue(
    title: str,
    body: str,
    labels: list[str],
    repo: str | None = None,
) -> int | None:
    """Create a new GitHub issue."""
    args = ["issue", "create", "--title", title, "--body", body]
    for label in labels:
        args.extend(["--label", label])
    if repo:
        args.extend(["--repo", repo])

    result = run_gh_command(args, check=False)
    if result.returncode != 0:
        print(f"Error creating issue: {result.stderr}", file=sys.stderr)
        return None

    # Parse issue number from output (e.g., "https://github.com/owner/repo/issues/123")
    try:
        url = result.stdout.strip()
        return int(url.split("/")[-1])
    except (ValueError, IndexError):
        return None


def add_issue_comment(issue_number: int, body: str, repo: str | None = None) -> bool:
    """Add a comment to an existing issue."""
    args = ["issue", "comment", str(issue_number), "--body", body]
    if repo:
        args.extend(["--repo", repo])

    result = run_gh_command(args, check=False)
    return result.returncode == 0


def format_issue_title(group: dict) -> str:
    """Format the issue title for a failure group."""
    category = group["category"]
    domain = group["domain"]
    count = group["count"]
    group_id = group["group_id"]

    # Short, scannable title
    return f"[{group_id}] {category}: {count} failures at {domain}"


def format_issue_body(group: dict) -> str:
    """Format the issue body for a failure group."""
    category = group["category"]
    error_type = group["error_type"]
    domain = group["domain"]
    confidence = group["confidence"]
    failures = group["failures"]
    states = group["states"]

    # Determine if auto-fixable
    auto_fixable = confidence >= 0.7 and category in ("url-update", "registry-update")

    body = f"""## Scrape Failure Report

**Category:** `{category}`
**Error Type:** `{error_type}`
**Domain:** `{domain}`
**Affected States:** {', '.join(sorted(states))}
**Auto-Fix Confidence:** {confidence:.0%}
**Auto-Fixable:** {'Yes' if auto_fixable else 'No'}

### Affected Hospitals ({len(failures)})

| CCN | Hospital | State | Error Message |
|-----|----------|-------|---------------|
"""

    # Add table rows (limit to 20 for readability)
    for f in failures[:20]:
        error_msg = f.get("error_message", "")[:50]
        if len(f.get("error_message", "")) > 50:
            error_msg += "..."
        body += f"| {f['ccn']} | {f['hospital'][:40]} | {f['state']} | {error_msg} |\n"

    if len(failures) > 20:
        body += f"\n*...and {len(failures) - 20} more*\n"

    # Add example URLs
    body += "\n### Sample URLs\n\n"
    seen_urls = set()
    for f in failures[:5]:
        url = f.get("file_url", "")
        if url and url not in seen_urls:
            body += f"- `{url}`\n"
            seen_urls.add(url)

    # Add fix guidance based on category
    body += "\n### Suggested Action\n\n"

    if category == "url-update":
        body += """This appears to be a URL change (HTTP 404). The automated system will:
1. Search the hospital's transparency page for the new URL
2. Update `dim/urls/{state}.json` with the correct URL
3. Create a PR for review

**To trigger auto-fix:** Add the `url-update` label.
"""
    elif category == "url-blocked":
        body += """This appears to be an access restriction (HTTP 403). Possible causes:
- IP-based blocking in CI environment
- User-agent restrictions
- Rate limiting

**Manual investigation needed** to determine if headers or authentication are required.
"""
    elif category == "registry-update":
        body += """No scraper is registered for this URL pattern. The automated system will:
1. Analyze the URL to determine the file format
2. Update `src/scrapers/registry.py` with the correct scraper mapping
3. Create a PR for review

**To trigger auto-fix:** Add the `registry-update` label.
"""
    elif category == "encoding-fix":
        body += """This appears to be a character encoding issue. Possible fixes:
- Update scraper to handle multiple encodings
- Add specific encoding detection for this domain

**To trigger auto-fix:** Add the `encoding-fix` label.
"""
    else:
        body += """This failure requires manual investigation. The error pattern does not
match known auto-fixable categories.

Please investigate the root cause and propose a fix manually.
"""

    body += "\n---\n*This issue was auto-generated by the failure detection system.*"

    return body


def get_labels_for_category(category: str) -> list[str]:
    """Get the appropriate labels for a failure category."""
    base_labels = ["scrape-failure", "ai-triage"]

    category_labels = {
        "url-update": ["url-update"],
        "url-blocked": ["needs-investigation"],
        "registry-update": ["registry-update"],
        "encoding-fix": ["encoding-fix"],
        "needs-human": ["needs-human"],
    }

    return base_labels + category_labels.get(category, [])


def main():
    parser = argparse.ArgumentParser(
        description="Create GitHub issues for scrape failures"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input JSON file from analyze_failures.py",
    )
    parser.add_argument(
        "--repo",
        help="GitHub repository (owner/repo). Uses current repo if not specified.",
    )
    parser.add_argument(
        "--min-failures",
        type=int,
        default=1,
        help="Minimum failures per group to create an issue",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.0,
        help="Minimum confidence threshold",
    )
    parser.add_argument(
        "--category",
        choices=["url-update", "url-blocked", "registry-update", "encoding-fix", "needs-human"],
        help="Only create issues for this category",
    )
    parser.add_argument(
        "--max-issues",
        type=int,
        default=10,
        help="Maximum number of new issues to create",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without creating issues",
    )
    parser.add_argument(
        "--auto-fixable-only",
        action="store_true",
        help="Only create issues for auto-fixable failures (confidence >= 0.7)",
    )

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    with open(args.input) as f:
        data = json.load(f)

    groups = data.get("groups", [])

    # Apply filters
    if args.min_failures > 1:
        groups = [g for g in groups if g["count"] >= args.min_failures]

    if args.min_confidence > 0:
        groups = [g for g in groups if g["confidence"] >= args.min_confidence]

    if args.category:
        groups = [g for g in groups if g["category"] == args.category]

    if args.auto_fixable_only:
        groups = [
            g for g in groups
            if g["confidence"] >= 0.7 and g["category"] in ("url-update", "registry-update")
        ]

    created = 0
    updated = 0
    skipped = 0

    for group in groups:
        if created >= args.max_issues:
            print(f"Reached max issues limit ({args.max_issues})", file=sys.stderr)
            break

        group_id = group["group_id"]
        title = format_issue_title(group)
        body = format_issue_body(group)
        labels = get_labels_for_category(group["category"])

        # Check for existing issue
        existing = search_existing_issue(group_id, args.repo)

        if args.dry_run:
            if existing:
                print(f"[DRY-RUN] Would update issue #{existing}: {title}")
            else:
                print(f"[DRY-RUN] Would create issue: {title}")
                print(f"          Labels: {', '.join(labels)}")
            continue

        if existing:
            # Add a comment to the existing issue with updated stats
            comment = f"## Updated Failure Report\n\n{body}"
            if add_issue_comment(existing, comment, args.repo):
                print(f"Updated issue #{existing}: {title}")
                updated += 1
            else:
                print(f"Failed to update issue #{existing}", file=sys.stderr)
                skipped += 1
        else:
            # Create new issue
            issue_num = create_issue(title, body, labels, args.repo)
            if issue_num:
                print(f"Created issue #{issue_num}: {title}")
                created += 1
            else:
                print(f"Failed to create issue: {title}", file=sys.stderr)
                skipped += 1

    print(f"\nSummary: {created} created, {updated} updated, {skipped} skipped", file=sys.stderr)


if __name__ == "__main__":
    main()
