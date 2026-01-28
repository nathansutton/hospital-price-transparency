#!/usr/bin/env python3
"""
Fix broken hospital URLs by scraping transparency pages for correct links.

This script:
1. Reads status files to find 404/403 failures
2. Looks up those CCNs in dim/urls/*.json
3. Scrapes each transparency page to find the correct file URL
4. Updates the URL files with the new URLs

Usage:
    uv run python scripts/fix_broken_urls.py [--dry-run] [--state XX] [--verbose]

    # Only fix specific error codes
    uv run python scripts/fix_broken_urls.py --error-codes 404

    # Output as JSON (for CI)
    uv run python scripts/fix_broken_urls.py --json
"""

import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


def find_failed_entries(
    status_dir: Path,
    urls_dir: Path,
    state_filter: str | None = None,
    error_codes: list[int] | None = None,
) -> list[dict]:
    """Find all entries that failed with 404/403 errors.

    Args:
        status_dir: Path to status directory containing state CSV files
        urls_dir: Path to urls directory containing URL JSON files
        state_filter: Optional state code to filter by
        error_codes: List of HTTP error codes to look for (default: [403, 404])

    Returns:
        List of entry dicts with file, state, index, ccn, hospital_name, old_url, transparency_page
    """
    if error_codes is None:
        error_codes = [403, 404]

    entries = []

    # Determine which states to process
    if state_filter:
        status_files = [status_dir / f"{state_filter.upper()}.csv"]
    else:
        status_files = list(status_dir.glob("*.csv"))

    # Collect failed CCNs from status files
    failed_ccns: dict[str, dict] = {}  # ccn -> {state, error_message, file_url}

    for status_file in status_files:
        if not status_file.exists():
            continue
        if status_file.name in ("summary.csv", "badge.json"):
            continue

        state = status_file.stem.upper()

        with open(status_file) as f:
            # Skip header
            f.readline()
            for line in f:
                if ",FAILURE," not in line:
                    continue
                if not any(f"Server returned {code}" in line for code in error_codes):
                    continue

                parts = line.strip().split(",")
                if len(parts) < 8:
                    continue

                ccn = parts[1]
                error_message = parts[7] if len(parts) > 7 else ""

                failed_ccns[ccn] = {
                    "state": state,
                    "error_message": error_message,
                }

    # Look up failed CCNs in urls files
    pattern = f"{state_filter.lower()}.json" if state_filter else "*.json"

    for url_file in urls_dir.glob(pattern):
        state = url_file.stem.upper()

        with open(url_file) as f:
            hospitals = json.load(f)

        for i, hospital in enumerate(hospitals):
            ccn = hospital.get("ccn", "")
            if ccn not in failed_ccns:
                continue
            if state_filter and failed_ccns[ccn]["state"] != state_filter.upper():
                continue

            entries.append({
                "file": url_file,
                "state": state,
                "index": i,
                "ccn": ccn,
                "hospital_name": hospital.get("hospital_name", ""),
                "old_url": hospital.get("file_url", ""),
                "transparency_page": hospital.get("transparency_page", ""),
                "error_message": failed_ccns[ccn]["error_message"],
            })

    return entries


def scrape_transparency_page(url: str, timeout: float = 30.0) -> str | None:
    """Scrape a transparency page to find a machine-readable file link.

    Looks for links containing common patterns:
    - standardcharges and .csv/.json
    - edge.sitecorecloud.io (Encompass CDN)
    - machine-readable

    Args:
        url: Transparency page URL to scrape
        timeout: Request timeout in seconds

    Returns:
        Found file URL or None
    """
    if not url:
        return None

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        found_links: list[tuple[str, int]] = []  # (url, priority)

        for link in soup.find_all("a", href=True):
            href = link["href"]
            href_lower = href.lower()

            # Handle relative URLs
            if href.startswith("/"):
                href = urljoin(url, href)
            elif not href.startswith("http"):
                href = urljoin(url, href)

            # Priority 1: standardcharges with csv/json extension
            if "standardcharges" in href_lower and (".csv" in href_lower or ".json" in href_lower):
                found_links.append((href, 1))
                continue

            # Priority 2: Sitecore CDN (Encompass)
            if "edge.sitecorecloud.io" in href_lower and (".csv" in href_lower or ".json" in href_lower):
                found_links.append((href, 2))
                continue

            # Priority 3: machine-readable patterns
            if "machine-readable" in href_lower:
                found_links.append((href, 3))
                continue

            # Priority 4: Any csv/json file that looks like pricing data
            if (".csv" in href_lower or ".json" in href_lower) and any(
                keyword in href_lower for keyword in ["price", "charge", "transparency", "cdm"]
            ):
                found_links.append((href, 4))

        # Return highest priority link
        if found_links:
            found_links.sort(key=lambda x: x[1])
            return found_links[0][0]

        return None

    except Exception as e:
        print(f"  Error scraping {url}: {e}")
        return None


def validate_url(url: str, timeout: float = 15.0) -> tuple[bool, int | None]:
    """Check if a URL is accessible.

    Args:
        url: URL to validate
        timeout: Request timeout in seconds

    Returns:
        Tuple of (is_valid, status_code)
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Range": "bytes=0-1024",  # Only fetch first 1KB
        }
        response = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=True)
        # 200 or 206 (partial content) both indicate success
        return response.status_code in (200, 206), response.status_code
    except Exception:
        return False, None


def update_url_file(url_file: Path, index: int, new_url: str, dry_run: bool = False) -> None:
    """Update a specific entry in a URL file.

    Args:
        url_file: Path to the JSON URL file
        index: Index of the entry to update
        new_url: New file_url value
        dry_run: If True, don't actually write
    """
    with open(url_file) as f:
        hospitals = json.load(f)

    hospitals[index]["file_url"] = new_url

    if not dry_run:
        with open(url_file, "w") as f:
            json.dump(hospitals, f, indent=2)
            f.write("\n")


def main():
    parser = argparse.ArgumentParser(description="Fix broken hospital URLs")
    parser.add_argument("--dry-run", action="store_true", help="Don't write changes")
    parser.add_argument("--state", type=str, help="Only process specific state")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--json", action="store_true", help="Output results as JSON (for CI)")
    parser.add_argument(
        "--error-codes",
        type=str,
        default="403,404",
        help="Comma-separated HTTP error codes to fix (default: 403,404)",
    )
    parser.add_argument(
        "--urls-dir",
        type=str,
        default=None,
        help="URLs directory (default: dim/urls)",
    )
    args = parser.parse_args()

    project_root = Path(__file__).parent.parent
    status_dir = project_root / "status"

    # Use urls by default, but allow override
    if args.urls_dir:
        urls_dir = Path(args.urls_dir)
    else:
        urls_dir = project_root / "dim" / "urls"

    # Parse error codes
    error_codes = [int(code.strip()) for code in args.error_codes.split(",")]

    results = {"fixed": [], "failed": [], "total": 0, "error_codes": error_codes}

    if not args.json:
        print(f"Finding entries with HTTP {error_codes} errors...")
        print(f"  Status dir: {status_dir}")
        print(f"  URLs dir: {urls_dir}")

    entries = find_failed_entries(status_dir, urls_dir, args.state, error_codes)
    results["total"] = len(entries)

    if not args.json:
        print(f"Found {len(entries)} entries to fix\n")

    for entry in entries:
        if not args.json:
            print(f"[{entry['state']}] {entry['hospital_name']} ({entry['ccn']})")
        if args.verbose and not args.json:
            print(f"  Old URL: {entry['old_url']}")
            print(f"  Error: {entry['error_message']}")
            print(f"  Transparency page: {entry['transparency_page']}")

        # Try to scrape the new URL from the transparency page
        new_url = scrape_transparency_page(entry["transparency_page"])

        result_entry = {
            "state": entry["state"],
            "ccn": entry["ccn"],
            "hospital_name": entry["hospital_name"],
            "old_url": entry["old_url"],
            "transparency_page": entry["transparency_page"],
            "error_message": entry["error_message"],
        }

        if new_url:
            # Check if new URL is different from old
            if new_url == entry["old_url"]:
                if not args.json:
                    print(f"  Found same URL (still broken): {new_url[:60]}...")
                result_entry["new_url"] = new_url
                result_entry["status"] = "same_url"
                result_entry["error"] = "Found URL matches broken URL"
                results["failed"].append(result_entry)
            else:
                if args.verbose and not args.json:
                    print(f"  Found URL: {new_url}")
                # Validate the new URL works
                is_valid, status_code = validate_url(new_url)
                if is_valid:
                    if not args.json:
                        print(f"  ✓ Validated! {new_url[:70]}...")
                    if not args.dry_run:
                        update_url_file(entry["file"], entry["index"], new_url)
                        if not args.json:
                            print("  Updated!")
                    else:
                        if not args.json:
                            print("  Would update (dry-run)")
                    result_entry["new_url"] = new_url
                    result_entry["status"] = "fixed"
                    results["fixed"].append(result_entry)
                else:
                    if not args.json:
                        print(f"  ✗ Found URL but validation failed (HTTP {status_code}): {new_url[:60]}...")
                    result_entry["new_url"] = new_url
                    result_entry["status"] = "validation_failed"
                    result_entry["error"] = f"URL validation failed (HTTP {status_code})"
                    results["failed"].append(result_entry)
        else:
            if not args.json:
                print("  ✗ Could not find machine-readable file link on transparency page")
            result_entry["status"] = "not_found"
            result_entry["error"] = "Could not find machine-readable file link"
            results["failed"].append(result_entry)

        # Rate limiting
        time.sleep(0.5)

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        # Print prominent summary box
        fixed_count = len(results["fixed"])
        failed_count = len(results["failed"])
        total_count = results["total"]

        print("\n" + "=" * 50)
        print(f"  FIXED: {fixed_count} / {total_count} URLs")
        print("=" * 50)

        if failed_count > 0:
            print(f"\nFailed: {failed_count} (may need manual review)")
            # Group failures by reason
            by_reason: dict[str, int] = {}
            for entry in results["failed"]:
                reason = entry.get("status", "unknown")
                by_reason[reason] = by_reason.get(reason, 0) + 1
            print("Failure breakdown:")
            for reason, count in sorted(by_reason.items(), key=lambda x: -x[1]):
                print(f"  - {reason}: {count}")

    return 1 if results["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
