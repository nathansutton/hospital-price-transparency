#!/usr/bin/env python3
"""URL validation utility for hospital price transparency files.

Tests each URL in hospital.csv and reports status.
Can also attempt to discover updated URLs.

Usage:
    # Check all URLs
    python scripts/validate_urls.py

    # Check specific hospital
    python scripts/validate_urls.py --npi 1104874684

    # Include non-automated hospitals
    python scripts/validate_urls.py --all

    # Export results to CSV
    python scripts/validate_urls.py --output url_status.csv
"""

import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import click
import pandas as pd
from bs4 import BeautifulSoup

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import ScraperConfig, load_hospital_configs
from src.utils.http_client import RetryHTTPClient
from src.utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


@click.command()
@click.option("--npi", default=None, help="Check only the hospital with this NPI")
@click.option("--all", "include_all", is_flag=True, help="Include non-automated hospitals")
@click.option("--output", "-o", default=None, help="Export results to CSV file")
@click.option("--discover", is_flag=True, help="Attempt to discover updated URLs")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose output")
def main(
    npi: str | None,
    include_all: bool,
    output: str | None,
    discover: bool,
    verbose: bool,
) -> None:
    """Validate hospital price transparency file URLs."""
    setup_logging(log_level="DEBUG" if verbose else "INFO")

    config = ScraperConfig()

    # Load hospital configs
    hospitals = load_hospital_configs(
        config,
        only_automated=not include_all,
        npi_filter=npi,
    )

    if not hospitals:
        click.echo("No hospitals found matching criteria")
        sys.exit(1)

    click.echo(f"Checking {len(hospitals)} hospitals...\n")

    http_client = RetryHTTPClient(timeout=30, max_retries=1)
    results = []

    with http_client:
        for hospital in hospitals:
            click.echo(f"{hospital.hospital} ({hospital.hospital_npi})")
            click.echo(f"  IDN: {hospital.idn}")
            click.echo(f"  URL: {hospital.file_url}")

            # Check file URL
            accessible, status_msg = http_client.check_url(hospital.file_url)

            result = {
                "hospital_npi": hospital.hospital_npi,
                "hospital": hospital.hospital,
                "idn": hospital.idn,
                "can_automate": hospital.can_automate,
                "file_url": hospital.file_url,
                "url_status": "OK" if accessible else status_msg,
                "url_accessible": accessible,
                "parent_url": hospital.parent_url,
                "parent_accessible": None,
                "discovered_url": None,
            }

            if accessible:
                click.echo("  ✓ File URL: OK")
            else:
                click.echo(f"  ✗ File URL: {status_msg}")

                # Check parent URL
                if hospital.parent_url:
                    parent_ok, parent_msg = http_client.check_url(hospital.parent_url)
                    result["parent_accessible"] = parent_ok

                    if parent_ok:
                        click.echo("  → Parent URL: OK")

                        # Try to discover updated file URL
                        if discover:
                            discovered = discover_file_url(
                                http_client,
                                hospital.parent_url,
                                hospital.file_url,
                            )
                            if discovered:
                                result["discovered_url"] = discovered
                                click.echo(f"  → Discovered: {discovered}")
                    else:
                        click.echo(f"  ✗ Parent URL: {parent_msg}")

            results.append(result)
            click.echo()

    # Summary
    df = pd.DataFrame(results)
    accessible_count = df["url_accessible"].sum()
    total = len(df)

    click.echo("=" * 60)
    click.echo(
        f"Summary: {accessible_count}/{total} URLs accessible ({accessible_count / total * 100:.1f}%)"
    )

    failed = df[~df["url_accessible"]]
    if not failed.empty:
        click.echo(f"\nFailed URLs ({len(failed)}):")
        for _, row in failed.iterrows():
            click.echo(f"  - {row['hospital']} ({row['idn']}): {row['url_status']}")

    # Export if requested
    if output:
        df.to_csv(output, index=False)
        click.echo(f"\nResults exported to: {output}")


def discover_file_url(
    http_client: RetryHTTPClient,
    parent_url: str,
    original_url: str,
) -> str | None:
    """Attempt to discover the updated file URL from the parent page.

    Args:
        http_client: HTTP client
        parent_url: URL of the parent transparency page
        original_url: Original file URL to help identify the file

    Returns:
        Discovered URL or None
    """
    try:
        response = http_client.get(parent_url)
        soup = BeautifulSoup(response.text, "lxml")

        # Parse the original URL to understand what we're looking for
        original_parsed = urlparse(original_url)
        _ = Path(original_parsed.path).name.lower()  # For future use

        # Look for links that might be the price file
        candidates = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            href_lower = href.lower()

            # Check for common price transparency file indicators
            if any(
                indicator in href_lower
                for indicator in [
                    "standardcharges",
                    "chargemaster",
                    "price",
                    ".csv",
                    ".json",
                    ".xlsx",
                ]
            ):
                # Convert relative URLs to absolute
                full_url = urljoin(parent_url, href)
                candidates.append(full_url)

        # Try to find the best match
        for candidate in candidates:
            # Check if it's accessible
            accessible, _ = http_client.check_url(candidate)
            if accessible:
                return candidate

        return None

    except Exception as e:
        logger.warning("url_discovery_failed", parent_url=parent_url, error=str(e))
        return None


if __name__ == "__main__":
    main()
