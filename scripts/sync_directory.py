#!/usr/bin/env python3
"""CLI for syncing hospital directory from hospitalpricingfiles.org.

Usage:
    # Scrape specific states
    python scripts/sync_directory.py --state TN --state NC

    # Scrape all states (full sync)
    python scripts/sync_directory.py --all

    # Dry run (don't save)
    python scripts/sync_directory.py --state TN --dry-run

    # Custom output path
    python scripts/sync_directory.py --state TN --output dim/hospital_directory.csv
"""

import asyncio
import sys
from pathlib import Path

import click

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.directory_scrapers import HospitalPricingFilesScraper
from src.utils.logger import setup_logging, get_logger


@click.command()
@click.option(
    "--state", "-s",
    multiple=True,
    help="State code(s) to scrape (e.g., TN, NC). Can be specified multiple times.",
)
@click.option(
    "--all", "scrape_all",
    is_flag=True,
    help="Scrape all US states (full sync).",
)
@click.option(
    "--output", "-o",
    type=click.Path(),
    default="dim/hospital_directory.csv",
    help="Output CSV path.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Fetch data but don't save to disk.",
)
@click.option(
    "--headless/--no-headless",
    default=True,
    help="Run browser in headless mode.",
)
@click.option(
    "--min-delay",
    type=float,
    default=2.0,
    help="Minimum delay between requests (seconds).",
)
@click.option(
    "--max-delay",
    type=float,
    default=5.0,
    help="Maximum delay between requests (seconds).",
)
@click.option(
    "-v", "--verbose",
    is_flag=True,
    help="Enable verbose logging.",
)
def main(
    state: tuple[str, ...],
    scrape_all: bool,
    output: str,
    dry_run: bool,
    headless: bool,
    min_delay: float,
    max_delay: float,
    verbose: bool,
):
    """Sync hospital directory from hospitalpricingfiles.org.

    Scrapes hospital metadata and price file URLs, saving to a CSV file
    that can be used to populate hospital_v2.csv.
    """
    setup_logging(log_level="DEBUG" if verbose else "INFO")
    logger = get_logger(__name__)

    # Validate arguments
    if not state and not scrape_all:
        raise click.UsageError("Must specify --state or --all")

    states = list(state) if state else None

    logger.info(
        "starting_directory_sync",
        states=states or "ALL",
        output=output,
        dry_run=dry_run,
    )

    # Initialize scraper
    scraper = HospitalPricingFilesScraper(
        output_path=Path(output) if not dry_run else None,
        min_delay=min_delay,
        max_delay=max_delay,
        headless=headless,
    )

    # Run async scraper
    async def run():
        total = 0
        async for entry in scraper.scrape_all(states=states):
            total += 1
            if verbose:
                click.echo(f"  {entry.ccn}: {entry.hospital_name} ({entry.state})")

        return total

    try:
        total = asyncio.run(run())

        click.echo(f"\n{'[DRY RUN] ' if dry_run else ''}Scraped {total} hospitals")

        if not dry_run:
            click.echo(f"Saved to: {output}")

    except KeyboardInterrupt:
        logger.info("interrupted_by_user")
        click.echo("\nInterrupted. Progress has been checkpointed.")
        sys.exit(1)
    except Exception as e:
        logger.exception("sync_failed", error=str(e))
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
