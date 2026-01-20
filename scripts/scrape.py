#!/usr/bin/env python3
"""Scrape hospitals from URL JSON files (dim/urls/*.json).

These JSON files are created by the browser-based scraper from
hospitalpricingfiles.org and contain CCN, hospital name, and file URLs.

Usage:
    # Scrape all states with URL files
    python scripts/scrape.py

    # Scrape specific state
    python scripts/scrape.py --state VT

    # Scrape specific hospital by CCN
    python scripts/scrape.py --ccn 470011

    # Dry run (fetch and parse but don't save)
    python scripts/scrape.py --state VT --dry-run

    # Verbose logging
    python scripts/scrape.py --state VT -v
"""

import csv
import sys
from datetime import UTC, datetime
from pathlib import Path

import click

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import ScraperConfig, load_concept_codes, load_hospital_configs_from_urls
from src.models import HospitalConfig, ScrapeResult, ScrapeStats, ScrapeStatus
from src.normalizers import CPTNormalizer
from src.scrapers import get_scraper
from src.utils.http_client import RetryHTTPClient
from src.utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


def write_state_status(
    config: ScraperConfig,
    state: str,
    results: list[tuple[HospitalConfig, ScrapeResult]],
) -> Path:
    """Write per-state status file to status/{STATE}.csv.

    Args:
        config: Scraper configuration
        state: Two-letter state code
        results: List of (hospital_config, scrape_result) tuples

    Returns:
        Path to the written status file
    """
    status_dir = config.status_dir
    status_dir.mkdir(parents=True, exist_ok=True)

    status_file = status_dir / f"{state.upper()}.csv"

    fieldnames = [
        "date",
        "ccn",
        "hospital",
        "status",
        "file_url",
        "records",
        "error_type",
        "error_message",
        "duration",
    ]

    with open(status_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for hospital, result in results:
            writer.writerow(
                {
                    "date": datetime.now(UTC).isoformat(),
                    "ccn": hospital.ccn,
                    "hospital": hospital.hospital,
                    "status": result.status.value,
                    "file_url": result.file_url,
                    "records": result.records_scraped or "",
                    "error_type": result.error_type or "",
                    "error_message": result.error_message or "",
                    "duration": f"{result.duration_seconds:.2f}" if result.duration_seconds else "",
                }
            )

    logger.info("wrote_state_status", state=state, file=str(status_file), count=len(results))
    return status_file


@click.command()
@click.option(
    "--state", "-s", default=None, help="Scrape only hospitals from this state (e.g., VT)"
)
@click.option("--ccn", default=None, help="Scrape only the hospital with this CCN")
@click.option("--validate-only", is_flag=True, help="Only validate URLs, don't scrape")
@click.option("--dry-run", is_flag=True, help="Fetch and parse but don't save files")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
@click.option("--json-logs", is_flag=True, help="Output logs in JSON format")
def main(
    state: str | None,
    ccn: str | None,
    validate_only: bool,
    dry_run: bool,
    verbose: bool,
    json_logs: bool,
) -> None:
    """Scrape hospitals from URL JSON files.

    Processes hospitals from dim/urls/*.json files created by the
    browser-based hospitalpricingfiles.org scraper.
    """
    # Set up configuration
    config = ScraperConfig(
        log_level="DEBUG" if verbose else "INFO",
        json_logs=json_logs,
    )

    # Set up logging
    setup_logging(
        log_level=config.log_level,
        log_dir=config.logs_dir if not dry_run else None,
        json_logs=config.json_logs,
    )

    logger.info(
        "scraper_started",
        version="2.0.0",
        state_filter=state,
        ccn_filter=ccn,
        validate_only=validate_only,
        dry_run=dry_run,
    )

    # Load hospital configurations from URL JSON files
    try:
        hospitals = load_hospital_configs_from_urls(
            config,
            state_filter=state,
            ccn_filter=ccn,
        )

        if not hospitals:
            if state:
                click.echo(f"Error: No hospitals found for state {state}")
                click.echo(f"Make sure dim/urls/{state.lower()}.json exists")
            elif ccn:
                click.echo(f"Error: No hospital found with CCN {ccn}")
            else:
                click.echo("Error: No URL JSON files found in dim/urls/")
            sys.exit(1)

        click.echo(f"Loaded {len(hospitals)} hospitals from URL files")

    except Exception as e:
        logger.exception("config_load_failed", error=str(e))
        click.echo(f"Error loading configuration: {e}")
        sys.exit(1)

    # Load CPT vocabulary
    try:
        concept_df = load_concept_codes(config)
        normalizer = CPTNormalizer(concept_df)
        logger.info("loaded_cpt_vocabulary", codes=len(concept_df))
    except Exception as e:
        logger.exception("vocabulary_load_failed", error=str(e))
        click.echo(f"Error loading CPT vocabulary: {e}")
        sys.exit(1)

    # Initialize HTTP client
    http_client = RetryHTTPClient(
        timeout=config.http_timeout,
        max_retries=config.max_retries,
    )

    # Track results by state for status file output
    stats = ScrapeStats(start_time=datetime.now())
    results_by_state: dict[str, list[tuple[HospitalConfig, ScrapeResult]]] = {}

    # Process each hospital
    with http_client:
        for hospital in hospitals:
            click.echo(f"\nProcessing: {hospital.hospital} ({hospital.ccn})")
            click.echo(f"  State: {hospital.state} | Format: {hospital.type}")

            result: ScrapeResult

            if validate_only:
                # Just check URL accessibility
                accessible, status_msg = http_client.check_url(hospital.file_url)
                status = ScrapeStatus.SUCCESS if accessible else ScrapeStatus.FAILURE

                result = ScrapeResult(
                    hospital_npi=hospital.hospital_npi,
                    ccn=hospital.ccn,
                    status=status,
                    file_url=hospital.file_url,
                    error_message=None if accessible else status_msg,
                )
                stats.add_result(result)

                symbol = "+" if accessible else "x"
                click.echo(f"  {symbol} URL: {status_msg}")

            else:
                # Get appropriate scraper
                scraper = get_scraper(
                    hospital_config=hospital,
                    scraper_config=config,
                    http_client=http_client,
                    normalizer=normalizer,
                )

                if scraper is None:
                    result = ScrapeResult.skipped(
                        hospital_npi=hospital.hospital_npi,
                        file_url=hospital.file_url,
                        reason=f"No scraper for format: {hospital.type}",
                        ccn=hospital.ccn,
                    )
                    stats.add_result(result)
                    click.echo(f"  - Skipped: No scraper for format {hospital.type}")

                elif dry_run:
                    # Dry run: fetch and parse but don't save
                    start_time = datetime.now()
                    try:
                        raw_data = scraper.fetch_data()
                        df = scraper.parse_data(raw_data)
                        normalized = scraper.normalize(df)
                        duration = (datetime.now() - start_time).total_seconds()

                        result = ScrapeResult.success(
                            hospital_npi=hospital.hospital_npi,
                            file_url=hospital.file_url,
                            records_scraped=len(normalized),
                            duration_seconds=duration,
                            ccn=hospital.ccn,
                        )
                        click.echo(f"  + Dry run: {len(normalized)} records (not saved)")

                    except Exception as e:
                        duration = (datetime.now() - start_time).total_seconds()
                        result = ScrapeResult.failure(
                            hospital_npi=hospital.hospital_npi,
                            file_url=hospital.file_url,
                            error=e,
                            duration_seconds=duration,
                            ccn=hospital.ccn,
                        )
                        click.echo(f"  x Error: {e}")

                    stats.add_result(result)

                else:
                    # Full scrape
                    result = scraper.scrape()
                    # Add CCN to result if not set
                    if not result.ccn:
                        result.ccn = hospital.ccn
                    stats.add_result(result)

                    if result.status == ScrapeStatus.SUCCESS:
                        click.echo(f"  + Success: {result.records_scraped} records")
                    else:
                        click.echo(f"  x Failed: {result.error_message}")

            # Track result by state
            state_key = hospital.state.upper()
            if state_key not in results_by_state:
                results_by_state[state_key] = []
            results_by_state[state_key].append((hospital, result))

    stats.end_time = datetime.now()

    # Write per-state status files (unless dry run)
    if not dry_run:
        click.echo("\n" + "=" * 50)
        click.echo("Writing status files...")
        for state_code, state_results in results_by_state.items():
            status_file = write_state_status(config, state_code, state_results)
            click.echo(f"  Wrote {status_file}")

    # Print summary
    click.echo("\n" + "=" * 50)
    click.echo(f"Summary: {stats.summary()}")
    click.echo(f"Success rate: {stats.success_rate:.1f}%")

    if stats.failed > 0:
        click.echo("\nFailed hospitals:")
        for state_results in results_by_state.values():
            for hospital, result in state_results:
                if result.status == ScrapeStatus.FAILURE:
                    click.echo(f"  - {hospital.ccn} ({hospital.hospital}): {result.error_message}")

    if stats.skipped > 0:
        click.echo("\nSkipped hospitals:")
        for state_results in results_by_state.values():
            for hospital, result in state_results:
                if result.status == ScrapeStatus.SKIPPED:
                    click.echo(f"  - {hospital.ccn} ({hospital.hospital}): {result.error_message}")

    logger.info(
        "scraper_completed",
        total=stats.total_hospitals,
        successful=stats.successful,
        failed=stats.failed,
        skipped=stats.skipped,
        total_records=stats.total_records,
        duration_seconds=stats.total_duration_seconds,
    )

    # Exit with error if any failures
    if stats.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
