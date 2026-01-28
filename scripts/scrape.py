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
import multiprocessing as mp
import sys
from datetime import UTC, datetime
from pathlib import Path

import click

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import ScraperConfig, get_data_age_days, load_hospital_configs_from_urls
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


def _worker_process(
    hospital: HospitalConfig,
    config: ScraperConfig,
    concept_df_path: Path,
    validate_only: bool,
    dry_run: bool,
    max_age_days: int,
    timeout: int,
    result_queue: mp.Queue,
) -> None:
    """Worker process that scrapes a single hospital.

    Creates its own HTTP client and normalizer (can't share across processes).
    Sends result back via queue.
    """
    import pandas as pd

    start_time = datetime.now()

    try:
        # Check if data is fresh enough to skip (incremental scraping)
        if max_age_days > 0 and not validate_only:
            data_age = get_data_age_days(config, hospital)
            if data_age is not None and data_age < max_age_days:
                result = ScrapeResult.skipped(
                    hospital_npi=hospital.hospital_npi,
                    file_url=hospital.file_url,
                    reason=f"Data is {data_age:.1f} days old (max age: {max_age_days})",
                    ccn=hospital.ccn,
                )
                result_queue.put((hospital, result, f"~ Skipped: Data is {data_age:.1f} days old"))
                return

        # Create HTTP client for this process
        http_client = RetryHTTPClient(
            timeout=timeout,
            max_retries=config.max_retries,
        )

        # Load normalizer for this process
        concept_df = pd.read_csv(concept_df_path, compression="gzip", sep="\t")
        concept_df = concept_df[concept_df["vocabulary_id"].isin(["CPT4", "HCPCS"])]
        normalizer = CPTNormalizer(concept_df[["concept_code"]])

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
            symbol = "+" if accessible else "x"
            result_queue.put((hospital, result, f"{symbol} URL: {status_msg}"))
            return

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
            result_queue.put((hospital, result, f"- Skipped: No scraper for format {hospital.type}"))
            return

        if dry_run:
            # Dry run: fetch and parse but don't save
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
            result_queue.put((hospital, result, f"+ Dry run: {len(normalized)} records (not saved)"))
            return

        # Full scrape
        result = scraper.scrape()
        # Add CCN to result if not set
        if not result.ccn:
            result.ccn = hospital.ccn

        if result.status == ScrapeStatus.SUCCESS:
            result_queue.put((hospital, result, f"+ Success: {result.records_scraped} records"))
        else:
            result_queue.put((hospital, result, f"x Failed: {result.error_message}"))

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        result = ScrapeResult.failure(
            hospital_npi=hospital.hospital_npi,
            file_url=hospital.file_url,
            error=e,
            duration_seconds=duration,
            ccn=hospital.ccn,
        )
        result_queue.put((hospital, result, f"x Error: {e}"))


def _process_hospital_with_timeout(
    hospital: HospitalConfig,
    config: ScraperConfig,
    concept_df_path: Path,
    validate_only: bool,
    dry_run: bool,
    max_age_days: int,
    timeout: int,
) -> tuple[HospitalConfig, ScrapeResult, str]:
    """Process a hospital with true timeout via process termination.

    Spawns a subprocess that can be killed if it exceeds the timeout.
    """
    result_queue: mp.Queue = mp.Queue()

    process = mp.Process(
        target=_worker_process,
        args=(
            hospital,
            config,
            concept_df_path,
            validate_only,
            dry_run,
            max_age_days,
            timeout,
            result_queue,
        ),
    )

    process.start()
    process.join(timeout=timeout)

    if process.is_alive():
        # Process exceeded timeout - kill it
        process.terminate()
        process.join(timeout=5)  # Give it 5s to terminate gracefully

        if process.is_alive():
            # Still alive after terminate, force kill
            process.kill()
            process.join(timeout=2)

        result = ScrapeResult.failure(
            hospital_npi=hospital.hospital_npi,
            file_url=hospital.file_url,
            error=TimeoutError(f"Killed after {timeout}s timeout"),
            duration_seconds=float(timeout),
            ccn=hospital.ccn,
        )
        return hospital, result, f"T Killed after {timeout}s"

    # Process completed - get result from queue
    try:
        return result_queue.get_nowait()
    except Exception:
        # Queue empty means process crashed without sending result
        result = ScrapeResult.failure(
            hospital_npi=hospital.hospital_npi,
            file_url=hospital.file_url,
            error=RuntimeError("Worker process crashed"),
            duration_seconds=0.0,
            ccn=hospital.ccn,
        )
        return hospital, result, "! Worker crashed"


@click.command()
@click.option(
    "--state", "-s", default=None, help="Scrape only hospitals from this state (e.g., VT)"
)
@click.option("--ccn", default=None, help="Scrape only the hospital with this CCN")
@click.option("--validate-only", is_flag=True, help="Only validate URLs, don't scrape")
@click.option("--dry-run", is_flag=True, help="Fetch and parse but don't save files")
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
@click.option("--json-logs", is_flag=True, help="Output logs in JSON format")
@click.option(
    "--max-age-days",
    default=0,
    type=int,
    help="Skip hospitals with data newer than N days (0=always scrape)",
)
@click.option(
    "--parallel",
    "-p",
    default=1,
    type=int,
    help="Number of parallel workers (default: 1, sequential)",
)
@click.option(
    "--timeout",
    "-t",
    default=1200,
    type=int,
    help="Timeout per hospital in seconds (default: 1200 = 20 minutes)",
)
def main(
    state: str | None,
    ccn: str | None,
    validate_only: bool,
    dry_run: bool,
    verbose: bool,
    json_logs: bool,
    max_age_days: int,
    parallel: int,
    timeout: int,
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
        version="2.1.0",
        state_filter=state,
        ccn_filter=ccn,
        validate_only=validate_only,
        dry_run=dry_run,
        max_age_days=max_age_days,
        parallel=parallel,
        timeout=timeout,
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

    # Verify CPT vocabulary exists (workers load it themselves)
    concept_df_path = config.concept_csv_path
    if not concept_df_path.exists():
        click.echo(f"Error: CPT vocabulary not found at {concept_df_path}")
        sys.exit(1)

    logger.info("cpt_vocabulary_path", path=str(concept_df_path))

    # Track results by state for status file output
    stats = ScrapeStats(start_time=datetime.now())
    results_by_state: dict[str, list[tuple[HospitalConfig, ScrapeResult]]] = {}

    # Process hospitals
    if parallel <= 1:
        # Sequential processing
        for i, hospital in enumerate(hospitals, 1):
            click.echo(f"\n[{i}/{len(hospitals)}] {hospital.hospital} ({hospital.ccn})")
            click.echo(f"  State: {hospital.state} | Format: {hospital.type}")

            hospital, result, message = _process_hospital_with_timeout(
                hospital=hospital,
                config=config,
                concept_df_path=concept_df_path,
                validate_only=validate_only,
                dry_run=dry_run,
                max_age_days=max_age_days,
                timeout=timeout,
            )
            click.echo(f"  {message}")

            stats.add_result(result)

            # Track result by state
            state_key = hospital.state.upper()
            if state_key not in results_by_state:
                results_by_state[state_key] = []
            results_by_state[state_key].append((hospital, result))

    else:
        # Parallel processing with process pool
        click.echo(f"\nProcessing {len(hospitals)} hospitals with {parallel} workers (timeout: {timeout}s)...")
        click.echo("Using multiprocessing - stuck workers will be killed.\n")

        # Use a semaphore to limit concurrent processes
        from concurrent.futures import ProcessPoolExecutor, as_completed

        with ProcessPoolExecutor(max_workers=parallel) as executor:
            # Submit all tasks
            future_to_hospital = {
                executor.submit(
                    _process_hospital_with_timeout,
                    hospital=h,
                    config=config,
                    concept_df_path=concept_df_path,
                    validate_only=validate_only,
                    dry_run=dry_run,
                    max_age_days=max_age_days,
                    timeout=timeout,
                ): h
                for h in hospitals
            }

            # Process results as they complete
            completed = 0
            for future in as_completed(future_to_hospital):
                completed += 1
                original_hospital = future_to_hospital[future]
                try:
                    hospital, result, message = future.result()

                    # Compact output for parallel mode
                    status_char = message[0] if message else "?"
                    hospital_name = hospital.hospital[:30] + "..." if len(hospital.hospital) > 30 else hospital.hospital
                    click.echo(f"[{completed}/{len(hospitals)}] {status_char} {hospital.ccn} ({hospital_name})")

                    # Track result by state
                    state_key = hospital.state.upper()
                    if state_key not in results_by_state:
                        results_by_state[state_key] = []
                    results_by_state[state_key].append((hospital, result))

                except Exception as e:
                    click.echo(f"[{completed}/{len(hospitals)}] ! {original_hospital.ccn} Error: {e}")
                    result = ScrapeResult.failure(
                        hospital_npi=original_hospital.hospital_npi,
                        file_url=original_hospital.file_url,
                        error=e,
                        duration_seconds=0.0,
                        ccn=original_hospital.ccn,
                    )

                    state_key = original_hospital.state.upper()
                    if state_key not in results_by_state:
                        results_by_state[state_key] = []
                    results_by_state[state_key].append((original_hospital, result))

                stats.add_result(result)

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
