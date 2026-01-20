"""Abstract base class for hospital price scrapers.

Defines the interface that all format-specific scrapers must implement.
Uses the Strategy pattern to allow different parsing strategies.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..config import ScraperConfig, get_output_path
from ..models import HospitalConfig, PriceRecord, ScrapeResult
from ..normalizers import CPTNormalizer
from ..utils.http_client import RetryHTTPClient
from ..utils.logger import ScrapeLogContext, get_logger

logger = get_logger(__name__)


class BaseScraper(ABC):
    """Abstract base class for all scrapers.

    Implements the Template Method pattern where the scrape() method
    defines the algorithm, and subclasses implement format-specific
    fetch_data() and parse_data() methods.
    """

    def __init__(
        self,
        hospital_config: HospitalConfig,
        scraper_config: ScraperConfig,
        http_client: RetryHTTPClient,
        normalizer: CPTNormalizer,
    ):
        """Initialize the scraper.

        Args:
            hospital_config: Configuration for the hospital to scrape
            scraper_config: Global scraper configuration
            http_client: HTTP client with retry logic
            normalizer: CPT code normalizer
        """
        self.hospital_config = hospital_config
        self.scraper_config = scraper_config
        self.http_client = http_client
        self.normalizer = normalizer
        self.logger = get_logger(
            __name__,
            hospital_npi=hospital_config.hospital_npi,
            hospital_name=hospital_config.hospital,
        )

    @abstractmethod
    def fetch_data(self) -> bytes | str | dict:
        """Fetch raw data from the hospital's price file URL.

        Returns:
            Raw data in the format appropriate for the data type
            (bytes for binary, str for text, dict for pre-parsed JSON)

        Raises:
            HTTPError: If the fetch fails
        """
        pass

    @abstractmethod
    def parse_data(self, raw_data: bytes | str | dict | list) -> pd.DataFrame:
        """Parse raw data into a DataFrame with required columns.

        The returned DataFrame must have at minimum:
        - vocabulary_id: Code type identifier (e.g., 'cpt')
        - concept_code: The procedure code
        - gross: Gross charge amount
        - cash: Cash/discounted price amount

        Args:
            raw_data: Raw data from fetch_data()

        Returns:
            DataFrame with parsed price data

        Raises:
            ValueError: If parsing fails
        """
        pass

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize parsed data to standard schema.

        Applies CPT code normalization, filters invalid codes,
        and transforms to the output format.

        Args:
            df: DataFrame from parse_data()

        Returns:
            Normalized DataFrame ready for output
        """
        return self.normalizer.normalize(df)

    def scrape(self) -> ScrapeResult:
        """Execute the full scrape workflow.

        This is the main entry point that orchestrates:
        1. Fetch raw data
        2. Parse to DataFrame
        3. Normalize to standard schema
        4. Save to JSONL file

        Returns:
            ScrapeResult with success/failure status and metadata
        """
        start_time = datetime.now()

        with ScrapeLogContext(
            self.logger,
            self.hospital_config.hospital_npi,
            self.hospital_config.hospital,
        ) as log_ctx:
            try:
                # Fetch raw data
                self.logger.debug("fetching_data", url=self.hospital_config.file_url)
                raw_data = self.fetch_data()

                # Parse to DataFrame
                self.logger.debug("parsing_data")
                df = self.parse_data(raw_data)
                self.logger.debug("parsed_records", count=len(df))

                # Normalize
                self.logger.debug("normalizing_data")
                normalized = self.normalize(df)
                self.logger.debug("normalized_records", count=len(normalized))

                # Save to file
                output_path = get_output_path(self.scraper_config, self.hospital_config)
                self._save_jsonl(normalized, output_path)

                duration = (datetime.now() - start_time).total_seconds()
                log_ctx.set_records_scraped(len(normalized))

                return ScrapeResult.success(
                    hospital_npi=self.hospital_config.hospital_npi,
                    file_url=self.hospital_config.file_url,
                    records_scraped=len(normalized),
                    duration_seconds=duration,
                )

            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.exception("scrape_error", error=str(e))
                return ScrapeResult.failure(
                    hospital_npi=self.hospital_config.hospital_npi,
                    file_url=self.hospital_config.file_url,
                    error=e,
                    duration_seconds=duration,
                )

    def _save_jsonl(self, df: pd.DataFrame, output_path: Path) -> None:
        """Save DataFrame to JSONL format.

        Args:
            df: Normalized DataFrame with cpt, type, price columns
            output_path: Path to save the file
        """
        # Validate records before saving
        valid_records = []
        for _, row in df.iterrows():
            try:
                record = PriceRecord(
                    cpt=row["cpt"],
                    type=row["type"],
                    price=row["price"],
                )
                valid_records.append(record.model_dump())
            except Exception as e:
                self.logger.warning(
                    "invalid_record",
                    cpt=row.get("cpt"),
                    error=str(e),
                )

        # Convert back to DataFrame and save
        if valid_records:
            pd.DataFrame(valid_records).to_json(
                output_path,
                orient="records",
                lines=True,
            )
            self.logger.info(
                "saved_output",
                path=str(output_path),
                records=len(valid_records),
            )
        else:
            self.logger.warning("no_valid_records")
