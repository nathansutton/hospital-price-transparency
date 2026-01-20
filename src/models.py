"""Pydantic models for type-safe data handling.

Provides validation for hospital configuration, price records, and scrape status.
"""

import re
import datetime as dt
from datetime import date, datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DataFormat(str, Enum):
    """Supported data formats for hospital price files."""

    CSV = "CSV"
    JSON = "JSON"
    XLSX = "XLSX"
    XML = "XML"
    ZIP = "ZIP"


class ScrapeStatus(str, Enum):
    """Status of a scrape operation."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    WIP = "WIP"
    SKIPPED = "SKIPPED"


class PriceType(str, Enum):
    """Type of price (gross charge vs cash/discounted price)."""

    GROSS = "gross"
    CASH = "cash"


# CPT code pattern: 5 characters, alphanumeric (e.g., 99213, 0001A)
CPT_PATTERN = re.compile(r"^[0-9A-Z]{5}$")


class HospitalConfig(BaseModel):
    """Configuration for a hospital from dim/hospital.csv or dim/hospital_v2.csv.

    Validates and normalizes hospital configuration data.
    Supports both NPI-based (v1) and CCN-based (v2) identification.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    # Identifiers - CCN is preferred in v2, NPI kept for backward compatibility
    ccn: str | None = Field(None, description="CMS Certification Number (6 alphanumeric chars)")
    hospital_npi: str = Field(..., description="National Provider Identifier (10 digits)")

    # Hospital metadata
    can_automate: bool = Field(..., description="Whether scraping can be automated")
    idn: str = Field(default="", description="Integrated Delivery Network name")
    hospital: str = Field(..., description="Hospital name")
    address: str = Field(default="", description="Hospital address")
    city: str = Field(default="", description="City name")
    cbsa: int | None = Field(None, description="Core Based Statistical Area code")
    cbsa_title: str = Field(default="", description="CBSA title")
    state: str = Field(..., min_length=2, max_length=2, description="Two-letter state code")
    zip_code: str = Field(default="", description="ZIP code")

    # URL configuration
    parent_url: str = Field(default="", description="URL to parent transparency page")
    file_url: str = Field(..., description="Direct URL to price file")

    # Format and scraper configuration
    type: DataFormat | None = Field(None, description="Data format (CSV, JSON, etc.)")
    scraper_type: str | None = Field(None, description="Explicit scraper class override")
    payer: bool | None = Field(None, description="Whether payer-specific data is included")

    # CSV parsing configuration
    skiprow: int = Field(0, description="Number of rows to skip when parsing")
    wide: bool = Field(False, description="Whether data is in wide format")
    gross: str | None = Field(None, description="Column name for gross charges")
    cash: str | None = Field(None, description="Column name for cash prices")
    cpt: str | None = Field(None, description="Column name for CPT codes")

    @field_validator("ccn")
    @classmethod
    def validate_ccn(cls, v: str | None) -> str | None:
        """Validate CCN is 6 alphanumeric characters."""
        if v is None or v == "":
            return None
        v = v.strip().upper()
        if len(v) != 6 or not v.isalnum():
            raise ValueError(f"CCN must be exactly 6 alphanumeric characters, got: {v}")
        return v

    @field_validator("hospital_npi")
    @classmethod
    def validate_npi(cls, v: str) -> str:
        """Validate NPI is 10 digits."""
        if not v.isdigit() or len(v) != 10:
            raise ValueError(f"NPI must be exactly 10 digits, got: {v}")
        return v

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Validate and uppercase state code."""
        return v.upper()

    @property
    def identifier(self) -> str:
        """Return the preferred identifier (CCN if available, else NPI)."""
        return self.ccn if self.ccn else self.hospital_npi

    @property
    def has_ccn(self) -> bool:
        """Check if this config has a CCN."""
        return self.ccn is not None


class PriceRecord(BaseModel):
    """A single price record from the scraped data.

    Represents one row of output in the JSONL files.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    cpt: str = Field(..., description="CPT/HCPCS code")
    type: PriceType = Field(..., description="Price type (gross or cash)")
    price: Annotated[float, Field(ge=0, description="Price in dollars")]

    @field_validator("cpt")
    @classmethod
    def validate_cpt(cls, v: str) -> str:
        """Validate CPT code format."""
        v = v.strip().upper()
        if not CPT_PATTERN.match(v):
            raise ValueError(f"Invalid CPT code format: {v}")
        return v


class ScrapeResult(BaseModel):
    """Result of a scrape operation for status.csv.

    Enhanced schema with detailed error tracking.
    Supports both NPI and CCN identifiers for the transition period.
    """

    model_config = ConfigDict(str_strip_whitespace=True)

    scrape_date: date = Field(default_factory=date.today, description="Scrape date")
    hospital_npi: str = Field(..., description="Hospital NPI")
    ccn: str | None = Field(None, description="Hospital CCN (if available)")
    status: ScrapeStatus = Field(..., description="Scrape status")
    file_url: str = Field(..., description="URL that was scraped")
    records_scraped: int | None = Field(None, description="Number of records extracted")
    error_type: str | None = Field(None, description="Exception type on failure")
    error_message: str | None = Field(None, description="Error message on failure")
    duration_seconds: float | None = Field(None, description="Scrape duration in seconds")

    @property
    def identifier(self) -> str:
        """Return the preferred identifier (CCN if available, else NPI)."""
        return self.ccn if self.ccn else self.hospital_npi

    @classmethod
    def success(
        cls,
        hospital_npi: str,
        file_url: str,
        records_scraped: int,
        duration_seconds: float,
        ccn: str | None = None,
    ) -> "ScrapeResult":
        """Create a successful scrape result."""
        return cls(
            hospital_npi=hospital_npi,
            ccn=ccn,
            status=ScrapeStatus.SUCCESS,
            file_url=file_url,
            records_scraped=records_scraped,
            duration_seconds=duration_seconds,
        )

    @classmethod
    def failure(
        cls,
        hospital_npi: str,
        file_url: str,
        error: Exception,
        duration_seconds: float,
        ccn: str | None = None,
    ) -> "ScrapeResult":
        """Create a failed scrape result."""
        return cls(
            hospital_npi=hospital_npi,
            ccn=ccn,
            status=ScrapeStatus.FAILURE,
            file_url=file_url,
            error_type=type(error).__name__,
            error_message=str(error)[:500],  # Truncate long messages
            duration_seconds=duration_seconds,
        )

    @classmethod
    def skipped(
        cls,
        hospital_npi: str,
        file_url: str,
        reason: str,
        ccn: str | None = None,
    ) -> "ScrapeResult":
        """Create a skipped scrape result."""
        return cls(
            hospital_npi=hospital_npi,
            ccn=ccn,
            status=ScrapeStatus.SKIPPED,
            file_url=file_url,
            error_message=reason,
        )


class ConceptCode(BaseModel):
    """OHDSI Athena concept code for CPT validation."""

    concept_code: str
    vocabulary_id: str = "CPT4"

    @property
    def is_cpt(self) -> bool:
        """Check if this is a CPT4 code."""
        return self.vocabulary_id == "CPT4"


class ScrapeStats(BaseModel):
    """Aggregate statistics for a scrape run."""

    total_hospitals: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    total_records: int = 0
    total_duration_seconds: float = 0.0
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.total_hospitals == 0:
            return 0.0
        return (self.successful / self.total_hospitals) * 100

    def add_result(self, result: ScrapeResult) -> None:
        """Update stats with a scrape result."""
        self.total_hospitals += 1
        if result.status == ScrapeStatus.SUCCESS:
            self.successful += 1
            self.total_records += result.records_scraped or 0
        elif result.status == ScrapeStatus.FAILURE:
            self.failed += 1
        elif result.status == ScrapeStatus.SKIPPED:
            self.skipped += 1
        self.total_duration_seconds += result.duration_seconds or 0.0

    def summary(self) -> str:
        """Generate a human-readable summary."""
        return (
            f"{self.successful}/{self.total_hospitals} hospitals successful | "
            f"{self.total_records:,} records | "
            f"{self.total_duration_seconds:.1f}s"
        )
