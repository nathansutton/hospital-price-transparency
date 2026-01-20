"""Base class for directory scrapers.

Directory scrapers discover hospital price transparency files from
aggregator sites, producing a catalog of hospitals with their file URLs.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd


@dataclass
class DirectoryEntry:
    """A single hospital entry from a directory source.

    Represents the metadata extracted from a hospital directory,
    including the CCN (CMS Certification Number) as the primary identifier.
    """

    ccn: str  # 6-character CMS Certification Number (primary key)
    hospital_name: str
    address: str = ""
    city: str = ""
    state: str = ""  # 2-letter state code
    zip_code: str = ""
    file_url: str = ""
    file_format: str = ""  # JSON, CSV, XLSX, ZIP, etc.
    source: str = ""  # Where this data came from
    scraped_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        """Validate and normalize fields."""
        # Normalize CCN (strip whitespace, uppercase)
        self.ccn = self.ccn.strip().upper()
        # Normalize state code
        self.state = self.state.strip().upper()[:2] if self.state else ""
        # Normalize file format
        self.file_format = self.file_format.strip().upper() if self.file_format else ""

    def is_valid(self) -> bool:
        """Check if the entry has minimum required fields."""
        return bool(self.ccn and self.hospital_name and self.file_url)

    def to_dict(self) -> dict:
        """Convert to dictionary for DataFrame creation."""
        return {
            "ccn": self.ccn,
            "hospital_name": self.hospital_name,
            "address": self.address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "file_url": self.file_url,
            "file_format": self.file_format,
            "source": self.source,
            "scraped_at": self.scraped_at.isoformat(),
        }


class BaseDirectoryScraper(ABC):
    """Abstract base class for directory scrapers.

    Subclasses implement scraping logic for specific directory sources.
    The base class provides common functionality for rate limiting,
    checkpointing, and output handling.
    """

    def __init__(
        self,
        output_path: Path | None = None,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
    ) -> None:
        """Initialize the directory scraper.

        Args:
            output_path: Path to save the directory CSV. Defaults to dim/hospital_directory.csv
            min_delay: Minimum delay between requests in seconds
            max_delay: Maximum delay between requests in seconds
        """
        self.output_path = output_path or Path("dim/hospital_directory.csv")
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._entries: list[DirectoryEntry] = []

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the name of this directory source."""
        ...

    @abstractmethod
    async def scrape_state(self, state: str) -> Iterator[DirectoryEntry]:
        """Scrape all hospitals for a given state.

        Args:
            state: Two-letter state code (e.g., "TN", "NC")

        Yields:
            DirectoryEntry for each hospital found
        """
        ...

    @abstractmethod
    async def scrape_all(self, states: list[str] | None = None) -> Iterator[DirectoryEntry]:
        """Scrape all hospitals, optionally filtered by states.

        Args:
            states: List of state codes to scrape. If None, scrapes all states.

        Yields:
            DirectoryEntry for each hospital found
        """
        ...

    def add_entry(self, entry: DirectoryEntry) -> None:
        """Add an entry to the collection."""
        if entry.is_valid():
            self._entries.append(entry)

    def save(self, append: bool = False) -> Path:
        """Save collected entries to CSV.

        Args:
            append: If True, append to existing file. Otherwise, overwrite.

        Returns:
            Path to the saved file
        """
        df = pd.DataFrame([e.to_dict() for e in self._entries])

        if append and self.output_path.exists():
            existing = pd.read_csv(self.output_path)
            df = pd.concat([existing, df], ignore_index=True)
            # Deduplicate by CCN, keeping the latest entry
            df = df.drop_duplicates(subset=["ccn"], keep="last")

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_path, index=False)

        return self.output_path

    def load_checkpoint(self) -> set[str]:
        """Load already-scraped CCNs from existing file.

        Returns:
            Set of CCNs that have already been scraped
        """
        if not self.output_path.exists():
            return set()

        df = pd.read_csv(self.output_path)
        return set(df["ccn"].astype(str).tolist())

    def clear(self) -> None:
        """Clear collected entries."""
        self._entries = []

    @property
    def entry_count(self) -> int:
        """Return the number of collected entries."""
        return len(self._entries)
