"""Scraper for hospitalpricingfiles.org directory.

This site aggregates hospital price transparency files from across the US.
It uses JavaScript rendering, requiring Playwright for reliable scraping.
"""

import asyncio
import random
import re
from typing import AsyncIterator
from pathlib import Path

from ..utils.logger import get_logger
from .base import BaseDirectoryScraper, DirectoryEntry

logger = get_logger(__name__)

# All US states (for full scrape)
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    "DC", "PR", "VI", "GU", "AS", "MP",  # Territories
]

# CCN pattern: 6 alphanumeric characters
CCN_PATTERN = re.compile(r"^[0-9A-Z]{6}$", re.IGNORECASE)


def validate_ccn(ccn: str) -> bool:
    """Validate a CCN (CMS Certification Number).

    CCN is a 6-character alphanumeric code assigned by CMS.
    First 2 characters are the state code (01-99 for states, territories).
    """
    if not ccn or len(ccn) != 6:
        return False
    return CCN_PATTERN.match(ccn) is not None


class HospitalPricingFilesScraper(BaseDirectoryScraper):
    """Scraper for hospitalpricingfiles.org.

    Uses Playwright to handle JavaScript rendering and dynamic content loading.
    Implements polite scraping with random delays and state-based checkpointing.
    """

    BASE_URL = "https://hospitalpricingfiles.org"

    def __init__(
        self,
        output_path: Path | None = None,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        headless: bool = True,
    ) -> None:
        """Initialize the scraper.

        Args:
            output_path: Path to save output CSV
            min_delay: Minimum delay between requests
            max_delay: Maximum delay between requests
            headless: Run browser in headless mode
        """
        super().__init__(output_path, min_delay, max_delay)
        self.headless = headless
        self._browser = None
        self._context = None

    @property
    def source_name(self) -> str:
        return "hospitalpricingfiles.org"

    async def _ensure_browser(self):
        """Ensure Playwright browser is initialized."""
        if self._browser is None:
            try:
                from playwright.async_api import async_playwright
            except ImportError:
                raise ImportError(
                    "Playwright is required for this scraper. "
                    "Install it with: pip install playwright && playwright install chromium"
                )

            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=self.headless)
            self._context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
            )
            logger.info("browser_started", headless=self.headless)

    async def _close_browser(self):
        """Close the browser and cleanup."""
        if self._browser:
            await self._browser.close()
            await self._playwright.stop()
            self._browser = None
            self._context = None
            logger.info("browser_closed")

    async def _random_delay(self):
        """Apply random delay between requests."""
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)

    async def _parse_hospital_row(self, row, state: str) -> DirectoryEntry | None:
        """Parse a single hospital row from the table.

        Args:
            row: Playwright element handle for the table row
            state: State code for this hospital

        Returns:
            DirectoryEntry if parsing succeeds, None otherwise
        """
        try:
            cells = await row.query_selector_all("td")
            if len(cells) < 4:
                return None

            # Extract text from cells
            ccn = await cells[0].inner_text()
            hospital_name = await cells[1].inner_text()
            address_text = await cells[2].inner_text() if len(cells) > 2 else ""

            # Extract file URL from link
            file_url = ""
            file_format = ""
            link = await row.query_selector("a[href]")
            if link:
                file_url = await link.get_attribute("href") or ""
                link_text = await link.inner_text()
                # Try to determine format from link text or URL
                file_format = self._detect_format(file_url, link_text)

            # Parse address components
            city, zip_code = self._parse_address(address_text)

            # Validate CCN
            ccn = ccn.strip()
            if not validate_ccn(ccn):
                logger.debug("invalid_ccn", ccn=ccn, hospital=hospital_name)
                return None

            return DirectoryEntry(
                ccn=ccn,
                hospital_name=hospital_name.strip(),
                address=address_text.strip(),
                city=city,
                state=state,
                zip_code=zip_code,
                file_url=file_url,
                file_format=file_format,
                source=self.source_name,
            )

        except Exception as e:
            logger.warning("row_parse_error", error=str(e))
            return None

    def _detect_format(self, url: str, link_text: str) -> str:
        """Detect file format from URL or link text."""
        url_lower = url.lower()
        text_lower = link_text.lower()

        if ".json" in url_lower or "json" in text_lower:
            return "JSON"
        elif ".csv" in url_lower or "csv" in text_lower:
            return "CSV"
        elif ".xlsx" in url_lower or ".xls" in url_lower or "excel" in text_lower:
            return "XLSX"
        elif ".zip" in url_lower or "zip" in text_lower:
            return "ZIP"
        elif ".xml" in url_lower or "xml" in text_lower:
            return "XML"
        return ""

    def _parse_address(self, address_text: str) -> tuple[str, str]:
        """Parse city and zip code from address string.

        Returns:
            Tuple of (city, zip_code)
        """
        # Common pattern: "City, ST 12345" or "123 Main St, City, ST 12345"
        zip_match = re.search(r"\b(\d{5}(?:-\d{4})?)\b", address_text)
        zip_code = zip_match.group(1) if zip_match else ""

        # Try to extract city (before state abbreviation)
        city_match = re.search(r",\s*([^,]+),\s*[A-Z]{2}\s*\d{5}", address_text)
        city = city_match.group(1).strip() if city_match else ""

        return city, zip_code

    async def scrape_state(self, state: str) -> AsyncIterator[DirectoryEntry]:
        """Scrape all hospitals for a given state.

        Args:
            state: Two-letter state code

        Yields:
            DirectoryEntry for each hospital found
        """
        await self._ensure_browser()

        state = state.upper()
        url = f"{self.BASE_URL}/states/{state.lower()}"

        logger.info("scraping_state", state=state, url=url)

        page = await self._context.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=60000)

            # Wait for table to load (adjust selector based on actual site structure)
            await page.wait_for_selector("table", timeout=30000)

            # Find all hospital rows
            rows = await page.query_selector_all("table tbody tr")

            logger.info("found_hospitals", state=state, count=len(rows))

            for row in rows:
                entry = await self._parse_hospital_row(row, state)
                if entry:
                    self.add_entry(entry)
                    yield entry

            await self._random_delay()

        except Exception as e:
            logger.error("state_scrape_error", state=state, error=str(e))
            raise
        finally:
            await page.close()

    async def scrape_all(
        self,
        states: list[str] | None = None,
        checkpoint_every: int = 5,
    ) -> AsyncIterator[DirectoryEntry]:
        """Scrape all hospitals from the directory.

        Args:
            states: List of state codes to scrape. If None, scrapes all US states.
            checkpoint_every: Save progress every N states

        Yields:
            DirectoryEntry for each hospital found
        """
        states_to_scrape = [s.upper() for s in (states or US_STATES)]

        # Load checkpoint to skip already-scraped states
        # (We track by CCN, but for efficiency we can also track completed states)
        existing_ccns = self.load_checkpoint()
        logger.info(
            "starting_full_scrape",
            states=len(states_to_scrape),
            existing_entries=len(existing_ccns),
        )

        await self._ensure_browser()

        try:
            for i, state in enumerate(states_to_scrape):
                state_count = 0
                async for entry in self.scrape_state(state):
                    if entry.ccn not in existing_ccns:
                        state_count += 1
                        yield entry

                logger.info(
                    "state_complete",
                    state=state,
                    new_entries=state_count,
                    total=self.entry_count,
                )

                # Checkpoint periodically
                if (i + 1) % checkpoint_every == 0:
                    self.save(append=True)
                    logger.info("checkpoint_saved", entries=self.entry_count)
                    self.clear()  # Clear memory after saving
                    existing_ccns = self.load_checkpoint()  # Reload for dedup

            # Final save
            if self.entry_count > 0:
                self.save(append=True)
                logger.info("final_save", entries=self.entry_count)

        finally:
            await self._close_browser()

    async def get_hospital_detail(self, ccn: str) -> DirectoryEntry | None:
        """Fetch detailed information for a specific hospital.

        Some directory sites have detail pages with additional metadata.

        Args:
            ccn: CMS Certification Number

        Returns:
            DirectoryEntry with full details, or None if not found
        """
        await self._ensure_browser()

        url = f"{self.BASE_URL}/hospital/{ccn}"
        page = await self._context.new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)

            # Parse detail page (structure will vary by site)
            # This is a placeholder - implement based on actual site structure
            name_el = await page.query_selector("h1")
            if not name_el:
                return None

            hospital_name = await name_el.inner_text()

            # Look for file download link
            link = await page.query_selector("a[href*='download'], a[href*='.json'], a[href*='.csv']")
            file_url = await link.get_attribute("href") if link else ""

            await self._random_delay()

            return DirectoryEntry(
                ccn=ccn,
                hospital_name=hospital_name.strip(),
                file_url=file_url,
                source=self.source_name,
            )

        except Exception as e:
            logger.warning("detail_fetch_error", ccn=ccn, error=str(e))
            return None
        finally:
            await page.close()
