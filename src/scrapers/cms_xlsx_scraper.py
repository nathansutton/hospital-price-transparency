"""CMS Standard XLSX format scraper.

Handles hospital price transparency files in Excel format (XLSX).
Common for behavioral health facilities like SUN Behavioral.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

import io

import pandas as pd
import requests

from ..utils.logger import get_logger
from .cms_csv_scraper import CMSStandardCSVScraper

logger = get_logger(__name__)

# Some behavioral health sites (sundelaware.com, sunbehavioral.com) have
# misconfigured WAF rules that block browser-like User-Agents but allow
# simple tool User-Agents like curl. Use a simple UA for these domains.
SIMPLE_UA_DOMAINS = ("sundelaware.com", "sunbehavioral.com")


class CMSStandardXLSXScraper(CMSStandardCSVScraper):
    """Scraper for XLSX files containing CMS-standard pricing data.

    This scraper:
    1. Downloads the XLSX file
    2. Reads the first sheet with pandas
    3. Converts to CSV text for parsing by the parent class

    Common patterns:
    - sundelaware.com, sunbehavioral.com - SUN Behavioral Health facilities
    """

    def _needs_simple_ua(self, url: str) -> bool:
        """Check if URL requires a simple User-Agent to avoid WAF blocks."""
        return any(domain in url.lower() for domain in SIMPLE_UA_DOMAINS)

    def fetch_data(self) -> str:
        """Fetch XLSX file and convert to CSV text.

        Returns:
            CSV text content converted from the XLSX file

        Raises:
            ValueError: If the file cannot be read as XLSX
        """
        url = self.hospital_config.file_url

        # Some servers block browser-like User-Agents; use simple UA
        if self._needs_simple_ua(url):
            self.logger.debug("using_simple_ua", url=url)
            response = requests.get(
                url,
                headers={"User-Agent": "curl/7.68.0"},
                timeout=60,
            )
            response.raise_for_status()
            xlsx_bytes = response.content
        else:
            response = self.http_client.get(url)
            xlsx_bytes = response.content

        self.logger.debug(
            "xlsx_downloaded",
            size_bytes=len(xlsx_bytes),
            url=self.hospital_config.file_url,
        )

        # Read XLSX with pandas (openpyxl engine)
        # Read all data as strings to preserve formatting consistency
        df = pd.read_excel(
            io.BytesIO(xlsx_bytes),
            sheet_name=0,  # First sheet
            header=None,  # No header - let parent class handle skiprows
            dtype=str,
            keep_default_na=False,
            engine="openpyxl",
        )

        self.logger.debug(
            "xlsx_parsed",
            rows=len(df),
            columns=len(df.columns),
        )

        # Convert to CSV string
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_text = csv_buffer.getvalue()

        self.logger.debug(
            "xlsx_converted_to_csv",
            csv_size_bytes=len(csv_text),
        )

        return csv_text
