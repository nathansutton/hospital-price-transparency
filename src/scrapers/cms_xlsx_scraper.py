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

        Handles cases where .xlsx URLs actually serve CSV or other formats.

        Returns:
            CSV text content converted from the XLSX file

        Raises:
            ValueError: If the file cannot be read as XLSX or CSV
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

        # Check if this is actually a CSV file (some servers mislabel files)
        # CSV files often start with UTF-8 BOM or text characters
        is_csv = (
            xlsx_bytes.startswith(b"\xef\xbb\xbf")  # UTF-8 BOM
            or xlsx_bytes[:20].startswith(b"hospital")
            or xlsx_bytes[:20].startswith(b'"')
            or b"," in xlsx_bytes[:100]
            and not xlsx_bytes.startswith(b"PK")  # Not a ZIP (XLSX is ZIP-based)
        )

        if is_csv:
            self.logger.info(
                "xlsx_url_is_csv",
                url=url[:80],
            )
            # Decode as CSV
            try:
                return xlsx_bytes.decode("utf-8-sig")
            except UnicodeDecodeError:
                return xlsx_bytes.decode("latin-1")

        # Read XLSX with pandas (openpyxl engine)
        # Read all data as strings to preserve formatting consistency
        try:
            df = pd.read_excel(
                io.BytesIO(xlsx_bytes),
                sheet_name=0,  # First sheet
                header=None,  # No header - let parent class handle skiprows
                dtype=str,
                keep_default_na=False,
                engine="openpyxl",
            )
        except Exception as e:
            # If XLSX parsing fails, try as CSV
            self.logger.warning(
                "xlsx_parse_failed_trying_csv",
                error=str(e)[:100],
            )
            try:
                return xlsx_bytes.decode("utf-8-sig")
            except UnicodeDecodeError:
                return xlsx_bytes.decode("latin-1")

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
