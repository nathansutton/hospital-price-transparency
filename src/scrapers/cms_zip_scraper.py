"""CMS Standard ZIP format scraper.

Handles hospital price transparency files distributed as ZIP archives
containing CMS-standard CSV files. This is common for Panacea/Trinity
Health endpoints.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

import io
import zipfile

from ..utils.logger import get_logger
from .cms_csv_scraper import CMSStandardCSVScraper

logger = get_logger(__name__)


class CMSStandardZIPScraper(CMSStandardCSVScraper):
    """Scraper for ZIP archives containing CMS-standard CSV files.

    This scraper:
    1. Downloads the ZIP file
    2. Extracts the first CSV file found
    3. Parses it using the standard CMS CSV format

    Common patterns:
    - trinityhealth.pt.panaceainc.com - Trinity Health via Panacea
    """

    def fetch_data(self) -> str:
        """Fetch ZIP file and extract the CSV content.

        Returns:
            CSV text content from the first CSV file in the archive

        Raises:
            ValueError: If no CSV file found in ZIP
            zipfile.BadZipFile: If the downloaded file is not a valid ZIP
        """
        # Download the ZIP file
        response = self.http_client.get(self.hospital_config.file_url)
        zip_bytes = response.content

        self.logger.debug(
            "zip_downloaded",
            size_bytes=len(zip_bytes),
            url=self.hospital_config.file_url,
        )

        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find CSV files in the archive
            csv_files = [
                name for name in zf.namelist()
                if name.lower().endswith('.csv')
            ]

            if not csv_files:
                raise ValueError(
                    f"No CSV file found in ZIP archive. "
                    f"Contents: {zf.namelist()}"
                )

            # Use the first CSV file (most archives have only one)
            csv_filename = csv_files[0]

            if len(csv_files) > 1:
                self.logger.warning(
                    "multiple_csv_in_zip",
                    files=csv_files,
                    using=csv_filename,
                )

            # Extract and decode the CSV
            csv_bytes = zf.read(csv_filename)
            csv_text = csv_bytes.decode('utf-8')

            self.logger.debug(
                "csv_extracted",
                filename=csv_filename,
                size_bytes=len(csv_bytes),
            )

            return csv_text
