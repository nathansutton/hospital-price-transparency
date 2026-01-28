"""CMS Standard ZIP format scraper.

Handles hospital price transparency files distributed as ZIP archives
containing CMS-standard CSV or JSON files. This is common for Panacea/Trinity
Health endpoints, Northwell, Baptist Health, and Brown Health systems.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

import io
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Any

import pandas as pd

from ..utils.logger import get_logger
from .base import BaseScraper
from .cms_csv_scraper import CMSStandardCSVScraper
from .cms_json_scraper import CMSStandardJSONScraper
from .cms_xlsx_scraper import CMSStandardXLSXScraper

logger = get_logger(__name__)

# Files that indicate Office Open XML format (XLSX, DOCX, etc.)
OOXML_MARKERS = {"[Content_Types].xml", "_rels/.rels", "xl/workbook.xml"}

# Encodings to try when UTF-8 fails (common in hospital data files)
FALLBACK_ENCODINGS = ["utf-8", "cp1252", "latin-1", "iso-8859-1"]


class CMSStandardZIPScraper(BaseScraper):
    """Scraper for ZIP archives containing CMS-standard CSV or JSON files.

    This scraper:
    1. Downloads the ZIP file
    2. Extracts the first CSV or JSON file found (CSV preferred)
    3. Parses it using the appropriate CMS format parser

    Common patterns:
    - trinityhealth.pt.panaceainc.com - Trinity Health via Panacea
    - northwell.edu - Northwell Health (JSON in ZIP)
    - baptisthealthal.com - Baptist Health Alabama (JSON in ZIP)
    - brownhealth.org - Brown Health Rhode Island (JSON in ZIP)
    """

    # Content type detected during fetch
    _content_type: str = "csv"
    _extracted_filename: str = ""

    def _decode_with_fallback(self, raw_bytes: bytes, filename: str) -> str:
        """Decode bytes trying multiple encodings.

        Args:
            raw_bytes: Raw file bytes
            filename: Filename for logging

        Returns:
            Decoded string

        Raises:
            UnicodeDecodeError: If all encodings fail
        """
        last_error = None
        for encoding in FALLBACK_ENCODINGS:
            try:
                return raw_bytes.decode(encoding)
            except UnicodeDecodeError as e:
                last_error = e
                self.logger.debug(
                    "encoding_failed",
                    encoding=encoding,
                    filename=filename,
                )
                continue

        # If we get here, all encodings failed
        raise last_error or UnicodeDecodeError(
            "utf-8", raw_bytes, 0, len(raw_bytes), "All fallback encodings failed"
        )

    def _extract_with_system_unzip(self, zip_bytes: bytes, target_file: str) -> bytes:
        """Extract a file using system unzip (fallback for unsupported compression).

        Some ZIP files use Deflate64 or other compression methods not supported
        by Python's zipfile module. This falls back to the system unzip command.

        Args:
            zip_bytes: Raw ZIP file bytes
            target_file: Name of file to extract from the archive

        Returns:
            Raw bytes of the extracted file

        Raises:
            NotImplementedError: If system unzip is not available
            RuntimeError: If extraction fails
        """
        # Check if unzip is available
        if not shutil.which("unzip"):
            raise NotImplementedError(
                "ZIP uses unsupported compression and system 'unzip' not available"
            )

        self.logger.info(
            "using_system_unzip",
            reason="unsupported compression method",
            target=target_file,
        )

        # Create temp directory for extraction
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, "archive.zip")
            output_path = os.path.join(tmpdir, target_file)

            # Write ZIP to temp file
            with open(zip_path, "wb") as f:
                f.write(zip_bytes)

            # Extract specific file using system unzip
            try:
                result = subprocess.run(
                    ["unzip", "-o", "-q", zip_path, target_file, "-d", tmpdir],
                    capture_output=True,
                    timeout=300,  # 5 minute timeout
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        f"unzip failed: {result.stderr.decode('utf-8', errors='replace')}"
                    )
            except subprocess.TimeoutExpired as e:
                raise RuntimeError("unzip timed out after 300 seconds") from e

            # Read extracted file
            # Handle case where file might be in a subdirectory
            if os.path.exists(output_path):
                with open(output_path, "rb") as f:
                    return f.read()

            # Search for the file (might be in a subdirectory)
            for root, _dirs, files in os.walk(tmpdir):
                for fname in files:
                    if fname == os.path.basename(target_file):
                        with open(os.path.join(root, fname), "rb") as f:
                            return f.read()

            raise RuntimeError(f"Extracted file not found: {target_file}")

    def fetch_data(self) -> str | dict[Any, Any] | bytes:
        """Fetch ZIP file and extract the CSV or JSON content.

        Returns:
            CSV text content, parsed JSON dict, or raw XLSX bytes from the archive

        Raises:
            ValueError: If no CSV or JSON file found in ZIP
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

        # Try to open as ZIP
        try:
            zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        except zipfile.BadZipFile:
            # Not a valid ZIP - might be CSV/JSON served with wrong extension
            # Try to detect format from content
            if zip_bytes.startswith(b"{") or zip_bytes.startswith(b"["):
                self._content_type = "json"
                json_result: dict[str, Any] = json.loads(zip_bytes.decode("utf-8-sig"))
                return json_result
            else:
                # Assume CSV
                self._content_type = "csv"
                for encoding in FALLBACK_ENCODINGS:
                    try:
                        return zip_bytes.decode(encoding)
                    except UnicodeDecodeError:
                        continue
                return zip_bytes.decode("utf-8")

        # Extract content from ZIP
        with zf:
            all_files = zf.namelist()

            # Check if this is actually an XLSX file (Office Open XML format)
            # XLSX files are ZIP archives containing [Content_Types].xml and xl/ folder
            is_xlsx = any(marker in all_files for marker in OOXML_MARKERS)
            if is_xlsx:
                self._content_type = "xlsx"
                self.logger.info(
                    "zip_is_xlsx",
                    url=self.hospital_config.file_url[:80],
                )
                # Return raw bytes for XLSX processing
                return zip_bytes

            # Find CSV files in the archive (preferred)
            csv_files = [name for name in all_files if name.lower().endswith(".csv")]

            # Find JSON files in the archive (fallback)
            json_files = [name for name in all_files if name.lower().endswith(".json")]

            if csv_files:
                # Use CSV (preferred format)
                self._content_type = "csv"
                target_file = csv_files[0]

                if len(csv_files) > 1:
                    self.logger.warning(
                        "multiple_csv_in_zip",
                        files=csv_files,
                        using=target_file,
                    )

                # Extract and decode the CSV with fallback encoding
                try:
                    raw_bytes = zf.read(target_file)
                except NotImplementedError:
                    # Compression method not supported (e.g., Deflate64)
                    # Fall back to system unzip
                    raw_bytes = self._extract_with_system_unzip(zip_bytes, target_file)

                content = self._decode_with_fallback(raw_bytes, target_file)

                self.logger.debug(
                    "csv_extracted",
                    filename=target_file,
                    size_bytes=len(raw_bytes),
                )
                self._extracted_filename = target_file
                return content

            elif json_files:
                # Fall back to JSON
                self._content_type = "json"
                target_file = json_files[0]

                if len(json_files) > 1:
                    self.logger.warning(
                        "multiple_json_in_zip",
                        files=json_files,
                        using=target_file,
                    )

                # Extract and decode the JSON
                try:
                    raw_bytes = zf.read(target_file)
                except NotImplementedError:
                    # Compression method not supported (e.g., Deflate64)
                    # Fall back to system unzip
                    raw_bytes = self._extract_with_system_unzip(zip_bytes, target_file)

                content = self._decode_with_fallback(raw_bytes, target_file)

                self.logger.debug(
                    "json_extracted",
                    filename=target_file,
                    size_bytes=len(raw_bytes),
                )
                self._extracted_filename = target_file
                json_data: dict[str, Any] = json.loads(content)
                return json_data

            else:
                raise ValueError(
                    f"No CSV or JSON file found in ZIP archive. Contents: {all_files[:10]}"
                )

    def parse_data(self, raw_data: str | dict | list | bytes | Path) -> pd.DataFrame:
        """Parse the extracted content using the appropriate parser.

        Args:
            raw_data: CSV text, parsed JSON data, raw XLSX bytes, or Path to temp file

        Returns:
            DataFrame with vocabulary_id, concept_code, gross, cash columns
        """
        if self._content_type == "json":
            # Use JSON parser
            json_parser = CMSStandardJSONScraper(
                hospital_config=self.hospital_config,
                scraper_config=self.scraper_config,
                http_client=self.http_client,
                normalizer=self.normalizer,
            )
            return json_parser.parse_data(raw_data)
        elif self._content_type == "xlsx":
            # Use XLSX parser - convert bytes to CSV text first
            xlsx_parser = CMSStandardXLSXScraper(
                hospital_config=self.hospital_config,
                scraper_config=self.scraper_config,
                http_client=self.http_client,
                normalizer=self.normalizer,
            )
            # Parse XLSX bytes directly
            df = pd.read_excel(  # type: ignore[call-overload]
                io.BytesIO(raw_data),  # type: ignore[arg-type]
                sheet_name=0,
                header=None,
                dtype=str,
                keep_default_na=False,
                engine="openpyxl",
            )
            # Convert to CSV string for parent parser
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_text = csv_buffer.getvalue()
            return xlsx_parser.parse_data(csv_text)
        else:
            # Use CSV parser
            csv_parser = CMSStandardCSVScraper(
                hospital_config=self.hospital_config,
                scraper_config=self.scraper_config,
                http_client=self.http_client,
                normalizer=self.normalizer,
            )
            return csv_parser.parse_data(raw_data)
