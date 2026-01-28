"""CMS Standard CSV format scraper (v2.0).

Handles the CMS-mandated standard CSV format used by many hospital systems.
This format has multiple header rows and pipe-delimited multi-value columns.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

import csv
import io
from pathlib import Path
from typing import Iterator

import pandas as pd

from ..utils.logger import get_logger
from .base import BaseScraper

logger = get_logger(__name__)

# Encodings to try when UTF-8 fails (common in hospital data files)
FALLBACK_ENCODINGS = ["utf-8", "cp1252", "latin-1", "iso-8859-1"]

# ZIP file magic bytes
ZIP_MAGIC = b"PK\x03\x04"

# Files larger than this will be processed in chunks (100 MB)
LARGE_FILE_THRESHOLD = 100 * 1024 * 1024

# Chunk size for reading large CSV files (number of rows)
CSV_CHUNK_SIZE = 50000


class CMSStandardCSVScraper(BaseScraper):
    """Scraper for CMS-standard machine-readable CSV files (v2.0 format).

    The CMS 2.0 CSV format has:
    - Row 0: Hospital metadata
    - Row 1: Hospital name and details
    - Row 2: Column headers
    - Row 3+: Data rows

    Columns include:
    - description
    - code|1, code|1|type, code|2, code|2|type, etc.
    - standard_charge_*

    Also handles:
    - ZIP files served with .csv URLs (Baptist Health pattern)
    - Pipe-delimited files (Craneware pattern)
    - Large files (>100MB) via chunked processing
    """

    # Flag to indicate if content was extracted from ZIP
    _from_zip: bool = False
    # Path to temp file for large file processing
    _temp_file: Path | None = None
    # Flag for large file chunked processing
    _use_chunked: bool = False

    def fetch_data(self) -> str | Path:
        """Fetch CSV data from the URL.

        Handles the case where a URL with .csv extension actually serves
        a ZIP file (common with Baptist Health).

        For large files (>100MB), streams to a temp file to avoid memory issues.

        Returns:
            str: CSV text content for normal files
            Path: Path to temp file for large files (use chunked parsing)
        """
        url = self.hospital_config.file_url

        # Check file size first for large file handling
        content_length = self.http_client.get_content_length(url)
        if content_length and content_length > LARGE_FILE_THRESHOLD:
            self.logger.info(
                "large_file_detected",
                size_mb=content_length / (1024 * 1024),
                threshold_mb=LARGE_FILE_THRESHOLD / (1024 * 1024),
            )
            self._use_chunked = True
            self._temp_file = self.http_client.stream_to_tempfile(url)
            return self._temp_file

        # Normal fetch for smaller files
        response = self.http_client.get(url)

        # Check if this is actually a ZIP file despite the URL
        content_type = response.headers.get("content-type", "").lower()
        is_zip_content = (
            "zip" in content_type
            or response.content[:4] == ZIP_MAGIC
        )

        if is_zip_content:
            self.logger.info(
                "csv_url_serves_zip",
                url=url[:80],
                content_type=content_type,
            )
            # Extract CSV from ZIP using the ZIP scraper's logic
            self._from_zip = True
            return self._extract_from_zip(response.content)

        # Check if server returned HTML instead of CSV (common error page or redirect)
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" in content_type:
            raise ValueError(
                f"Server returned HTML instead of CSV (Content-Type: {content_type})"
            )

        # Try multiple encodings - servers often lie about encoding
        # Many hospital files are ISO-8859-1 but served as UTF-8
        raw_bytes = response.content
        for encoding in FALLBACK_ENCODINGS:
            try:
                text = raw_bytes.decode(encoding)
                # Quick sanity check - valid CSV should have printable chars
                # in first line (header row)
                first_line = text.split('\n')[0][:100] if text else ''

                # Check for HTML content (error pages, redirects, JavaScript apps)
                first_lower = first_line.lower()
                if first_lower.startswith("<!doctype html") or first_lower.startswith("<html"):
                    raise ValueError(
                        "Server returned HTML instead of CSV (URL may have moved or require auth)"
                    )

                # Check for garbage characters (control chars except \t)
                if not any(ord(c) < 32 and c not in '\t\r\n' for c in first_line):
                    return text
            except UnicodeDecodeError:
                continue
        # If all fail, return whatever requests decoded
        return response.text

    def _extract_from_zip(self, zip_bytes: bytes) -> str:
        """Extract CSV content from a ZIP file.

        Args:
            zip_bytes: Raw ZIP file bytes

        Returns:
            CSV text content
        """
        import json
        import zipfile

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            all_files = zf.namelist()

            # Find CSV files
            csv_files = [name for name in all_files if name.lower().endswith(".csv")]

            if csv_files:
                target_file = csv_files[0]
                raw_bytes = zf.read(target_file)

                # Try multiple encodings
                for encoding in FALLBACK_ENCODINGS:
                    try:
                        return raw_bytes.decode(encoding)
                    except UnicodeDecodeError:
                        continue

                # If all fail, let it raise
                return raw_bytes.decode("utf-8")

            # Check for JSON files (some ZIPs contain JSON)
            json_files = [name for name in all_files if name.lower().endswith(".json")]
            if json_files:
                # This shouldn't happen for CSV scraper, but handle gracefully
                raise ValueError(
                    f"ZIP contains JSON, not CSV. Use ZIP scraper. Contents: {all_files}"
                )

            raise ValueError(f"No CSV file found in ZIP. Contents: {all_files}")

    def _detect_delimiter(self, text: str) -> str:
        """Detect the delimiter used in CSV text.

        Args:
            text: CSV text content

        Returns:
            Detected delimiter character
        """
        # Get first few lines for analysis
        lines = text.split("\n")[:10]
        sample = "\n".join(lines)

        # Use csv.Sniffer to detect delimiter
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",|\t;")
            return dialect.delimiter
        except csv.Error:
            # Default to comma if detection fails
            return ","

    def _extract_records_from_df(self, df: pd.DataFrame) -> list[dict]:
        """Extract price records from a DataFrame chunk.

        Args:
            df: DataFrame with CSV data

        Returns:
            List of record dicts with vocabulary_id, concept_code, gross, cash
        """
        records = []

        # Create case-insensitive column lookup (also strip trailing spaces)
        col_map = {c.lower().strip(): c for c in df.columns}
        col_names_lower = list(col_map.keys())
        is_craneware = "hcpcs" in col_names_lower or "service_code" in col_names_lower
        # Simple 'code' column format (e.g., Google Drive files, some proprietary formats)
        has_simple_code = "code" in col_names_lower and "code|1" not in col_names_lower

        for _, row in df.iterrows():
            codes = []

            if is_craneware:
                # Craneware format: HCPCS column contains the code
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower in ("hcpcs", "medicare_hcpcs", "cpt", "cpt4"):
                        code = str(row.get(col, "")).strip()
                        if code and len(code) == 5:
                            # Assume HCPCS unless column name says CPT
                            code_type = "CPT" if "cpt" in col_lower else "HCPCS"
                            codes.append((code, code_type))
            elif has_simple_code:
                # Simple 'Code' column format - assume CPT/HCPCS based on code pattern
                code_col = col_map.get("code")
                if code_col:
                    code = str(row.get(code_col, "")).strip()
                    # 5-digit numeric codes are likely CPT/HCPCS
                    if code and len(code) == 5 and code.isdigit():
                        # CPT codes are 5 digits, HCPCS start with letter (handled by normalizer)
                        codes.append((code, "CPT"))
            else:
                # CMS format: code|1, code|1|type, code|2, code|2|type, etc.
                # Use case-insensitive matching for column names
                for i in range(1, 10):  # Support up to 9 codes per row
                    code_col_lower = f"code|{i}"
                    type_col_lower = f"code|{i}|type"

                    # Get actual column names (may have different case or trailing spaces)
                    code_col = col_map.get(code_col_lower) or col_map.get(code_col_lower.rstrip())
                    type_col = col_map.get(type_col_lower) or col_map.get(type_col_lower.rstrip())

                    if code_col and type_col:
                        code = str(row.get(code_col, "")).strip()
                        code_type = str(row.get(type_col, "")).strip().upper()

                        if code and code_type in ("CPT", "HCPCS", "CPT4"):
                            codes.append((code, code_type))

            if not codes:
                continue

            # Find gross charge and cash price
            gross = None
            cash = None

            # Look for price columns (various naming conventions)
            for col in df.columns:
                col_lower = col.lower()
                val = row.get(col, "")

                # Gross/list price columns
                if gross is None and any(
                    x in col_lower for x in ["gross", "price", "charge", "amount"]
                ):
                    # Avoid matching "discounted" or "cash" columns
                    if not any(x in col_lower for x in ["cash", "discounted", "negotiated"]):
                        try:
                            gross = float(str(val).replace(",", "").replace("$", ""))
                        except (ValueError, TypeError):
                            pass

                # Cash/discounted price columns
                if cash is None and any(
                    x in col_lower for x in ["cash", "discounted", "self_pay"]
                ):
                    try:
                        cash = float(str(val).replace(",", "").replace("$", ""))
                    except (ValueError, TypeError):
                        pass

            # Create a record for each valid code
            for code, code_type in codes:
                vocab_id = "cpt" if code_type in ("CPT", "CPT4") else "hcpcs"
                records.append(
                    {
                        "vocabulary_id": vocab_id,
                        "concept_code": code,
                        "gross": gross,
                        "cash": cash,
                    }
                )

        return records

    def _read_csv_chunked(self, file_path: Path, skiprows: int, delimiter: str) -> Iterator[pd.DataFrame]:
        """Read a large CSV file in chunks.

        Args:
            file_path: Path to the CSV file
            skiprows: Number of rows to skip at the start
            delimiter: Column delimiter

        Yields:
            DataFrame chunks
        """
        # Try to detect encoding from first few bytes
        with open(file_path, "rb") as f:
            sample = f.read(1024)

        encoding = "utf-8"
        for enc in FALLBACK_ENCODINGS:
            try:
                sample.decode(enc)
                encoding = enc
                break
            except UnicodeDecodeError:
                continue

        self.logger.info("chunked_csv_read_start", encoding=encoding, chunk_size=CSV_CHUNK_SIZE)

        try:
            reader = pd.read_csv(
                file_path,
                skiprows=skiprows,
                dtype=str,
                keep_default_na=False,
                delimiter=delimiter,
                on_bad_lines="skip",
                encoding=encoding,
                chunksize=CSV_CHUNK_SIZE,
            )
            for chunk in reader:
                yield chunk
        except Exception as e:
            self.logger.warning("chunked_csv_read_error", error=str(e)[:100])
            # Fall back to single read with Python engine
            df = pd.read_csv(
                file_path,
                skiprows=skiprows,
                dtype=str,
                keep_default_na=False,
                delimiter=delimiter,
                on_bad_lines="skip",
                encoding=encoding,
                engine="python",
            )
            yield df

    def parse_data(self, raw_data: bytes | str | dict | list | Path) -> pd.DataFrame:
        """Parse CMS standard CSV format (v2.0).

        Args:
            raw_data: CSV text content, or Path to temp file for large files

        Returns:
            DataFrame with vocabulary_id, concept_code, gross, cash columns
        """
        # Handle large file chunked processing
        if isinstance(raw_data, Path):
            return self._parse_large_file(raw_data)

        if isinstance(raw_data, bytes):
            # Try multiple encodings (Windows-generated files often use cp1252)
            for encoding in FALLBACK_ENCODINGS:
                try:
                    raw_data = raw_data.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                # If all fail, try UTF-8 and let it raise
                raw_data = raw_data.decode("utf-8")
        if not isinstance(raw_data, str):
            raise ValueError(f"Expected str, bytes, or Path, got {type(raw_data).__name__}")

        # Detect delimiter (handles pipe-delimited Craneware files)
        delimiter = self._detect_delimiter(raw_data)
        if delimiter != ",":
            self.logger.info("detected_delimiter", delimiter=repr(delimiter))

        # Detect skiprows based on file format
        # CMS format has 2 header rows (metadata + hospital info)
        # Other formats (Craneware) have column headers on row 0
        skiprows = 2
        first_line = raw_data.split("\n")[0].lower() if raw_data else ""

        # Check for non-CMS formats that have headers on row 0
        if delimiter == "|":
            # Craneware pipe-delimited format - headers on row 0
            skiprows = 0
        elif "service_code" in first_line or "hcpcs" in first_line:
            # Non-CMS format with column headers on first line
            skiprows = 0
        elif "hospital_name" not in first_line and "description" in first_line:
            # Looks like data columns on first line (not CMS metadata)
            skiprows = 0

        # Read CSV with detected delimiter
        # Use on_bad_lines='skip' to handle malformed rows gracefully
        # First try C parser, fall back to Python parser for malformed files
        df = None
        try:
            df = pd.read_csv(
                io.StringIO(raw_data),
                skiprows=skiprows,
                dtype=str,
                keep_default_na=False,
                low_memory=False,
                delimiter=delimiter,
                on_bad_lines="skip",
                engine="c",
            )
        except pd.errors.ParserError as e:
            # C parser failed (buffer overflow, EOF in string, etc.)
            self.logger.warning(
                "csv_c_parser_failed_using_python",
                error=str(e)[:100],
            )

        # Fall back to Python parser if C parser failed
        if df is None:
            try:
                df = pd.read_csv(
                    io.StringIO(raw_data),
                    skiprows=skiprows,
                    dtype=str,
                    keep_default_na=False,
                    delimiter=delimiter,
                    on_bad_lines="skip",
                    engine="python",
                    quoting=csv.QUOTE_NONE,  # Ignore quote characters entirely
                )
            except Exception as e:
                # Last resort: try to normalize line endings and parse line-by-line
                self.logger.warning(
                    "csv_python_parser_failed_line_by_line",
                    error=str(e)[:100],
                )
                # Replace all embedded newlines (CR, CRLF variations) with space
                # keeping only actual line terminators
                import re
                normalized = re.sub(r'\r\n|\r|\n(?=[^,\n]*,)', ' ', raw_data)
                df = pd.read_csv(
                    io.StringIO(normalized),
                    skiprows=skiprows,
                    dtype=str,
                    keep_default_na=False,
                    delimiter=delimiter,
                    on_bad_lines="skip",
                    engine="python",
                )

        # Normalize column names: strip whitespace around pipes and lowercase
        # CMS 3.0 uses "code | 1 | type" while 2.0 uses "code|1|type"
        df.columns = [
            col.replace(" | ", "|").replace("| ", "|").replace(" |", "|") for col in df.columns
        ]

        self.logger.debug("csv_columns", columns=list(df.columns)[:20])

        # Extract records using shared logic
        records = self._extract_records_from_df(df)

        self.logger.debug("cms_csv_parsed", records=len(records))
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(columns=["vocabulary_id", "concept_code", "gross", "cash"])
        )

    def _parse_large_file(self, file_path: Path) -> pd.DataFrame:
        """Parse a large CSV file using chunked processing.

        Args:
            file_path: Path to the temp file containing CSV data

        Returns:
            DataFrame with vocabulary_id, concept_code, gross, cash columns
        """
        try:
            # Read first few lines to detect format
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                first_lines = "".join(f.readline() for _ in range(10))

            # Detect delimiter
            delimiter = self._detect_delimiter(first_lines)
            if delimiter != ",":
                self.logger.info("detected_delimiter", delimiter=repr(delimiter))

            # Detect skiprows
            skiprows = 2
            first_line = first_lines.split("\n")[0].lower() if first_lines else ""
            if delimiter == "|":
                skiprows = 0
            elif "service_code" in first_line or "hcpcs" in first_line:
                skiprows = 0
            elif "hospital_name" not in first_line and "description" in first_line:
                skiprows = 0

            # Process in chunks
            all_records = []
            chunk_count = 0

            for chunk_df in self._read_csv_chunked(file_path, skiprows, delimiter):
                # Normalize column names
                chunk_df.columns = [
                    col.replace(" | ", "|").replace("| ", "|").replace(" |", "|")
                    for col in chunk_df.columns
                ]

                if chunk_count == 0:
                    self.logger.debug("csv_columns", columns=list(chunk_df.columns)[:20])

                # Extract records from this chunk
                chunk_records = self._extract_records_from_df(chunk_df)
                all_records.extend(chunk_records)
                chunk_count += 1

                if chunk_count % 10 == 0:
                    self.logger.debug(
                        "chunked_processing_progress",
                        chunks=chunk_count,
                        records_so_far=len(all_records),
                    )

            self.logger.info(
                "chunked_processing_complete",
                chunks=chunk_count,
                total_records=len(all_records),
            )

            return (
                pd.DataFrame(all_records)
                if all_records
                else pd.DataFrame(columns=["vocabulary_id", "concept_code", "gross", "cash"])
            )
        finally:
            # Clean up temp file
            if file_path.exists():
                file_path.unlink()
                self.logger.debug("temp_file_cleaned", path=str(file_path))


class TennovaCMSCSVScraper(CMSStandardCSVScraper):
    """Scraper for Tennova Healthcare CSV files (CMS 2.0 format).

    Tennova files follow the CMS standard CSV format.
    """

    pass  # Same implementation as parent
