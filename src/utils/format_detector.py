"""Format detection utilities for hospital price files.

Auto-detects file format from URLs and content to enable
automatic scraper selection at scale.
"""

import re
from enum import Enum
from urllib.parse import urlparse

from ..models import DataFormat
from .logger import get_logger

logger = get_logger(__name__)


class JSONSchemaVersion(str, Enum):
    """CMS JSON schema versions."""

    CMS_V1 = "CMS_V1"  # Legacy (pre-2024)
    CMS_V2 = "CMS_V2"  # Current standard
    HYVE = "HYVE"  # Hyve Healthcare variant
    ADVENT = "ADVENT"  # Advent Health variant
    UNKNOWN = "UNKNOWN"


class FormatDetectionResult:
    """Result of format detection analysis."""

    def __init__(
        self,
        data_format: DataFormat | None,
        json_schema: JSONSchemaVersion | None = None,
        confidence: float = 0.0,
        suggested_scraper: str | None = None,
        csv_columns: dict | None = None,
    ):
        """Initialize detection result.

        Args:
            data_format: Detected file format (CSV, JSON, etc.)
            json_schema: For JSON files, the detected schema version
            confidence: Confidence score 0.0-1.0
            suggested_scraper: Suggested scraper class name
            csv_columns: For CSV, suggested column mappings
        """
        self.data_format = data_format
        self.json_schema = json_schema
        self.confidence = confidence
        self.suggested_scraper = suggested_scraper
        self.csv_columns = csv_columns or {}

    def __repr__(self) -> str:
        return (
            f"FormatDetectionResult(format={self.data_format}, "
            f"schema={self.json_schema}, confidence={self.confidence:.2f}, "
            f"scraper={self.suggested_scraper})"
        )


def detect_format_from_url(url: str) -> FormatDetectionResult:
    """Detect file format from URL.

    Analyzes file extension and URL patterns to determine format.

    Args:
        url: File URL to analyze

    Returns:
        FormatDetectionResult with detected format
    """
    url_lower = url.lower()
    parsed = urlparse(url)
    path = parsed.path.lower()

    # Check file extensions
    if path.endswith(".json") or ".json" in path:
        return FormatDetectionResult(
            data_format=DataFormat.JSON,
            confidence=0.9,
            suggested_scraper="CMSStandardJSONScraper",
        )

    if path.endswith(".csv") or ".csv" in path:
        return FormatDetectionResult(
            data_format=DataFormat.CSV,
            confidence=0.9,
            suggested_scraper="CSVScraper",
        )

    if path.endswith(".xlsx") or path.endswith(".xls"):
        return FormatDetectionResult(
            data_format=DataFormat.XLSX,
            confidence=0.9,
        )

    if path.endswith(".zip"):
        return FormatDetectionResult(
            data_format=DataFormat.ZIP,
            confidence=0.9,
        )

    if path.endswith(".xml"):
        return FormatDetectionResult(
            data_format=DataFormat.XML,
            confidence=0.9,
        )

    # Check URL patterns for common transparency platforms
    if "panaceainc.com" in url_lower:
        # Panacea platform typically serves CSV
        return FormatDetectionResult(
            data_format=DataFormat.CSV,
            confidence=0.7,
            suggested_scraper="CSVScraper",
        )

    if "hyve" in url_lower or "covenant" in url_lower:
        return FormatDetectionResult(
            data_format=DataFormat.JSON,
            json_schema=JSONSchemaVersion.HYVE,
            confidence=0.7,
            suggested_scraper="HyveCMSJSONScraper",
        )

    # Check for common MRF (Machine Readable File) patterns
    if "mrf" in url_lower or "machine-readable" in url_lower or "transparency" in url_lower:
        # Most MRF files are JSON
        return FormatDetectionResult(
            data_format=DataFormat.JSON,
            confidence=0.5,
            suggested_scraper="CMSStandardJSONScraper",
        )

    return FormatDetectionResult(
        data_format=None,
        confidence=0.0,
    )


def detect_json_schema(data: dict | list) -> JSONSchemaVersion:
    """Detect which CMS JSON schema version the data uses.

    Args:
        data: Parsed JSON data

    Returns:
        Detected schema version
    """
    # Handle list wrapper
    if isinstance(data, list) and data:
        data = data[0] if isinstance(data[0], dict) else {}

    if not isinstance(data, dict):
        return JSONSchemaVersion.UNKNOWN

    # CMS v2.0 indicators
    if "standard_charge_information" in data:
        # Check for specific charge structure
        charges = data.get("standard_charge_information", [])
        if charges and isinstance(charges[0], dict):
            first_charge = charges[0]
            if "code_information" in first_charge:
                return JSONSchemaVersion.CMS_V2

    # Legacy CMS v1 indicators
    if "chargemaster" in data or "charges" in data:
        charges = data.get("chargemaster", data.get("charges", []))
        if charges and isinstance(charges[0], dict):
            first_charge = charges[0]
            if "billing_code_information" in first_charge:
                return JSONSchemaVersion.CMS_V1

    # Hyve platform indicators
    if "hyve" in str(data.get("source", "")).lower():
        return JSONSchemaVersion.HYVE

    # Advent Health indicators
    if "advent" in str(data.get("hospital_name", "")).lower():
        return JSONSchemaVersion.ADVENT

    return JSONSchemaVersion.UNKNOWN


def detect_csv_columns(headers: list[str]) -> dict[str, str]:
    """Detect CSV column mappings for CPT, gross, and cash fields.

    Args:
        headers: List of CSV column headers

    Returns:
        Dict mapping standardized names to actual column names
    """
    headers_lower = [h.lower().strip() for h in headers]
    mappings = {}

    # CPT/HCPCS code column detection
    cpt_patterns = [
        r"^cpt",
        r"^hcpcs",
        r"^code$",
        r"^billing.?code",
        r"^procedure.?code",
        r"^service.?code",
    ]
    for i, header in enumerate(headers_lower):
        for pattern in cpt_patterns:
            if re.search(pattern, header):
                mappings["cpt"] = headers[i]
                break
        if "cpt" in mappings:
            break

    # Gross charge column detection
    gross_patterns = [
        r"^gross",
        r"^standard.?charge",
        r"^list.?price",
        r"^chargemaster",
        r"gross.?charge",
        r"standard_gross",
    ]
    for i, header in enumerate(headers_lower):
        for pattern in gross_patterns:
            if re.search(pattern, header):
                mappings["gross"] = headers[i]
                break
        if "gross" in mappings:
            break

    # Cash/discounted price column detection
    cash_patterns = [
        r"^cash",
        r"^discount",
        r"^self.?pay",
        r"cash.?price",
        r"discounted.?cash",
        r"self_pay",
    ]
    for i, header in enumerate(headers_lower):
        for pattern in cash_patterns:
            if re.search(pattern, header):
                mappings["cash"] = headers[i]
                break
        if "cash" in mappings:
            break

    return mappings


def analyze_content(
    content: bytes | str,
    url: str | None = None,
) -> FormatDetectionResult:
    """Analyze file content to detect format and schema.

    Performs deeper inspection than URL-based detection.

    Args:
        content: Raw file content
        url: Optional URL for additional context

    Returns:
        FormatDetectionResult with full analysis
    """
    # Start with URL detection if available
    result = detect_format_from_url(url) if url else FormatDetectionResult(data_format=None)

    # Convert bytes to string for text analysis
    if isinstance(content, bytes):
        try:
            content_str = content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                content_str = content.decode("latin-1")
            except UnicodeDecodeError:
                # Binary file (likely ZIP or Excel)
                if url and url.lower().endswith((".xlsx", ".xls")):
                    return FormatDetectionResult(
                        data_format=DataFormat.XLSX,
                        confidence=0.95,
                    )
                return FormatDetectionResult(
                    data_format=DataFormat.ZIP,
                    confidence=0.6,
                )
    else:
        content_str = content

    # Check for JSON
    content_stripped = content_str.strip()
    if content_stripped.startswith("{") or content_stripped.startswith("["):
        import json

        try:
            data = json.loads(content_str)
            schema = detect_json_schema(data)

            scraper_map = {
                JSONSchemaVersion.CMS_V2: "CMSStandardJSONScraper",
                JSONSchemaVersion.CMS_V1: "CMSStandardJSONScraper",
                JSONSchemaVersion.HYVE: "HyveCMSJSONScraper",
                JSONSchemaVersion.ADVENT: "AdventHealthJSONScraper",
            }

            return FormatDetectionResult(
                data_format=DataFormat.JSON,
                json_schema=schema,
                confidence=0.95 if schema != JSONSchemaVersion.UNKNOWN else 0.7,
                suggested_scraper=scraper_map.get(schema, "CMSStandardJSONScraper"),
            )
        except json.JSONDecodeError:
            pass

    # Check for CSV
    lines = content_str.split("\n")[:5]  # Check first 5 lines
    if lines:
        first_line = lines[0]
        # CSV typically has commas or tabs
        if "," in first_line or "\t" in first_line:
            delimiter = "\t" if "\t" in first_line else ","
            headers = [h.strip().strip('"') for h in first_line.split(delimiter)]

            csv_columns = detect_csv_columns(headers)

            return FormatDetectionResult(
                data_format=DataFormat.CSV,
                confidence=0.8 if csv_columns else 0.6,
                suggested_scraper="CSVScraper",
                csv_columns=csv_columns,
            )

    # Check for XML
    if content_stripped.startswith("<?xml") or content_stripped.startswith("<"):
        return FormatDetectionResult(
            data_format=DataFormat.XML,
            confidence=0.9,
        )

    return result


def suggest_scraper_config(url: str, content: bytes | str | None = None) -> dict:
    """Suggest full scraper configuration for a hospital.

    Combines URL and content analysis to recommend configuration.

    Args:
        url: File URL
        content: Optional file content for deeper analysis

    Returns:
        Dict with suggested configuration fields
    """
    if content:
        result = analyze_content(content, url)
    else:
        result = detect_format_from_url(url)

    config = {
        "file_url": url,
        "type": result.data_format.value if result.data_format else None,
        "scraper_type": result.suggested_scraper,
    }

    # Add CSV column mappings if detected
    if result.csv_columns:
        if "cpt" in result.csv_columns:
            config["cpt"] = result.csv_columns["cpt"]
        if "gross" in result.csv_columns:
            config["gross"] = result.csv_columns["gross"]
        if "cash" in result.csv_columns:
            config["cash"] = result.csv_columns["cash"]

    logger.debug(
        "scraper_config_suggested",
        url=url,
        format=result.data_format,
        confidence=result.confidence,
    )

    return config
