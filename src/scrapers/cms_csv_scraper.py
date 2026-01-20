"""CMS Standard CSV format scraper (v2.0).

Handles the CMS-mandated standard CSV format used by many hospital systems.
This format has multiple header rows and pipe-delimited multi-value columns.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

import io

import pandas as pd

from ..utils.logger import get_logger
from .base import BaseScraper

logger = get_logger(__name__)


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
    """

    def fetch_data(self) -> str:
        """Fetch CSV data from the URL."""
        response = self.http_client.get(self.hospital_config.file_url)
        return response.text

    def parse_data(self, raw_data: bytes | str | dict | list) -> pd.DataFrame:
        """Parse CMS standard CSV format (v2.0).

        Args:
            raw_data: CSV text content

        Returns:
            DataFrame with vocabulary_id, concept_code, gross, cash columns
        """
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8")
        if not isinstance(raw_data, str):
            raise ValueError(f"Expected str or bytes, got {type(raw_data).__name__}")
        # Read with header row at row 2 (0-indexed)
        df = pd.read_csv(
            io.StringIO(raw_data),
            skiprows=2,
            dtype=str,
            keep_default_na=False,
            low_memory=False,
        )

        # Normalize column names: strip whitespace around pipes and lowercase
        # CMS 3.0 uses "code | 1 | type" while 2.0 uses "code|1|type"
        df.columns = [
            col.replace(" | ", "|").replace("| ", "|").replace(" |", "|") for col in df.columns
        ]

        self.logger.debug("csv_columns", columns=list(df.columns)[:20])

        records = []

        for _, row in df.iterrows():
            # Find all code columns (code|1, code|2, etc.)
            codes = []
            for i in range(1, 10):  # Support up to 9 codes per row
                code_col = f"code|{i}"
                type_col = f"code|{i}|type"

                if code_col in df.columns and type_col in df.columns:
                    code = str(row.get(code_col, "")).strip()
                    code_type = str(row.get(type_col, "")).strip().upper()

                    if code and code_type in ("CPT", "HCPCS", "CPT4"):
                        codes.append((code, code_type))

            if not codes:
                continue

            # Find gross charge and cash price
            gross = None
            cash = None

            # Look for standard_charge_gross or similar columns
            for col in df.columns:
                col_lower = col.lower()
                val = row.get(col, "")

                if "gross" in col_lower and gross is None:
                    try:
                        gross = float(str(val).replace(",", "").replace("$", ""))
                    except (ValueError, TypeError):
                        pass

                if ("cash" in col_lower or "discounted" in col_lower) and cash is None:
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

        self.logger.debug("cms_csv_parsed", records=len(records))
        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(columns=["vocabulary_id", "concept_code", "gross", "cash"])
        )


class TennovaCMSCSVScraper(CMSStandardCSVScraper):
    """Scraper for Tennova Healthcare CSV files (CMS 2.0 format).

    Tennova files follow the CMS standard CSV format.
    """

    pass  # Same implementation as parent
