"""CMS Standard JSON format scraper (v2.0).

Handles the CMS-mandated standard JSON format used by many hospital systems.
This format was updated in 2024 and uses different field names than earlier versions.

Reference: https://www.cms.gov/hospital-price-transparency/resources
"""

from typing import Any

import pandas as pd

from ..utils.logger import get_logger
from .base import BaseScraper

logger = get_logger(__name__)


class CMSStandardJSONScraper(BaseScraper):
    """Scraper for CMS-standard machine-readable JSON files (v2.0 format).

    The CMS 2.0 schema includes:
    - hospital_name, hospital_location at top level
    - standard_charge_information array with charge details

    Each charge item contains:
    - description
    - code_information (array of {code, type})
    - standard_charges (array with gross_charge, discounted_cash, etc.)

    This scraper handles field name variations across different hospital
    implementations while maintaining strict CPT/HCPCS code validation.
    """

    # Field name variations for finding charge items array
    CHARGE_ARRAY_FIELDS = [
        "standard_charge_information",
        "charges",
        "standard_charges",
        "items",
        "chargemaster",
        "charge_information",
    ]

    # Field name variations for code information
    CODE_INFO_FIELDS = [
        "code_information",
        "billing_code_information",
        "billing_codes",
        "codes",
        "code_info",
        "billing_code",
    ]

    # Field name variations for the code value itself
    CODE_VALUE_FIELDS = ["code", "billing_code", "code_value", "cpt", "hcpcs"]

    # Field name variations for code type
    CODE_TYPE_FIELDS = ["type", "code_type", "billing_code_type", "code_system"]

    # Field name variations for gross charges
    GROSS_FIELDS = [
        "gross_charge",
        "gross",
        "gross_charges",
        "standard_charge",
        "charge",
        "list_price",
        "chargemaster_price",
        "maximum",
    ]

    # Field name variations for cash/discounted prices
    CASH_FIELDS = [
        "discounted_cash",
        "discounted_cash_price",
        "cash",
        "cash_price",
        "self_pay",
        "self_pay_price",
        "minimum",
        "cash_discount",
    ]

    # Valid CPT/HCPCS code type identifiers
    VALID_CODE_TYPES = {"CPT", "CPT4", "HCPCS", "CPT-4", "HCPC"}

    def fetch_data(self) -> dict[Any, Any]:
        """Fetch JSON data from the URL."""
        result = self.http_client.get_json(self.hospital_config.file_url)
        return dict(result) if isinstance(result, dict) else {}

    def _get_first_match(self, data: dict[str, Any], fields: list[str], default: Any = None) -> Any:
        """Get the first matching field value from a dict."""
        for field in fields:
            if field in data:
                return data[field]
        return default

    def _find_charges_array(self, data: dict[str, Any] | list[Any]) -> list[Any]:
        """Find the main charges array in the data structure.

        Handles various data structures:
        - Direct list at root
        - Dict with charges array at known field
        - Nested structures
        """
        if isinstance(data, list):
            # Check if this is the charges array directly
            if data and isinstance(data[0], dict):
                # If first item looks like a charge item, use it
                if any(f in data[0] for f in self.CODE_INFO_FIELDS + ["code", "description"]):
                    return list(data)
                # Otherwise, might be a wrapper - check first item
                for field in self.CHARGE_ARRAY_FIELDS:
                    if field in data[0]:
                        return list(data[0][field])
            return list(data)

        if isinstance(data, dict):
            # Try direct field access
            for field in self.CHARGE_ARRAY_FIELDS:
                if field in data and isinstance(data[field], list):
                    return list(data[field])

            # Try nested access (e.g., data.charges.items)
            for field in self.CHARGE_ARRAY_FIELDS:
                if field in data and isinstance(data[field], dict):
                    nested = data[field]
                    for inner_field in self.CHARGE_ARRAY_FIELDS:
                        if inner_field in nested and isinstance(nested[inner_field], list):
                            return list(nested[inner_field])

        return []

    def _extract_codes(self, item: dict) -> list[tuple[str, str]]:
        """Extract all valid codes from a charge item.

        Returns list of (code, vocabulary_id) tuples.
        """
        codes = []

        # Try to find code_information array
        code_info_list = self._get_first_match(item, self.CODE_INFO_FIELDS, [])

        # Ensure it's a list
        if isinstance(code_info_list, dict):
            code_info_list = [code_info_list]
        elif not isinstance(code_info_list, list):
            code_info_list = []

        for code_info in code_info_list:
            if not isinstance(code_info, dict):
                continue

            code = self._get_first_match(code_info, self.CODE_VALUE_FIELDS, "")
            code_type = self._get_first_match(code_info, self.CODE_TYPE_FIELDS, "")

            if not code:
                continue

            # Normalize code type
            code_type_upper = str(code_type).upper().replace("-", "")
            if code_type_upper not in self.VALID_CODE_TYPES:
                continue

            # Map code type to vocabulary_id
            vocab_id = "hcpcs" if code_type_upper in ("HCPCS", "HCPC") else "cpt"
            codes.append((str(code).strip(), vocab_id))

        # Also check for direct code field on the item
        if not codes:
            direct_code = self._get_first_match(item, self.CODE_VALUE_FIELDS)
            direct_type = self._get_first_match(item, self.CODE_TYPE_FIELDS, "CPT")
            if direct_code:
                code_type_upper = str(direct_type).upper().replace("-", "")
                if code_type_upper in self.VALID_CODE_TYPES:
                    vocab_id = "hcpcs" if code_type_upper in ("HCPCS", "HCPC") else "cpt"
                    codes.append((str(direct_code).strip(), vocab_id))

        return codes

    def _extract_prices(self, item: dict) -> tuple[float | None, float | None]:
        """Extract gross and cash prices from a charge item.

        Returns (gross, cash) tuple.
        """
        gross = None
        cash = None

        # First try direct fields on the item
        for field in self.GROSS_FIELDS:
            if field in item and item[field] is not None:
                try:
                    gross = float(item[field])
                    break
                except (ValueError, TypeError):
                    continue

        for field in self.CASH_FIELDS:
            if field in item and item[field] is not None:
                try:
                    cash = float(item[field])
                    break
                except (ValueError, TypeError):
                    continue

        # Try standard_charges array (CMS 2.0 format)
        std_charges = item.get("standard_charges", [])
        if isinstance(std_charges, list):
            for sc in std_charges:
                if not isinstance(sc, dict):
                    continue

                if gross is None:
                    for field in self.GROSS_FIELDS:
                        if field in sc and sc[field] is not None:
                            try:
                                gross = float(sc[field])
                                break
                            except (ValueError, TypeError):
                                continue

                if cash is None:
                    for field in self.CASH_FIELDS:
                        if field in sc and sc[field] is not None:
                            try:
                                cash = float(sc[field])
                                break
                            except (ValueError, TypeError):
                                continue

        return gross, cash

    def parse_data(self, raw_data: bytes | str | dict | list) -> pd.DataFrame:
        """Parse CMS standard JSON format (v2.0) with field variation handling.

        Args:
            raw_data: Parsed JSON data following CMS schema

        Returns:
            DataFrame with vocabulary_id, concept_code, gross, cash columns
        """
        if not isinstance(raw_data, (dict, list)):
            raise ValueError(f"Expected dict or list, got {type(raw_data).__name__}")
        charges = self._find_charges_array(raw_data)

        if not charges:
            self.logger.warning(
                "no_charges_found",
                hospital=self.hospital_config.hospital,
                data_type=type(raw_data).__name__,
            )
            return pd.DataFrame(columns=["vocabulary_id", "concept_code", "gross", "cash"])

        records = []
        codes_seen = set()
        parse_errors = 0

        for item in charges:
            if not isinstance(item, dict):
                continue

            try:
                codes = self._extract_codes(item)
                if not codes:
                    continue

                gross, cash = self._extract_prices(item)

                for code, vocab_id in codes:
                    # Skip duplicates
                    key = (code, vocab_id)
                    if key in codes_seen:
                        continue
                    codes_seen.add(key)

                    records.append(
                        {
                            "vocabulary_id": vocab_id,
                            "concept_code": code,
                            "gross": gross,
                            "cash": cash,
                        }
                    )
            except Exception as e:
                parse_errors += 1
                if parse_errors <= 10:  # Log first 10 errors
                    self.logger.debug("item_parse_error", error=str(e))

        if parse_errors > 0:
            self.logger.info(
                "parse_completed_with_errors",
                records=len(records),
                errors=parse_errors,
            )

        self.logger.debug(
            "cms_json_parsed",
            records=len(records),
            unique_codes=len(codes_seen),
        )

        return (
            pd.DataFrame(records)
            if records
            else pd.DataFrame(columns=["vocabulary_id", "concept_code", "gross", "cash"])
        )


class HyveCMSJSONScraper(CMSStandardJSONScraper):
    """Scraper for Hyve Healthcare CMS JSON format (Covenant Health).

    Uses the same CMS 2.0 format as CMSStandardJSONScraper.
    """

    pass  # Same implementation as parent
