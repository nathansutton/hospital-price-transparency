"""CPT code normalization logic.

Extracts and modernizes the cleanup_charges() function from the original scrape.py.
Uses OHDSI Athena vocabulary for validation.
"""

import re
from pathlib import Path

import pandas as pd

from ..utils.logger import get_logger

logger = get_logger(__name__)


class CPTNormalizer:
    """Normalizes hospital price data to standard CPT/HCPCS schema.

    Handles:
    - Code cleaning (remove leading zeros, validate format)
    - Price column normalization (remove $, commas, convert to numeric)
    - Filtering to valid CPT4 and HCPCS codes using OHDSI Athena vocabulary
    - Melting wide format to long format (cpt, type, price)
    """

    # Pattern for valid CPT codes (5 alphanumeric characters)
    CPT_PATTERN = re.compile(r"^[0-9A-Z]{5}$")

    def __init__(self, concept_df: pd.DataFrame | None = None):
        """Initialize the normalizer.

        Args:
            concept_df: DataFrame with 'concept_code' column containing valid CPT4 codes.
                       If None, validation against Athena vocabulary is skipped.
        """
        self.concept_codes: set[str] = set()
        if concept_df is not None:
            self.concept_codes = set(concept_df["concept_code"].astype(str).str.strip())
            logger.info("loaded_concept_codes", count=len(self.concept_codes))

    @classmethod
    def from_file(cls, concept_path: Path) -> "CPTNormalizer":
        """Create a normalizer from the OHDSI Athena CONCEPT.csv.gz file.

        Args:
            concept_path: Path to CONCEPT.csv.gz

        Returns:
            Initialized CPTNormalizer
        """
        df = pd.read_csv(concept_path, compression="gzip", sep="\t")
        # Load both CPT4 and HCPCS vocabularies
        df = df[df["vocabulary_id"].isin(["CPT4", "HCPCS"])]
        return cls(df[["concept_code"]])

    @staticmethod
    def strip_leading_zero(code: str) -> str:
        """Remove leading zero from 6-character codes.

        Some data sources pad CPT codes with a leading zero.
        E.g., "099213" -> "99213"

        Args:
            code: The CPT code to clean

        Returns:
            Cleaned CPT code
        """
        code = str(code).strip()
        if len(code) == 6 and code[0] == "0":
            return code[1:]
        return code

    @staticmethod
    def clean_price(value: str | float | int) -> float | None:
        """Clean price value by removing currency symbols and commas.

        Args:
            value: Price value (may contain $, commas, etc.)

        Returns:
            Float price or None if invalid
        """
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        # Remove currency symbols and commas
        cleaned = str(value).replace(",", "").replace("$", "").strip()

        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def normalize(
        self,
        df: pd.DataFrame,
        rename: bool = False,
        gross_col: str | None = None,
        cash_col: str | None = None,
        cpt_col: str | None = None,
    ) -> pd.DataFrame:
        """Normalize a DataFrame to the standard output schema.

        Expected input columns (after optional renaming):
        - vocabulary_id: Code type ('cpt', 'CPT4', etc.)
        - concept_code: The procedure code
        - gross: Gross charge amount
        - cash: Cash/discounted price

        Output schema:
        - cpt: 5-character CPT code
        - type: 'gross' or 'cash'
        - price: Numeric price value

        Args:
            df: Input DataFrame with price data
            rename: If True, rename columns from *_col parameters
            gross_col: Column name for gross charges (if rename=True)
            cash_col: Column name for cash prices (if rename=True)
            cpt_col: Column name for CPT codes (if rename=True)

        Returns:
            Normalized DataFrame
        """
        df = df.copy()

        # Rename columns if specified
        if rename and gross_col and cash_col and cpt_col:
            df["gross"] = df[gross_col]
            df["cash"] = df[cash_col]
            df["concept_code"] = df[cpt_col].apply(
                lambda x: self.strip_leading_zero(str(x).strip())
            )
            df["vocabulary_id"] = "cpt"

        # Ensure required columns exist
        required = ["vocabulary_id", "concept_code", "gross", "cash"]
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Clean CPT codes
        df["concept_code"] = df["concept_code"].apply(
            lambda x: self.strip_leading_zero(str(x).strip()) if pd.notna(x) else ""
        )

        # Clean price columns and ensure numeric dtype
        df["gross"] = df["gross"].apply(self.clean_price)
        df["cash"] = df["cash"].apply(self.clean_price)

        # Convert to numeric dtype (handles None -> NaN properly)
        df["gross"] = pd.to_numeric(df["gross"], errors="coerce")
        df["cash"] = pd.to_numeric(df["cash"], errors="coerce")

        # Filter to CPT and HCPCS vocabulary only
        df["vocabulary_id"] = df["vocabulary_id"].astype(str).str.lower()
        df = df[df["vocabulary_id"].isin(["cpt", "cpt4", "hcpcs"])]

        # Filter to valid concept codes if we have the vocabulary
        if self.concept_codes:
            initial_count = len(df)
            df = pd.merge(
                df,
                pd.DataFrame({"concept_code": list(self.concept_codes)}),
                on="concept_code",
                how="inner",
            )
            filtered_count = initial_count - len(df)
            if filtered_count > 0:
                logger.debug("filtered_invalid_codes", count=filtered_count)

        # Aggregate duplicates by taking max price
        df = df.groupby(["vocabulary_id", "concept_code"])[["cash", "gross"]].max().reset_index()

        # Melt to long format
        df = pd.melt(
            df,
            id_vars="concept_code",
            value_vars=["cash", "gross"],
            var_name="type",
            value_name="price",
        )

        # Rename to output schema
        df = df.rename(columns={"concept_code": "cpt"})

        # Final cleanup
        df = df.drop_duplicates()
        df = df.dropna(subset=["price"])
        df = df[df["price"] > 0]  # Remove zero prices
        df["price"] = df["price"].round(2)
        df = df.sort_values(["cpt", "type"])

        # Validate CPT format
        valid_mask = df["cpt"].apply(lambda x: bool(self.CPT_PATTERN.match(str(x))))
        invalid_count = (~valid_mask).sum()
        if invalid_count > 0:
            logger.warning("invalid_cpt_format", count=invalid_count)
            df = df[valid_mask]

        return df.reset_index(drop=True)
