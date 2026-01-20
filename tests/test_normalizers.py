"""Tests for CPT normalization logic."""

import pandas as pd
import pytest

from src.normalizers.cpt_normalizer import CPTNormalizer


class TestCPTNormalizer:
    """Tests for CPTNormalizer class."""

    @pytest.fixture
    def normalizer(self):
        """Create a normalizer without vocabulary validation."""
        return CPTNormalizer(concept_df=None)

    @pytest.fixture
    def normalizer_with_vocab(self):
        """Create a normalizer with a small vocabulary."""
        vocab_df = pd.DataFrame({"concept_code": ["99213", "99214", "99215", "00142"]})
        return CPTNormalizer(concept_df=vocab_df)

    def test_strip_leading_zero(self, normalizer):
        """Test removal of leading zeros from 6-character codes."""
        assert normalizer.strip_leading_zero("099213") == "99213"
        assert normalizer.strip_leading_zero("99213") == "99213"
        assert normalizer.strip_leading_zero("0099213") == "0099213"  # 7 chars, no change
        assert normalizer.strip_leading_zero("12345") == "12345"

    def test_clean_price_numeric(self, normalizer):
        """Test cleaning of already-numeric prices."""
        assert normalizer.clean_price(100.50) == 100.50
        assert normalizer.clean_price(100) == 100.0

    def test_clean_price_string(self, normalizer):
        """Test cleaning of string prices with formatting."""
        assert normalizer.clean_price("$100.50") == 100.50
        assert normalizer.clean_price("1,234.56") == 1234.56
        assert normalizer.clean_price("$1,234.56") == 1234.56
        assert normalizer.clean_price("  100.00  ") == 100.0

    def test_clean_price_invalid(self, normalizer):
        """Test cleaning of invalid prices."""
        assert normalizer.clean_price("N/A") is None
        assert normalizer.clean_price("") is None
        assert normalizer.clean_price(None) is None

    def test_normalize_basic(self, normalizer):
        """Test basic normalization with rename."""
        df = pd.DataFrame(
            {
                "CPT_Code": ["99213", "99214"],
                "Gross_Charge": ["$100.00", "$200.00"],
                "Cash_Price": ["$80.00", "$160.00"],
            }
        )

        result = normalizer.normalize(
            df,
            rename=True,
            gross_col="Gross_Charge",
            cash_col="Cash_Price",
            cpt_col="CPT_Code",
        )

        assert len(result) == 4  # 2 codes * 2 price types
        assert set(result["cpt"].unique()) == {"99213", "99214"}
        assert set(result["type"].unique()) == {"cash", "gross"}

    def test_normalize_filters_non_cpt(self, normalizer):
        """Test that non-CPT codes are filtered."""
        df = pd.DataFrame(
            {
                "vocabulary_id": ["cpt", "icd10", "cpt"],
                "concept_code": ["99213", "A00.0", "99214"],
                "gross": [100, 200, 300],
                "cash": [80, 160, 240],
            }
        )

        result = normalizer.normalize(df)

        # Should only have CPT codes
        assert len(result["cpt"].unique()) == 2
        assert "A00.0" not in result["cpt"].values

    def test_normalize_deduplicates(self, normalizer):
        """Test that duplicate records are deduplicated by max price."""
        df = pd.DataFrame(
            {
                "vocabulary_id": ["cpt", "cpt"],
                "concept_code": ["99213", "99213"],
                "gross": [100, 150],  # Should take max (150)
                "cash": [80, 70],  # Should take max (80)
            }
        )

        result = normalizer.normalize(df)

        gross_price = result[(result["cpt"] == "99213") & (result["type"] == "gross")][
            "price"
        ].iloc[0]
        cash_price = result[(result["cpt"] == "99213") & (result["type"] == "cash")]["price"].iloc[
            0
        ]

        assert gross_price == 150.0
        assert cash_price == 80.0

    def test_normalize_drops_zero_prices(self, normalizer):
        """Test that zero and negative prices are dropped."""
        df = pd.DataFrame(
            {
                "vocabulary_id": ["cpt", "cpt"],
                "concept_code": ["99213", "99214"],
                "gross": [100, 0],
                "cash": [80, 50],
            }
        )

        result = normalizer.normalize(df)

        # 99214 gross should be dropped (price=0)
        assert len(result) == 3

    def test_normalize_with_vocabulary(self, normalizer_with_vocab):
        """Test normalization filters to valid vocabulary codes."""
        df = pd.DataFrame(
            {
                "vocabulary_id": ["cpt", "cpt", "cpt"],
                "concept_code": ["99213", "99999", "00142"],  # 99999 not in vocab
                "gross": [100, 200, 300],
                "cash": [80, 160, 240],
            }
        )

        result = normalizer_with_vocab.normalize(df)

        # Should only have codes in vocabulary
        assert "99999" not in result["cpt"].values
        assert "99213" in result["cpt"].values
        assert "00142" in result["cpt"].values

    def test_normalize_output_schema(self, normalizer):
        """Test that output has correct schema."""
        df = pd.DataFrame(
            {
                "vocabulary_id": ["cpt"],
                "concept_code": ["99213"],
                "gross": [100.555],
                "cash": [80.444],
            }
        )

        result = normalizer.normalize(df)

        # Check columns
        assert list(result.columns) == ["cpt", "type", "price"]

        # Check rounding
        assert all(result["price"].apply(lambda x: round(x, 2) == x))

        # Check sorting
        assert result.iloc[0]["type"] == "cash"  # cash before gross
