"""Tests for Pydantic data models."""

from datetime import date

import pytest
from pydantic import ValidationError

from src.models import (
    DataFormat,
    HospitalConfig,
    PriceRecord,
    PriceType,
    ScrapeResult,
    ScrapeStats,
    ScrapeStatus,
)


class TestHospitalConfig:
    """Tests for HospitalConfig model."""

    def test_valid_hospital_config(self):
        """Test creating a valid hospital config."""
        config = HospitalConfig(
            hospital_npi="1234567890",
            can_automate=True,
            idn="Test Health",
            hospital="Test Hospital",
            address="123 Main St",
            cbsa=12345,
            cbsa_title="Test City",
            state="TN",
            parent_url="https://example.com",
            file_url="https://example.com/prices.csv",
            type=DataFormat.CSV,
            skiprow=1,
            gross="Gross Charge",
            cash="Cash Price",
            cpt="CPT Code",
        )

        assert config.hospital_npi == "1234567890"
        assert config.state == "TN"

    def test_invalid_npi_length(self):
        """Test that NPI validation fails for wrong length."""
        with pytest.raises(ValidationError) as exc_info:
            HospitalConfig(
                hospital_npi="123456789",  # 9 digits, should be 10
                can_automate=True,
                idn="Test",
                hospital="Test",
                address="Test",
                cbsa=12345,
                cbsa_title="Test",
                state="TN",
                parent_url="https://example.com",
                file_url="https://example.com/file.csv",
            )

        assert "NPI must be exactly 10 digits" in str(exc_info.value)

    def test_invalid_npi_non_numeric(self):
        """Test that NPI validation fails for non-numeric."""
        with pytest.raises(ValidationError):
            HospitalConfig(
                hospital_npi="12345ABCDE",
                can_automate=True,
                idn="Test",
                hospital="Test",
                address="Test",
                cbsa=12345,
                cbsa_title="Test",
                state="TN",
                parent_url="https://example.com",
                file_url="https://example.com/file.csv",
            )

    def test_state_uppercase(self):
        """Test that state is uppercased."""
        config = HospitalConfig(
            hospital_npi="1234567890",
            can_automate=True,
            idn="Test",
            hospital="Test",
            address="Test",
            cbsa=12345,
            cbsa_title="Test",
            state="tn",  # lowercase
            parent_url="https://example.com",
            file_url="https://example.com/file.csv",
        )

        assert config.state == "TN"


class TestPriceRecord:
    """Tests for PriceRecord model."""

    def test_valid_price_record(self):
        """Test creating a valid price record."""
        record = PriceRecord(cpt="99213", type=PriceType.GROSS, price=100.50)

        assert record.cpt == "99213"
        assert record.type == PriceType.GROSS
        assert record.price == 100.50

    def test_cpt_uppercase(self):
        """Test that CPT codes are uppercased."""
        record = PriceRecord(cpt="0001a", type=PriceType.CASH, price=50.0)

        assert record.cpt == "0001A"

    def test_invalid_cpt_format(self):
        """Test that invalid CPT format fails."""
        with pytest.raises(ValidationError) as exc_info:
            PriceRecord(cpt="9921", type=PriceType.GROSS, price=100.0)  # 4 chars

        assert "Invalid CPT code format" in str(exc_info.value)

    def test_negative_price_fails(self):
        """Test that negative price fails."""
        with pytest.raises(ValidationError):
            PriceRecord(cpt="99213", type=PriceType.GROSS, price=-100.0)


class TestScrapeResult:
    """Tests for ScrapeResult model."""

    def test_success_factory(self):
        """Test success factory method."""
        result = ScrapeResult.success(
            hospital_npi="1234567890",
            file_url="https://example.com/file.csv",
            records_scraped=100,
            duration_seconds=5.5,
        )

        assert result.status == ScrapeStatus.SUCCESS
        assert result.records_scraped == 100
        assert result.error_type is None
        assert result.error_message is None

    def test_failure_factory(self):
        """Test failure factory method."""
        error = ValueError("Test error")
        result = ScrapeResult.failure(
            hospital_npi="1234567890",
            file_url="https://example.com/file.csv",
            error=error,
            duration_seconds=1.0,
        )

        assert result.status == ScrapeStatus.FAILURE
        assert result.error_type == "ValueError"
        assert result.error_message == "Test error"
        assert result.records_scraped is None

    def test_failure_truncates_long_message(self):
        """Test that long error messages are truncated."""
        error = ValueError("x" * 1000)
        result = ScrapeResult.failure(
            hospital_npi="1234567890",
            file_url="https://example.com/file.csv",
            error=error,
            duration_seconds=1.0,
        )

        assert len(result.error_message) == 500

    def test_skipped_factory(self):
        """Test skipped factory method."""
        result = ScrapeResult.skipped(
            hospital_npi="1234567890",
            file_url="https://example.com/file.csv",
            reason="No scraper available",
        )

        assert result.status == ScrapeStatus.SKIPPED
        assert result.error_message == "No scraper available"


class TestScrapeStats:
    """Tests for ScrapeStats model."""

    def test_add_result_success(self):
        """Test adding a successful result."""
        stats = ScrapeStats()
        result = ScrapeResult.success(
            hospital_npi="1234567890",
            file_url="https://example.com",
            records_scraped=100,
            duration_seconds=5.0,
        )

        stats.add_result(result)

        assert stats.total_hospitals == 1
        assert stats.successful == 1
        assert stats.failed == 0
        assert stats.total_records == 100
        assert stats.total_duration_seconds == 5.0

    def test_add_result_failure(self):
        """Test adding a failed result."""
        stats = ScrapeStats()
        result = ScrapeResult.failure(
            hospital_npi="1234567890",
            file_url="https://example.com",
            error=ValueError("Test"),
            duration_seconds=1.0,
        )

        stats.add_result(result)

        assert stats.total_hospitals == 1
        assert stats.successful == 0
        assert stats.failed == 1

    def test_success_rate(self):
        """Test success rate calculation."""
        stats = ScrapeStats()
        stats.successful = 8
        stats.failed = 2
        stats.total_hospitals = 10

        assert stats.success_rate == 80.0

    def test_success_rate_zero_hospitals(self):
        """Test success rate with no hospitals."""
        stats = ScrapeStats()

        assert stats.success_rate == 0.0

    def test_summary(self):
        """Test summary generation."""
        stats = ScrapeStats()
        stats.successful = 10
        stats.total_hospitals = 13
        stats.total_records = 50000
        stats.total_duration_seconds = 120.5

        summary = stats.summary()

        assert "10/13" in summary
        assert "50,000" in summary
        assert "120.5s" in summary
