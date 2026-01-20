"""Tests for scraper implementations."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.config import ScraperConfig
from src.models import DataFormat, HospitalConfig
from src.normalizers import CPTNormalizer
from src.scrapers.cms_csv_scraper import CMSStandardCSVScraper
from src.scrapers.cms_json_scraper import CMSStandardJSONScraper, HyveCMSJSONScraper
from src.scrapers.registry import ScraperRegistry, get_scraper
from src.utils.http_client import RetryHTTPClient


@pytest.fixture
def mock_http_client():
    """Create a mock HTTP client."""
    return MagicMock(spec=RetryHTTPClient)


@pytest.fixture
def mock_normalizer():
    """Create a mock CPT normalizer."""
    normalizer = MagicMock(spec=CPTNormalizer)
    # Make normalize return input unchanged for testing
    normalizer.normalize.side_effect = lambda df, **kwargs: df
    return normalizer


@pytest.fixture
def scraper_config():
    """Create a test scraper config."""
    return ScraperConfig()


@pytest.fixture
def json_hospital_config():
    """Create a JSON hospital config for testing."""
    return HospitalConfig(
        hospital_npi="1234567890",
        ccn="340001",
        can_automate=True,
        idn="Parkridge",
        hospital="Test Hospital",
        address="123 Test St",
        cbsa=12345,
        cbsa_title="Test City",
        state="TN",
        parent_url="https://example.com",
        file_url="https://example.com/prices.json",
        type=DataFormat.JSON,
    )


@pytest.fixture
def json_covenant_config():
    """Create a Covenant Health JSON config for testing."""
    return HospitalConfig(
        hospital_npi="1234567890",
        ccn="340002",
        can_automate=True,
        idn="Covenant Health",
        hospital="Test Covenant Hospital",
        address="123 Test St",
        cbsa=12345,
        cbsa_title="Test City",
        state="TN",
        parent_url="https://example.com",
        file_url="https://example.com/prices.json",
        type=DataFormat.JSON,
    )


@pytest.fixture
def csv_hospital_config():
    """Create a CSV hospital config for testing."""
    return HospitalConfig(
        hospital_npi="1234567890",
        ccn="340003",
        can_automate=True,
        idn="Unknown",
        hospital="Test CSV Hospital",
        address="123 Test St",
        cbsa=12345,
        cbsa_title="Test City",
        state="TN",
        parent_url="https://example.com",
        file_url="https://example.com/prices.csv",
        type=DataFormat.CSV,
    )


class TestCMSStandardJSONScraper:
    """Tests for CMSStandardJSONScraper."""

    def test_parse_data_cms_format(self, json_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test CMS standard JSON parsing with standard_charge_information format."""
        scraper = CMSStandardJSONScraper(
            hospital_config=json_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        json_data = {
            "standard_charge_information": [
                {
                    "billing_code_information": [{"type": "CPT", "code": "99213"}],
                    "standard_charges": [
                        {"gross_charge": 100, "discounted_cash": 80}
                    ],
                },
                {
                    "billing_code_information": [{"type": "CPT", "code": "99214"}],
                    "standard_charges": [
                        {"gross_charge": 150, "discounted_cash": 120}
                    ],
                },
            ]
        }

        df = scraper.parse_data(json_data)

        assert len(df) == 2
        assert df.iloc[0]["concept_code"] == "99213"
        assert df.iloc[0]["gross"] == 100
        assert df.iloc[0]["cash"] == 80

    def test_parse_data_flat_list(self, json_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test CMS JSON parsing with flat list format (direct charges array)."""
        scraper = CMSStandardJSONScraper(
            hospital_config=json_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        # Some hospitals use a flat list format
        json_data = [
            {
                "code": "99213",
                "type": "CPT",
                "description": "Office Visit",
                "gross_charge": 100,
                "discounted_cash": 80,
            },
            {
                "code": "99214",
                "type": "CPT",
                "description": "Extended Visit",
                "gross_charge": 150,
                "discounted_cash": 120,
            },
        ]

        df = scraper.parse_data(json_data)

        assert len(df) == 2
        assert df.iloc[0]["concept_code"] == "99213"


class TestHyveCMSJSONScraper:
    """Tests for HyveCMSJSONScraper (Covenant Health format).
    
    HyveCMSJSONScraper inherits from CMSStandardJSONScraper without modifications,
    so we just test that it can be instantiated and uses the same parsing.
    """

    def test_inherits_cms_standard(self, json_covenant_config, scraper_config, mock_http_client, mock_normalizer):
        """Test that HyveCMSJSONScraper inherits from CMSStandardJSONScraper."""
        scraper = HyveCMSJSONScraper(
            hospital_config=json_covenant_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        assert isinstance(scraper, CMSStandardJSONScraper)

    def test_parse_data_uses_cms_format(self, json_covenant_config, scraper_config, mock_http_client, mock_normalizer):
        """Test HyveCMSJSONScraper uses CMS standard format parsing."""
        scraper = HyveCMSJSONScraper(
            hospital_config=json_covenant_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        # Use CMS standard format
        json_data = {
            "standard_charge_information": [
                {
                    "billing_code_information": [{"type": "CPT", "code": "99213"}],
                    "standard_charges": [
                        {"gross_charge": 100, "discounted_cash": 80}
                    ],
                },
            ]
        }

        df = scraper.parse_data(json_data)

        assert len(df) == 1
        assert df.iloc[0]["concept_code"] == "99213"


class TestCMSStandardCSVScraper:
    """Tests for CMSStandardCSVScraper."""

    def test_parse_data(self, csv_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test CMS standard CSV parsing with pipe-delimited code columns."""
        scraper = CMSStandardCSVScraper(
            hospital_config=csv_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        # CMS 2.0 CSV format: 2 header rows, then data
        csv_data = """Hospital Name,Test CSV Hospital,,,,,,
Hospital Address,123 Test St,,,,,,
description,code|1,code|1|type,code|2,code|2|type,standard_charge|gross,standard_charge|discounted_cash,notes
Office Visit,99213,CPT,G0001,HCPCS,100.00,80.00,
Extended Visit,99214,CPT,,,150.00,120.00,
"""

        df = scraper.parse_data(csv_data)

        assert len(df) >= 1
        # The parser should find CPT codes
        assert any(df["concept_code"] == "99213")


class TestScraperRegistry:
    """Tests for ScraperRegistry."""

    def test_get_idn_scraper_covenant(self, json_covenant_config):
        """Test getting IDN-specific scraper for Covenant Health."""
        scraper_class = ScraperRegistry.get_scraper_class(json_covenant_config)
        assert scraper_class == HyveCMSJSONScraper

    def test_get_idn_scraper_parkridge(self, json_hospital_config):
        """Test getting IDN-specific scraper for Parkridge."""
        scraper_class = ScraperRegistry.get_scraper_class(json_hospital_config)
        assert scraper_class == CMSStandardJSONScraper

    def test_get_format_scraper_json(self):
        """Test getting format-based scraper for JSON."""
        config = HospitalConfig(
            hospital_npi="1234567890",
            ccn="340099",
            can_automate=True,
            idn="Unknown",  # Not a registered IDN
            hospital="Test",
            address="Test",
            cbsa=12345,
            cbsa_title="Test",
            state="TN",
            parent_url="https://example.com",
            file_url="https://example.com/file.json",
            type=DataFormat.JSON,
        )
        scraper_class = ScraperRegistry.get_scraper_class(config)
        assert scraper_class == CMSStandardJSONScraper

    def test_get_format_scraper_csv(self, csv_hospital_config):
        """Test getting format-based scraper for CSV."""
        scraper_class = ScraperRegistry.get_scraper_class(csv_hospital_config)
        assert scraper_class == CMSStandardCSVScraper

    def test_get_url_provider_scraper(self):
        """Test URL pattern-based scraper selection."""
        config = HospitalConfig(
            hospital_npi="1234567890",
            ccn="340099",
            can_automate=True,
            idn="",
            hospital="Test",
            address="Test",
            cbsa=12345,
            cbsa_title="Test",
            state="TN",
            parent_url="https://example.com",
            # ClaraPrice URL pattern -> JSON scraper
            file_url="https://claraprice.net/machine-readable/hospital/123",
            type=None,  # Let URL pattern determine type
        )
        scraper_class = ScraperRegistry.get_scraper_class(config)
        assert scraper_class == CMSStandardJSONScraper

    def test_create_scraper(self, json_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test creating a scraper instance."""
        scraper = ScraperRegistry.create_scraper(
            hospital_config=json_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        assert scraper is not None
        assert isinstance(scraper, CMSStandardJSONScraper)

    def test_explicit_scraper_type(self, json_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test explicit scraper_type override."""
        json_hospital_config.scraper_type = "HyveCMSJSONScraper"
        
        scraper = ScraperRegistry.create_scraper(
            hospital_config=json_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        assert scraper is not None
        assert isinstance(scraper, HyveCMSJSONScraper)


class TestGetScraper:
    """Tests for get_scraper convenience function."""

    def test_get_scraper_returns_instance(self, json_hospital_config, scraper_config, mock_http_client, mock_normalizer):
        """Test get_scraper returns a configured scraper instance."""
        scraper = get_scraper(
            hospital_config=json_hospital_config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        assert scraper is not None
        assert isinstance(scraper, CMSStandardJSONScraper)

    def test_get_scraper_returns_none_for_unknown_format(self, scraper_config, mock_http_client, mock_normalizer):
        """Test get_scraper returns None for unknown format without IDN."""
        config = HospitalConfig(
            hospital_npi="1234567890",
            ccn="999999",
            can_automate=True,
            idn="Unknown",
            hospital="Test",
            address="Test",
            cbsa=12345,
            cbsa_title="Test",
            state="TN",
            parent_url="https://example.com",
            file_url="https://example.com/file.unknown",
            type=None,  # Unknown format
        )

        scraper = get_scraper(
            hospital_config=config,
            scraper_config=scraper_config,
            http_client=mock_http_client,
            normalizer=mock_normalizer,
        )

        assert scraper is None
