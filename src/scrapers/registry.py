"""Scraper registry and factory.

Provides a factory pattern for instantiating the appropriate scraper
based on hospital configuration (IDN, data format, or URL patterns).

For URL-based configs from hospitalpricingfiles.org, we use URL pattern
matching to route to appropriate scrapers since IDN info isn't available.
"""

import re

from ..config import ScraperConfig
from ..models import DataFormat, HospitalConfig
from ..normalizers import CPTNormalizer
from ..utils.http_client import RetryHTTPClient
from ..utils.logger import get_logger
from .base import BaseScraper
from .cms_csv_scraper import CMSStandardCSVScraper, TennovaCMSCSVScraper
from .cms_json_scraper import CMSStandardJSONScraper, HyveCMSJSONScraper
from .cms_xlsx_scraper import CMSStandardXLSXScraper
from .cms_zip_scraper import CMSStandardZIPScraper

logger = get_logger(__name__)


class ScraperRegistry:
    """Registry of available scrapers by IDN, CCN, URL patterns, and format.

    Uses a multi-level lookup with priority:
    1. Explicit scraper_type field (highest priority)
    2. CCN-specific scraper (for per-hospital overrides)
    3. URL provider-based scraper (for hospitalpricingfiles.org URLs)
    4. IDN-specific scraper (for hospital systems)
    5. Format-based scraper (CSV, JSON, etc.)

    This allows custom parsing logic for specific hospitals or systems
    while falling back to generic format handlers.
    """

    # CCN-specific scrapers (for individual hospital overrides)
    CCN_SCRAPERS: dict[str, type[BaseScraper]] = {
        # Add CCN-specific overrides here, e.g.:
        # "440039": CustomScraperForVanderbilt,
    }

    # URL provider-based scrapers (for URLs from hospitalpricingfiles.org)
    # Maps URL pattern (substring or regex) to (scraper_class, is_regex)
    # Checked in order - first match wins
    URL_PROVIDER_SCRAPERS: list[tuple[str, type[BaseScraper], bool]] = [
        # ClaraPrice endpoints serve JSON
        (r"claraprice\.net.*machine-readable", CMSStandardJSONScraper, True),
        # Craneware API endpoints serve CMS CSV (not JSON!)
        (r"craneware\.com/api-pricing-transparency", CMSStandardCSVScraper, True),
        # Hospital Price Index blob storage - CMS 2.0 CSV
        ("sthpiprd.blob.core.windows.net", CMSStandardCSVScraper, False),
        # AccuReg price transparency endpoints - CMS CSV
        ("pricetransparency.accureg.net", CMSStandardCSVScraper, False),
        # UHS behavioral health files - CMS CSV
        ("uhsfilecdn.eskycity.net", CMSStandardCSVScraper, False),
        # Encompass Health rehab - CMS CSV (legacy domain)
        ("encompasshealth.com", CMSStandardCSVScraper, False),
        # Encompass Health rehab - CMS CSV (new Sitecore CDN)
        ("edge.sitecorecloud.io/encompasshee", CMSStandardCSVScraper, False),
        # Select Medical specialty hospitals - CMS CSV
        ("resources.selectmedical.com", CMSStandardCSVScraper, False),
        # Panacea/Trinity Health endpoints - ZIP containing CMS CSV
        ("panaceainc.com", CMSStandardZIPScraper, False),
        # SUN Behavioral Health facilities - XLSX format
        (r"sun(behavioral|delaware)\.com.*\.xlsx", CMSStandardXLSXScraper, True),
        # HCA Digital Asset Manager (Mission Health, etc.) - CMS JSON format
        ("www.hcadam.com/api/public/content", CMSStandardJSONScraper, False),
        # EZCost/machine-readable-files.com provider - CMS CSV
        ("machine-readable-files.com", CMSStandardCSVScraper, False),
        # Centauri Health Solutions API exports - CMS CSV
        (r"centaurihs\.com/ptapp/api/cdm/export", CMSStandardCSVScraper, True),
        # Cloudinary CDN (Iredell Health System) - CMS CSV
        ("res.cloudinary.com/dpmykpsih", CMSStandardCSVScraper, False),
        # Para-HCFS price transparency (Cape Fear, Sampson) - CMS CSV
        ("apps.para-hcfs.com", CMSStandardCSVScraper, False),
        # Kindred Healthcare disclosure - CMS JSON (despite .aspx extension)
        ("hospitalpricedisclosure.com", CMSStandardJSONScraper, False),
        # Google Drive links - usually CSV files
        ("drive.google.com", CMSStandardCSVScraper, False),
    ]

    # IDN-specific scrapers
    IDN_SCRAPERS: dict[str, type[BaseScraper]] = {
        "Covenant Health": HyveCMSJSONScraper,
        "Memorial": CMSStandardJSONScraper,
        "Tennova Healthcare": TennovaCMSCSVScraper,
        "Parkridge": CMSStandardJSONScraper,
        "Mission Health": CMSStandardJSONScraper,
    }

    # Explicit scraper type name -> class mapping
    SCRAPER_TYPES: dict[str, type[BaseScraper]] = {
        "CMSStandardJSONScraper": CMSStandardJSONScraper,
        "CMSStandardCSVScraper": CMSStandardCSVScraper,
        "CMSStandardXLSXScraper": CMSStandardXLSXScraper,
        "CMSStandardZIPScraper": CMSStandardZIPScraper,
        "HyveCMSJSONScraper": HyveCMSJSONScraper,
        "TennovaCMSCSVScraper": TennovaCMSCSVScraper,
    }

    # Format-based fallback scrapers
    FORMAT_SCRAPERS: dict[DataFormat, type[BaseScraper]] = {
        DataFormat.CSV: CMSStandardCSVScraper,  # CMS 2.0/3.0 format (most common)
        DataFormat.JSON: CMSStandardJSONScraper,  # Default JSON handler for scale
        DataFormat.XLSX: CMSStandardXLSXScraper,  # Excel format (behavioral health)
        DataFormat.ZIP: CMSStandardZIPScraper,  # ZIP containing CMS CSV
    }

    @classmethod
    def _get_url_provider_scraper(cls, file_url: str) -> type[BaseScraper] | None:
        """Check URL against known provider patterns.

        Args:
            file_url: The file URL to check

        Returns:
            Scraper class if URL matches a known provider pattern, None otherwise
        """
        if not file_url:
            return None

        url_lower = file_url.lower()
        for pattern, scraper_class, is_regex in cls.URL_PROVIDER_SCRAPERS:
            if is_regex:
                if re.search(pattern, url_lower):
                    return scraper_class
            else:
                if pattern.lower() in url_lower:
                    return scraper_class

        return None

    @classmethod
    def get_scraper_class(cls, hospital_config: HospitalConfig) -> type[BaseScraper] | None:
        """Get the appropriate scraper class for a hospital.

        Priority order:
        1. Explicit scraper_type field
        2. CCN-specific scraper
        3. URL provider-based scraper (for hospitalpricingfiles.org URLs)
        4. IDN-specific scraper
        5. Format-based fallback

        Args:
            hospital_config: Hospital configuration

        Returns:
            Scraper class or None if no suitable scraper found
        """
        # 1. Check for explicit scraper_type override
        if hospital_config.scraper_type:
            if hospital_config.scraper_type in cls.SCRAPER_TYPES:
                logger.debug(
                    "using_explicit_scraper",
                    scraper=hospital_config.scraper_type,
                    hospital=hospital_config.hospital,
                )
                return cls.SCRAPER_TYPES[hospital_config.scraper_type]
            else:
                logger.warning(
                    "unknown_scraper_type",
                    scraper_type=hospital_config.scraper_type,
                    hospital=hospital_config.hospital,
                )

        # 2. Check for CCN-specific scraper
        if hospital_config.ccn and hospital_config.ccn in cls.CCN_SCRAPERS:
            logger.debug(
                "using_ccn_scraper",
                ccn=hospital_config.ccn,
                hospital=hospital_config.hospital,
            )
            return cls.CCN_SCRAPERS[hospital_config.ccn]

        # 3. Check for URL provider-based scraper
        url_scraper = cls._get_url_provider_scraper(hospital_config.file_url)
        if url_scraper:
            logger.debug(
                "using_url_provider_scraper",
                scraper=url_scraper.__name__,
                url=hospital_config.file_url[:80],
                hospital=hospital_config.hospital,
            )
            return url_scraper

        # 4. Check for IDN-specific scraper
        if hospital_config.idn and hospital_config.idn in cls.IDN_SCRAPERS:
            return cls.IDN_SCRAPERS[hospital_config.idn]

        # 5. Fall back to format-based scraper
        if hospital_config.type and hospital_config.type in cls.FORMAT_SCRAPERS:
            return cls.FORMAT_SCRAPERS[hospital_config.type]

        return None

    @classmethod
    def create_scraper(
        cls,
        hospital_config: HospitalConfig,
        scraper_config: ScraperConfig,
        http_client: RetryHTTPClient,
        normalizer: CPTNormalizer,
    ) -> BaseScraper | None:
        """Create a scraper instance for a hospital.

        Args:
            hospital_config: Hospital configuration
            scraper_config: Global scraper configuration
            http_client: HTTP client with retry logic
            normalizer: CPT code normalizer

        Returns:
            Configured scraper instance or None if unsupported
        """
        scraper_class = cls.get_scraper_class(hospital_config)

        if scraper_class is None:
            logger.warning(
                "no_scraper_found",
                idn=hospital_config.idn,
                ccn=hospital_config.ccn,
                format=hospital_config.type,
                hospital=hospital_config.hospital,
            )
            return None

        logger.debug(
            "creating_scraper",
            scraper=scraper_class.__name__,
            hospital=hospital_config.hospital,
            identifier=hospital_config.identifier,
        )

        return scraper_class(
            hospital_config=hospital_config,
            scraper_config=scraper_config,
            http_client=http_client,
            normalizer=normalizer,
        )

    @classmethod
    def register_ccn_scraper(cls, ccn: str, scraper_class: type[BaseScraper]) -> None:
        """Register a custom scraper for a specific CCN.

        Args:
            ccn: CMS Certification Number (6 alphanumeric chars)
            scraper_class: Scraper class to use for this hospital
        """
        ccn = ccn.strip().upper()
        cls.CCN_SCRAPERS[ccn] = scraper_class
        logger.info("registered_ccn_scraper", ccn=ccn, scraper=scraper_class.__name__)

    @classmethod
    def register_idn_scraper(cls, idn: str, scraper_class: type[BaseScraper]) -> None:
        """Register a custom scraper for an IDN.

        Args:
            idn: Integrated Delivery Network name
            scraper_class: Scraper class to use for this IDN
        """
        cls.IDN_SCRAPERS[idn] = scraper_class
        logger.info("registered_scraper", idn=idn, scraper=scraper_class.__name__)

    @classmethod
    def register_format_scraper(
        cls, data_format: DataFormat, scraper_class: type[BaseScraper]
    ) -> None:
        """Register a scraper for a data format.

        Args:
            data_format: Data format (CSV, JSON, etc.)
            scraper_class: Scraper class to use for this format
        """
        cls.FORMAT_SCRAPERS[data_format] = scraper_class
        logger.info(
            "registered_format_scraper", format=data_format.value, scraper=scraper_class.__name__
        )

    @classmethod
    def register_url_provider_scraper(
        cls, pattern: str, scraper_class: type[BaseScraper], is_regex: bool = False
    ) -> None:
        """Register a scraper for a URL provider pattern.

        Args:
            pattern: URL pattern (substring or regex) to match
            scraper_class: Scraper class to use for matching URLs
            is_regex: If True, pattern is treated as a regex; otherwise substring match
        """
        cls.URL_PROVIDER_SCRAPERS.append((pattern, scraper_class, is_regex))
        logger.info(
            "registered_url_provider_scraper",
            pattern=pattern,
            scraper=scraper_class.__name__,
            is_regex=is_regex,
        )

    @classmethod
    def list_available_scrapers(cls) -> dict[str, list[str]]:
        """List all available scrapers by category.

        Returns:
            Dict with categories as keys and lists of scraper names as values
        """
        return {
            "explicit_types": list(cls.SCRAPER_TYPES.keys()),
            "ccn_specific": list(cls.CCN_SCRAPERS.keys()),
            "url_provider": [f"{p[0]} -> {p[1].__name__}" for p in cls.URL_PROVIDER_SCRAPERS],
            "idn_specific": list(cls.IDN_SCRAPERS.keys()),
            "format_based": [f.value for f in cls.FORMAT_SCRAPERS.keys()],
        }


def get_scraper(
    hospital_config: HospitalConfig,
    scraper_config: ScraperConfig,
    http_client: RetryHTTPClient,
    normalizer: CPTNormalizer,
) -> BaseScraper | None:
    """Convenience function to create a scraper for a hospital.

    Args:
        hospital_config: Hospital configuration
        scraper_config: Global scraper configuration
        http_client: HTTP client with retry logic
        normalizer: CPT code normalizer

    Returns:
        Configured scraper instance or None if unsupported
    """
    return ScraperRegistry.create_scraper(
        hospital_config=hospital_config,
        scraper_config=scraper_config,
        http_client=http_client,
        normalizer=normalizer,
    )
