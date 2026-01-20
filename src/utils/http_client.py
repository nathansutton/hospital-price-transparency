"""HTTP client with automatic retry logic.

Replaces shell curl commands with a pure Python implementation that handles
retries, timeouts, and various HTTP error conditions gracefully.
"""

import io
from typing import Any

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .logger import get_logger

logger = get_logger(__name__)


class HTTPError(Exception):
    """Base exception for HTTP-related errors."""

    def __init__(self, message: str, status_code: int | None = None, url: str | None = None):
        self.status_code = status_code
        self.url = url
        super().__init__(message)


class RetryableHTTPError(HTTPError):
    """HTTP error that can be retried (5xx, timeouts, connection errors)."""

    pass


class PermanentHTTPError(HTTPError):
    """HTTP error that should not be retried (4xx except 429)."""

    pass


class RetryHTTPClient:
    """HTTP client with automatic retry logic for transient failures.

    Features:
    - Exponential backoff for retries
    - Handles 429 (rate limit), 5xx (server errors)
    - Configurable timeout and retry count
    - Automatic content-type detection
    - Browser-like User-Agent to avoid blocks
    """

    DEFAULT_HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }

    def __init__(
        self,
        timeout: int = 60,
        max_retries: int = 3,
        backoff_multiplier: float = 2.0,
    ):
        """Initialize the HTTP client.

        Args:
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            backoff_multiplier: Multiplier for exponential backoff
        """
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_multiplier = backoff_multiplier
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)

    def _should_retry(self, status_code: int) -> bool:
        """Determine if a status code warrants a retry."""
        # Retry on rate limit or server errors
        return status_code == 429 or (500 <= status_code < 600)

    def _make_request(self, url: str, stream: bool = False) -> requests.Response:
        """Make a single HTTP request, raising appropriate exceptions."""
        try:
            response = self.session.get(url, timeout=self.timeout, stream=stream)

            if response.status_code == 200:
                return response

            if self._should_retry(response.status_code):
                raise RetryableHTTPError(
                    f"Server returned {response.status_code}",
                    status_code=response.status_code,
                    url=url,
                )
            else:
                raise PermanentHTTPError(
                    f"Server returned {response.status_code}",
                    status_code=response.status_code,
                    url=url,
                )

        except requests.exceptions.Timeout as e:
            raise RetryableHTTPError(f"Request timed out: {e}", url=url) from e
        except requests.exceptions.ConnectionError as e:
            raise RetryableHTTPError(f"Connection error: {e}", url=url) from e
        except requests.exceptions.RequestException as e:
            raise HTTPError(f"Request failed: {e}", url=url) from e

    @retry(
        retry=retry_if_exception_type(RetryableHTTPError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=30),
        reraise=True,
    )
    def get(self, url: str) -> requests.Response:
        """Fetch a URL with automatic retry on transient failures.

        Args:
            url: The URL to fetch

        Returns:
            Response object on success

        Raises:
            PermanentHTTPError: For non-retryable errors (404, 403, etc.)
            RetryableHTTPError: If all retry attempts fail
        """
        logger.debug("http_request", url=url)
        response = self._make_request(url)
        logger.debug(
            "http_response",
            url=url,
            status_code=response.status_code,
            content_length=len(response.content),
        )
        return response

    def get_json(self, url: str) -> Any:
        """Fetch and parse JSON from a URL.

        Args:
            url: The URL to fetch

        Returns:
            Parsed JSON data (dict or list)

        Raises:
            HTTPError: On fetch failure
            ValueError: On JSON parse failure
        """
        response = self.get(url)
        try:
            return response.json()
        except ValueError as e:
            raise ValueError(f"Failed to parse JSON from {url}: {e}") from e

    def get_csv(
        self,
        url: str,
        skiprows: int = 0,
        dtype: str | dict[str, str] = "object",
        keep_default_na: bool = False,
    ) -> pd.DataFrame:
        """Fetch and parse CSV from a URL.

        Args:
            url: The URL to fetch
            skiprows: Number of rows to skip at the start
            dtype: Data type for columns (default: object to preserve strings)
            keep_default_na: Whether to interpret NA values

        Returns:
            DataFrame with CSV data

        Raises:
            HTTPError: On fetch failure
            pd.errors.ParserError: On CSV parse failure
        """
        response = self.get(url)
        return pd.read_csv(
            io.StringIO(response.text),
            skiprows=skiprows,
            dtype=dtype,
            keep_default_na=keep_default_na,
        )

    def check_url(self, url: str) -> tuple[bool, str]:
        """Check if a URL is accessible.

        Args:
            url: The URL to check

        Returns:
            Tuple of (is_accessible, status_message)
        """
        try:
            response = self.session.head(url, timeout=self.timeout, allow_redirects=True)
            if response.status_code == 200:
                return True, "OK"
            elif response.status_code == 405:
                # HEAD not allowed, try GET
                response = self.session.get(url, timeout=self.timeout, stream=True)
                response.close()
                if response.status_code == 200:
                    return True, "OK"
                return False, f"HTTP {response.status_code}"
            else:
                return False, f"HTTP {response.status_code}"
        except requests.exceptions.Timeout:
            return False, "Timeout"
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection error: {str(e)[:50]}"
        except requests.exceptions.RequestException as e:
            return False, f"Error: {str(e)[:50]}"

    def close(self) -> None:
        """Close the underlying session."""
        self.session.close()

    def __enter__(self) -> "RetryHTTPClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
