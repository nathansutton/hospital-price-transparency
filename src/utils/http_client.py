"""HTTP client with automatic retry logic.

Replaces shell curl commands with a pure Python implementation that handles
retries, timeouts, and various HTTP error conditions gracefully.
"""

import io
import ssl
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .logger import get_logger

logger = get_logger(__name__)


class LegacySSLAdapter(HTTPAdapter):
    """HTTP adapter that allows legacy SSL renegotiation.

    Some older hospital web servers (Beth Israel Deaconess, Beverly Hospital,
    Mount Auburn, etc.) require legacy SSL renegotiation which is disabled
    by default in modern OpenSSL/Python for security reasons.

    This adapter creates a custom SSL context that allows these connections
    while maintaining security for other TLS features.
    """

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the connection pool with a custom SSL context."""
        # Create SSL context with legacy renegotiation enabled
        ctx = ssl.create_default_context()

        # Allow legacy server connections (unsafe renegotiation)
        # This is needed for older hospital servers
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT

        # Also be lenient with certificate verification issues
        # Some hospital servers have misconfigured certificate chains
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


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
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,text/csv,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
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

        # Mount legacy SSL adapter for older hospital servers
        # This allows connections to servers requiring legacy SSL renegotiation
        legacy_adapter = LegacySSLAdapter()
        self.session.mount("https://", legacy_adapter)

    def _transform_url(self, url: str) -> str:
        """Transform URLs that need special handling.

        Handles:
        - Google Drive view links -> direct download links
        """
        import re

        # Google Drive: /file/d/{ID}/view... -> /uc?export=download&confirm=t&id={ID}
        # The confirm=t bypasses the virus scan warning for small files
        gdrive_match = re.match(r"https://drive\.google\.com/file/d/([^/]+)/view", url)
        if gdrive_match:
            file_id = gdrive_match.group(1)
            transformed = f"https://drive.google.com/uc?export=download&confirm=t&id={file_id}"
            logger.debug("transformed_google_drive_url", original=url[:60], transformed=transformed)
            return transformed

        return url

    def _handle_google_drive_virus_scan(
        self, response: requests.Response, file_id: str
    ) -> requests.Response:
        """Handle Google Drive virus scan warning page for large files.

        For files > ~100MB, Google Drive shows a virus scan warning and requires
        extracting a UUID from the page to complete the download.
        """
        import re

        # Check if response is the virus scan warning page
        if response.headers.get("Content-Type", "").startswith("text/html"):
            text = response.text
            if "Virus scan warning" in text or "Google Drive - Virus scan warning" in text:
                # Extract UUID from the form
                uuid_match = re.search(r'name="uuid"\s+value="([^"]+)"', text)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    # Build the actual download URL
                    download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t&uuid={uuid}"
                    logger.debug("google_drive_virus_scan_bypass", uuid=uuid[:20])
                    return self.session.get(download_url, timeout=self.timeout)
                else:
                    logger.warning("google_drive_virus_scan_no_uuid")
        return response

    def _should_retry(self, status_code: int) -> bool:
        """Determine if a status code warrants a retry."""
        # Retry on rate limit or server errors
        return status_code == 429 or (500 <= status_code < 600)

    def _make_request(self, url: str, stream: bool = False) -> requests.Response:
        """Make a single HTTP request, raising appropriate exceptions."""
        import re

        # Track if this is a Google Drive URL for virus scan handling
        gdrive_file_id = None
        gdrive_match = re.match(r"https://drive\.google\.com/file/d/([^/]+)/view", url)
        if gdrive_match:
            gdrive_file_id = gdrive_match.group(1)

        # Transform special URLs (Google Drive, etc.)
        url = self._transform_url(url)
        try:
            response = self.session.get(url, timeout=self.timeout, stream=stream)

            if response.status_code == 200:
                # Handle Google Drive virus scan warning for large files
                if gdrive_file_id:
                    response = self._handle_google_drive_virus_scan(response, gdrive_file_id)
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

        Handles UTF-8 BOM (byte order mark) which some hospital systems include.
        Detects HTML responses and provides clear error messages.

        Args:
            url: The URL to fetch

        Returns:
            Parsed JSON data (dict or list)

        Raises:
            HTTPError: On fetch failure
            ValueError: On JSON parse failure or HTML response
        """
        import json

        response = self.get(url)

        # Check content type for HTML
        content_type = response.headers.get("Content-Type", "").lower()
        if "text/html" in content_type:
            raise ValueError(f"Server returned HTML instead of JSON (Content-Type: {content_type})")

        try:
            return response.json()
        except ValueError as e:
            # Try decoding with utf-8-sig to handle BOM
            try:
                text = response.content.decode("utf-8-sig")
                return json.loads(text)
            except Exception:
                pass

            # Check if content looks like HTML (common when servers return error pages)
            content_start = response.content[:500].decode("utf-8", errors="ignore").lower()
            if "<!doctype html" in content_start or "<html" in content_start:
                raise ValueError(
                    "Server returned HTML instead of JSON (URL may have moved or require auth)"
                ) from e

            raise ValueError(f"Failed to parse JSON from {url}: {e}") from e

    def get_csv(
        self,
        url: str,
        skiprows: int = 0,
        dtype: str | dict[str, str] = "object",
        keep_default_na: bool = False,
    ) -> pd.DataFrame:
        """Fetch and parse CSV from a URL.

        Handles UTF-8 BOM (byte order mark) which some hospital systems include.

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

        # Try encodings in order of likelihood
        # utf-8-sig handles BOM, cp1252 is common Windows encoding
        encodings = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
        text = None
        for encoding in encodings:
            try:
                text = response.content.decode(encoding)
                break
            except UnicodeDecodeError:
                continue

        if text is None:
            # Last resort: decode with errors='replace' to not lose data
            text = response.content.decode("utf-8", errors="replace")
            logger.warning("csv_encoding_fallback", url=url[:60])

        df: pd.DataFrame = pd.read_csv(  # type: ignore[call-overload]
            io.StringIO(text),
            skiprows=skiprows,
            dtype=dtype,
            keep_default_na=keep_default_na,
            on_bad_lines="skip",
        )
        return df

    def get_content_length(self, url: str) -> int | None:
        """Get the content length of a URL without downloading.

        Args:
            url: The URL to check

        Returns:
            Content length in bytes, or None if not available
        """
        url = self._transform_url(url)
        try:
            response = self.session.head(url, timeout=30, allow_redirects=True)
            if response.status_code == 200:
                length = response.headers.get("content-length")
                return int(length) if length else None
            elif response.status_code == 405:
                # HEAD not allowed, try streaming GET and check headers
                response = self.session.get(url, timeout=30, stream=True)
                length = response.headers.get("content-length")
                response.close()
                return int(length) if length else None
        except Exception:
            pass
        return None

    def stream_to_tempfile(self, url: str, chunk_size: int = 8192) -> "Path":
        """Stream download a URL to a temporary file.

        For large files, this avoids loading the entire content into memory.

        Args:
            url: The URL to download
            chunk_size: Size of chunks to download at a time

        Returns:
            Path to the temporary file containing the downloaded content
        """
        import tempfile
        from pathlib import Path

        logger.debug("streaming_download_start", url=url[:80])
        response = self._make_request(url, stream=True)

        # Create temp file with appropriate extension
        suffix = ".tmp"
        if ".csv" in url.lower():
            suffix = ".csv"
        elif ".json" in url.lower():
            suffix = ".json"
        elif ".zip" in url.lower():
            suffix = ".zip"

        fd, temp_path = tempfile.mkstemp(suffix=suffix)
        try:
            total_bytes = 0
            with open(fd, "wb") as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        total_bytes += len(chunk)
            logger.debug("streaming_download_complete", bytes=total_bytes, path=temp_path)
            return Path(temp_path)
        except Exception:
            # Clean up on error
            import os

            os.close(fd)
            os.unlink(temp_path)
            raise

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
