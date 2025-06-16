"""
PDF generation functionality for FunderMapsSDK.

This module provides PDF generation capabilities using the PDF.co API service.
"""

import httpx
import logging
from typing import Dict, Any, Optional

from fundermapssdk.config import PDFCoConfig

BASE_URL = "https://api.pdf.co/v1"

# Default timeout settings
DEFAULT_TIMEOUT = 60.0
DEFAULT_CONNECT_TIMEOUT = 5.0

# PDF.co API error codes
PDF_CO_ERRORS = {
    400: "Bad Request - Invalid parameters",
    401: "Unauthorized - Invalid API key",
    402: "Payment Required - Insufficient credits",
    403: "Forbidden - Access denied",
    404: "Not Found - Resource not found",
    429: "Too Many Requests - Rate limit exceeded",
    500: "Internal Server Error - PDF.co service error",
    503: "Service Unavailable - PDF.co service temporarily unavailable",
}


class PDFGenerationError(Exception):
    """Custom exception for PDF generation errors."""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        response_data: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data


class PDFProvider:
    """
    Provider for generating PDFs from URLs using the PDF.co API service.

    This class encapsulates the functionality for converting web pages to PDF documents
    through the PDF.co service.

    Attributes:
        _sdk: Reference to the parent SDK instance
        config: PDF.co configuration settings
    """

    def __init__(self, sdk, config: PDFCoConfig):
        """
        Initialize the PDF provider with SDK reference and configuration.

        Args:
            sdk: The parent SDK instance that contains the logger
            config: PDF.co configuration containing API key
        """
        self._sdk = sdk
        self.config = config

    async def generate_pdf(
        self,
        url: str,
        name: str,
        paper_size: str = "A4",
        orientation: str = "Portrait",
        margins: str = "10mm",
        timeout: Optional[float] = None,
        async_mode: bool = False,
    ) -> Dict[str, Any]:
        """
        Generate a PDF from a URL using PDF.co service.

        Args:
            url: The URL to convert to PDF
            name: The name for the generated PDF
            paper_size: Paper size for the PDF (default: A4)
            orientation: Page orientation (Portrait or Landscape)
            margins: Page margins (default: 10mm)
            timeout: Request timeout in seconds (default: 60.0)
            async_mode: Whether to use async processing (default: False)

        Returns:
            Dict containing the PDF generation result

        Raises:
            PDFGenerationError: If PDF generation fails
            httpx.RequestError: If HTTP request fails
        """
        self._log_debug(f"Generating PDF from {url} with name '{name}'")

        # Validate inputs
        if not url or not url.strip():
            raise PDFGenerationError("URL cannot be empty")
        if not name or not name.strip():
            raise PDFGenerationError("Name cannot be empty")

        parameters = {
            "url": url.strip(),
            "name": name.strip(),
            "paperSize": paper_size,
            "orientation": orientation,
            "margins": margins,
            "async": async_mode,
        }

        headers = {
            "x-api-key": self.config.api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }

        request_timeout = timeout or DEFAULT_TIMEOUT
        timeout_config = httpx.Timeout(request_timeout, connect=DEFAULT_CONNECT_TIMEOUT)

        try:
            async with httpx.AsyncClient(timeout=timeout_config) as client:
                api_url = f"{BASE_URL}/pdf/convert/from/url"

                self._log_debug(f"Making request to PDF.co API: {api_url}")
                response = await client.post(api_url, headers=headers, data=parameters)

                # Handle HTTP errors with custom messages
                if response.status_code != 200:
                    error_message = PDF_CO_ERRORS.get(
                        response.status_code,
                        f"HTTP {response.status_code} - Unknown error",
                    )
                    self._log_error(f"PDF generation failed: {error_message}")
                    raise PDFGenerationError(
                        error_message,
                        status_code=response.status_code,
                        response_data=response.json() if response.content else None,
                    )

                result = response.json()

                # Check for API-level errors in the response
                if result.get("error", False):
                    error_message = result.get("message", "Unknown API error")
                    self._log_error(f"PDF.co API error: {error_message}")
                    raise PDFGenerationError(
                        f"PDF.co API error: {error_message}", response_data=result
                    )

                self._log_debug(f"PDF successfully generated from {url}")
                return result

        except httpx.TimeoutException as e:
            error_message = f"Request timed out after {request_timeout}s"
            self._log_error(error_message)
            raise PDFGenerationError(error_message) from e

        except httpx.RequestError as e:
            error_message = f"HTTP request failed: {str(e)}"
            self._log_error(error_message)
            raise PDFGenerationError(error_message) from e

    def _log_debug(self, message: str) -> None:
        """Log debug message with class name prefix."""
        self._sdk._logger.log(logging.DEBUG, f"{self.__class__.__name__}: {message}")

    def _log_error(self, message: str) -> None:
        """Log error message with class name prefix."""
        self._sdk._logger.log(logging.ERROR, f"{self.__class__.__name__}: {message}")

    def __logger(self, level, message):
        """Legacy logger method for backward compatibility."""
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
