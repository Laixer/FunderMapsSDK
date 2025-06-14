import httpx
import logging

from fundermapssdk.config import PDFCoConfig

BASE_URL = "https://api.pdf.co/v1"


class PDFProvider:
    def __init__(self, sdk, config: PDFCoConfig):
        self._sdk = sdk
        self.config = config

    async def generate_pdf(self, url: str, name: str):
        self.__logger(logging.DEBUG, f"Generating PDF from {url}")

        parameters = {
            "url": url,
            "name": name,
            "paperSize": "A4",
            "async": False,
        }
        headers = {
            "x-api-key": self.config.api_key,
        }

        timeout = httpx.Timeout(60.0, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            url = f"{BASE_URL}/pdf/convert/from/url"

            response = await client.post(url, headers=headers, data=parameters)
            response.raise_for_status()

            self.__logger(logging.DEBUG, f"PDF generated from {url}")

            return response.json()

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
