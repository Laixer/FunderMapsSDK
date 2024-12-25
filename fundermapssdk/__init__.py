import os
import logging
import httpx

from fundermapssdk import util
from fundermapssdk.db import DbProvider
from fundermapssdk.gdal import GDALProvider
from fundermapssdk.mail import MailProvider
from fundermapssdk.config import DatabaseConfig, MailConfig, S3Config, PDFCoConfig
from fundermapssdk.pdf import PDFProvider
from fundermapssdk.storage import ObjectStorageProvider

logger = logging.getLogger(__name__)


class File:
    def __init__(self, sdk):
        self.sdk = sdk

    # TODO: Find some temporary directory to download the file
    async def http_download(self, url, dest_path, min_size=0):
        """
        Downloads a file from the given URL and saves it to the specified destination path.

        Args:
            url (str): The URL of the file to download.
            dest_path (str): The destination path where the downloaded file will be saved.

        Raises:
            httpx.HTTPError: If there is an error during the HTTP request.

        """

        # TODO: Download into a temporary directory
        dest_dir = os.path.dirname(dest_path)
        extension = os.path.basename(dest_path)

        # Remove any existing files with the same extension
        util.remove_files(dest_dir, extension=extension)

        try:
            logger.debug(f"Downloading file from {url} to {dest_path}")

            async with httpx.AsyncClient() as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()

                    with open(dest_path, "wb") as file:
                        async for chunk in response.aiter_bytes():
                            file.write(chunk)

            logger.debug(f"File downloaded from {url} to {dest_path}")

            if min_size > 0:
                util.validate_file_size(dest_path, min_size)
                logger.debug(f"File size validated: {dest_path}")

        except Exception as e:
            logger.error(f"Error downloading file: {e}")

            # Remove the file if it was created
            util.remove_files(dest_dir, extension=extension)

            raise


class FunderMapsSDK:
    """
    FunderMapsSDK class represents the main entry point for interacting with the FunderMaps SDK.

    Attributes:
        mail_config (MailConfig | None): The configuration for the mail service. Defaults to None.
        db_config (DatabaseConfig | None): The configuration for the database service. Defaults to None.

    Methods:
        __init__(mail_config: MailConfig | None = None, db_config: DatabaseConfig | None = None):
            Initializes a new instance of the FunderMapsSDK class.

        _mail_provider() -> MailProvider:
            Returns the mail service provider.

        _db_provider() -> DbProvider:
            Returns the database service provider.

        mail -> MailProvider:
            Property that returns the mail service provider.

        db -> DbProvider:
            Property that returns the database service provider.
    """

    mail_config: MailConfig | None
    db_config: DatabaseConfig | None
    s3_config: S3Config | None
    pdf_config: PDFCoConfig | None

    def __init__(
        self,
        mail_config: MailConfig | None = None,
        db_config: DatabaseConfig | None = None,
        s3_config: S3Config | None = None,
        pdf_config: PDFCoConfig | None = None,
    ):
        self.sdk_directory = os.path.dirname(os.path.realpath(__file__))

        self.mail_config = mail_config
        self.db_config = db_config
        self.s3_config = s3_config
        self.pdf_config = pdf_config

        self._service_providers = {
            "file": File(self),
        }
        self._logger = logger

    def _mail_provider(self):
        if self.mail_config is None:
            raise ValueError("Mail configuration is not set")

        if "mail" not in self._service_providers:
            self._service_providers["mail"] = MailProvider(self, self.mail_config)
            self._logger.debug("Mail provider initialized")

        return self._service_providers["mail"]

    def _db_provider(self):
        if self.db_config is None:
            raise ValueError("Database configuration is not set")

        if "db" not in self._service_providers:
            self._service_providers["db"] = DbProvider(self, self.db_config)
            self._logger.debug("Database provider initialized")

        return self._service_providers["db"]

    def _gdal_provider(self):
        if self.db_config is None:
            raise ValueError("Database configuration is not set")

        if "gdal" not in self._service_providers:
            self._service_providers["gdal"] = GDALProvider(self, self.db_config)
            self._logger.debug("GDAL provider initialized")

        return self._service_providers["gdal"]

    def _s3_provider(self):
        if self.s3_config is None:
            raise ValueError("S3 configuration is not set")

        if "s3" not in self._service_providers:
            self._service_providers["s3"] = ObjectStorageProvider(self, self.s3_config)
            self._logger.debug("S3 provider initialized")

        return self._service_providers["s3"]

    def _pdf_provider(self):
        if self.pdf_config is None:
            raise ValueError("PDF configuration is not set")

        if "pdf" not in self._service_providers:
            self._service_providers["pdf"] = PDFProvider(self, self.pdf_config)
            self._logger.debug("PDF provider initialized")

        return self._service_providers["pdf"]

    @property
    def file(self):
        return self._service_providers["file"]

    @property
    def mail(self):
        return self._mail_provider()

    @property
    def db(self):
        return self._db_provider()

    @property
    def gdal(self):
        return self._gdal_provider()

    @property
    def s3(self):
        return self._s3_provider()

    @property
    def pdf(self):
        return self._pdf_provider()
