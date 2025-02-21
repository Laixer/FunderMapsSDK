import os
import logging


from fundermapssdk.db import DbProvider
from fundermapssdk.gdal import GDALProvider
from fundermapssdk.config import DatabaseConfig, S3Config, PDFCoConfig
from fundermapssdk.storage import ObjectStorageProvider

logger = logging.getLogger(__name__)


class FunderMapsSDK:
    """
    FunderMapsSDK class represents the main entry point for interacting with the FunderMaps SDK.

    Attributes:
        mail_config (MailConfig | None): The configuration for the mail service. Defaults to None.
        db_config (DatabaseConfig | None): The configuration for the database service. Defaults to None.

    Methods:
        __init__(mail_config: MailConfig | None = None, db_config: DatabaseConfig | None = None):
            Initializes a new instance of the FunderMapsSDK class.

        db -> DbProvider:
            Property that returns the database service provider.
    """

    db_config: DatabaseConfig | None
    s3_config: S3Config | None
    pdf_config: PDFCoConfig | None

    def __init__(
        self,
        db_config: DatabaseConfig | None = None,
        s3_config: S3Config | None = None,
        pdf_config: PDFCoConfig | None = None,
        **kwargs,
    ):
        self.sdk_directory = os.path.dirname(os.path.realpath(__file__))

        self.db_config = db_config
        self.s3_config = s3_config
        self.pdf_config = pdf_config

        self._service_providers = {}
        self._logger = kwargs.get("logger", logger)

    def _db_provider(self) -> DbProvider:
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

    @property
    def db(self):
        return self._db_provider()

    @property
    def gdal(self):
        return self._gdal_provider()

    @property
    def s3(self):
        return self._s3_provider()
