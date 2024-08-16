import os
import logging

from fundermapssdk.db import DbProvider
from fundermapssdk.gdal import GDALProvider
from fundermapssdk.mail import MailProvider
from fundermapssdk.config import DatabaseConfig, MailConfig, S3Config
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

    def __init__(
        self,
        mail_config: MailConfig | None = None,
        db_config: DatabaseConfig | None = None,
        s3_config: S3Config | None = None,
    ):
        self.mail_config = mail_config
        self.db_config = db_config
        self.s3_config = s3_config

        self._service_providers = {}
        self._logger = logger

        self.sdk_directory = os.path.dirname(os.path.realpath(__file__))

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
