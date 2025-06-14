import os
import logging
from typing import Dict, Any, TypeVar


from fundermapssdk.db import DbProvider
from fundermapssdk.gdal import GDALProvider
from fundermapssdk.mail import MailProvider
from fundermapssdk.pdf import PDFProvider
from fundermapssdk.config import DatabaseConfig, S3Config, PDFCoConfig, MailConfig
from fundermapssdk.storage import ObjectStorageProvider

T = TypeVar("T")

logger = logging.getLogger(__name__)


class FunderMapsSDK:
    """
    FunderMapsSDK class represents the main entry point for interacting with the FunderMaps SDK.

    This class provides a unified interface to access various services including database operations,
    GDAL/geospatial processing, object storage, email services, and PDF generation.

    Attributes:
        db_config (DatabaseConfig | None): Configuration for database operations.
        s3_config (S3Config | None): Configuration for S3-compatible object storage.
        pdf_config (PDFCoConfig | None): Configuration for PDF generation services.
        mail_config (MailConfig | None): Configuration for email services.
        sdk_directory (str): Directory path of the SDK installation.

    Properties:
        db (DbProvider): Database service provider.
        gdal (GDALProvider): GDAL/geospatial service provider.
        s3 (ObjectStorageProvider): Object storage service provider.
        mail (MailProvider): Email service provider.
        pdf (PDFProvider): PDF generation service provider.
    """

    db_config: DatabaseConfig | None
    s3_config: S3Config | None
    pdf_config: PDFCoConfig | None
    mail_config: MailConfig | None

    def __init__(
        self,
        db_config: DatabaseConfig | None = None,
        s3_config: S3Config | None = None,
        pdf_config: PDFCoConfig | None = None,
        mail_config: MailConfig | None = None,
        **kwargs,
    ):
        self.sdk_directory = os.path.dirname(os.path.realpath(__file__))

        self.db_config = db_config
        self.s3_config = s3_config
        self.pdf_config = pdf_config
        self.mail_config = mail_config

        self._service_providers: Dict[str, Any] = {}
        self._logger: logging.Logger = kwargs.get("logger", logger)

        # Provider configuration mapping
        self._provider_configs = {
            "db": (DbProvider, self.db_config, "Database configuration is not set"),
            "gdal": (GDALProvider, self.db_config, "Database configuration is not set"),
            "s3": (
                ObjectStorageProvider,
                self.s3_config,
                "S3 configuration is not set",
            ),
            "mail": (MailProvider, self.mail_config, "Mail configuration is not set"),
            "pdf": (PDFProvider, self.pdf_config, "PDF configuration is not set"),
        }

    def _get_provider(self, provider_key: str) -> Any:
        """
        Generic method to get or create a service provider.

        Args:
            provider_key: The key identifying the provider type.

        Returns:
            The initialized service provider.

        Raises:
            ValueError: If the required configuration is not set.
            KeyError: If the provider key is not recognized.
        """
        if provider_key not in self._provider_configs:
            raise KeyError(f"Unknown provider: {provider_key}")

        provider_class, config, error_message = self._provider_configs[provider_key]

        if config is None:
            raise ValueError(error_message)

        if provider_key not in self._service_providers:
            self._service_providers[provider_key] = provider_class(self, config)
            self._logger.debug(f"{provider_class.__name__} initialized")

        return self._service_providers[provider_key]

    @property
    def db(self) -> DbProvider:
        return self._get_provider("db")

    @property
    def gdal(self) -> GDALProvider:
        return self._get_provider("gdal")

    @property
    def s3(self) -> ObjectStorageProvider:
        return self._get_provider("s3")

    @property
    def mail(self) -> MailProvider:
        return self._get_provider("mail")

    @property
    def pdf(self) -> PDFProvider:
        return self._get_provider("pdf")
