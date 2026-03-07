import logging
from pathlib import Path
from typing import Any

from fundermapsworker.config import DatabaseConfig, MailConfig, PDFCoConfig, S3Config
from fundermapsworker.providers.db import DbProvider
from fundermapsworker.providers.gdal import GDALProvider
from fundermapsworker.providers.mail import MailProvider
from fundermapsworker.providers.pdf import PDFProvider
from fundermapsworker.providers.storage import ObjectStorageProvider

logger = logging.getLogger(__name__)


class FunderMapsWorker:
    """Main entry point for the FunderMaps worker.

    Provides lazy-initialized access to service providers: database, GDAL,
    object storage, email, and PDF generation.
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
        self.base_directory = Path(__file__).resolve().parent

        self.db_config = db_config
        self.s3_config = s3_config
        self.pdf_config = pdf_config
        self.mail_config = mail_config

        self._service_providers: dict[str, Any] = {}
        self._logger: logging.Logger = kwargs.get("logger", logger)

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


# Backwards compatibility alias
FunderMapsSDK = FunderMapsWorker
