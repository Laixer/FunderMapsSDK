import logging

from fundermapssdk.db import DbProvider
from fundermapssdk.mail import MailProvider
from fundermapssdk.config import DatabaseConfig, MailConfig

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

    def __init__(
        self,
        mail_config: MailConfig | None = None,
        db_config: DatabaseConfig | None = None,
    ):
        self.mail_config = mail_config
        self.db_config = db_config

        self._service_providers = {}
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

    @property
    def mail(self):
        return self._mail_provider()

    @property
    def db(self):
        return self._db_provider()
