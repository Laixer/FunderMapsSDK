import sys
import logging
import asyncio
from configparser import ConfigParser

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, PDFCoConfig, S3Config


task_registry = {}


def fundermaps_task(func):
    """
    Decorator function to register a task in the FunderMaps SDK.

    Parameters:
        func (function): The task function to be registered.

    Returns:
        function: The registered task function.
    """
    task_registry[func.__name__] = func
    return func


class App:
    """
    Represents an application.

    Args:
        config (ConfigParser): The configuration parser object.
        logger (logging.Logger): The logger object.

    Attributes:
        config (ConfigParser): The configuration parser object.
        logger (logging.Logger): The logger object.

    Methods:
        _run_tasks: Runs the tasks.
        run: Runs the application.

    """

    def __init__(self, config: ConfigParser, logger: logging.Logger):
        """
        Initializes a new instance of the App class.

        Args:
            config (ConfigParser): The configuration parser object.
            logger (logging.Logger): The logger object for logging.

        Returns:
            None
        """

        self.config = config
        self.logger = logger

    async def _run_tasks(self, fundermaps: FunderMapsSDK, *args):
        """
        Run the tasks.

        Args:
            fundermaps (FunderMapsSDK): An instance of the FunderMapsSDK.

        Returns:
            None
        """

        for task_name, task_func in task_registry.items():
            self.logger.debug(f"Running task '{task_name}'")
            await task_func(fundermaps, *args)

    async def invoke(self, *args):
        """
        Run the application.

        This method initializes the necessary configurations for the application,
        creates an instance of the `FunderMapsSDK` class, and runs the tasks using
        asyncio.

        Raises:
            Exception: If an error occurs during the execution of the application.

        """

        try:
            if self.config.has_section("database"):
                db_config = DatabaseConfig(
                    database=self.config.get("database", "database"),
                    host=self.config.get("database", "host"),
                    user=self.config.get("database", "username"),
                    password=self.config.get("database", "password"),
                    port=self.config.getint("database", "port"),
                )
            else:
                db_config = None

            if self.config.has_section("s3"):
                s3_config = S3Config(
                    access_key=self.config.get("s3", "access_key"),
                    secret_key=self.config.get("s3", "secret_key"),
                    service_uri=self.config.get("s3", "service_uri"),
                    bucket=self.config.get("s3", "bucket"),
                )
            else:
                s3_config = None

            if self.config.has_section("pdf"):
                pdf_config = PDFCoConfig(api_key=self.config.get("pdf", "api_key"))
            else:
                pdf_config = None

            fundermaps = FunderMapsSDK(
                db_config=db_config, s3_config=s3_config, pdf_config=pdf_config
            )

            self.logger.info("Starting application")
            await self._run_tasks(fundermaps, *args)
            self.logger.info("Application finished")

        except Exception as e:
            self.logger.error("An error occurred", exc_info=e)
            sys.exit(1)

    def asyncio_invoke(self, *args):
        asyncio.run(self.invoke(*args))
