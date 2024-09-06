import sys
import logging
import asyncio
from configparser import ConfigParser

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, PDFCoConfig, S3Config


task_registry = {}
task_registry_post = {}


def fundermaps_task(func):
    task_registry[func.__name__] = func
    return func


def fundermaps_task_post(func):
    task_registry_post[func.__name__] = func
    return func


class FunderMapsTask:
    def __init__(
        self, fundermaps: FunderMapsSDK, name: str, logger: logging.Logger = None
    ):
        self.fundermaps = fundermaps
        self.name = name
        self.logger = logger or logging.getLogger(name)

    async def run(self):
        raise NotImplementedError("Method 'run' must be implemented")

    async def post_run(self):
        pass

    async def __call__(self):
        try:
            self.logger.info(f"Starting task '{self.name}'")
            await self.run()
            self.logger.info(f"Task finished '{self.name}'")
        except Exception as e:
            self.logger.error("An error occurred", exc_info=e)
        finally:
            await self.post_run()


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

    async def _run_tasks(self, fundermaps: FunderMapsSDK):
        """
        Run tasks and post tasks using the provided FunderMapsSDK instance.

        Args:
            fundermaps (FunderMapsSDK): An instance of the FunderMapsSDK.

        Returns:
            None
        """

        try:
            for task_name, task_func in task_registry.items():
                self.logger.debug(f"Running task '{task_name}'")
                await task_func(fundermaps)
        finally:
            for task_name, task_func in task_registry_post.items():
                self.logger.debug(f"Running post task '{task_name}'")
                await task_func(fundermaps)

    def run(self):
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
            asyncio.run(self._run_tasks(fundermaps))
            self.logger.info("Application finished")

        except Exception as e:
            self.logger.error("An error occurred", exc_info=e)
            sys.exit(1)
