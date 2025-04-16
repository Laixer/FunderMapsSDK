import os
import time
import logging
import argparse
import colorlog
from pathlib import Path
from dotenv import load_dotenv

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config


class FunderMapsCommand:
    """Base class for FunderMaps CLI commands."""

    def __init__(self, description: str):
        self.description = description
        self.logger = None
        self.fundermaps = None
        self.args = None
        self.start_time = None

    def _load_env_files(self):
        """Load environment variables from .env files."""
        dotenv_paths = [
            Path(".env"),
            Path(".env.local"),
            Path(os.path.dirname(os.path.abspath(__file__))) / ".." / ".env",
            Path("/etc/fundermaps/config.env"),
        ]

        for dotenv_path in dotenv_paths:
            if dotenv_path.exists():
                load_dotenv(dotenv_path=str(dotenv_path))

    def _setup_argument_parser(self) -> argparse.ArgumentParser:
        """Set up argument parser with common arguments."""
        self._load_env_files()

        parser = argparse.ArgumentParser(description=self.description)

        db_group = parser.add_argument_group("Database Configuration")
        db_group.add_argument(
            "--db-host",
            default=os.environ.get("FUNDERMAPS_DB_HOST"),
            help="Database host (env: FUNDERMAPS_DB_HOST)",
        )
        db_group.add_argument(
            "--db-name",
            default=os.environ.get("FUNDERMAPS_DB_NAME"),
            help="Database name (env: FUNDERMAPS_DB_NAME)",
        )
        db_group.add_argument(
            "--db-user",
            default=os.environ.get("FUNDERMAPS_DB_USER"),
            help="Database user (env: FUNDERMAPS_DB_USER)",
        )
        db_group.add_argument(
            "--db-password",
            default=os.environ.get("FUNDERMAPS_DB_PASSWORD"),
            help="Database password (env: FUNDERMAPS_DB_PASSWORD)",
        )
        db_group.add_argument(
            "--db-port",
            type=int,
            default=int(os.environ.get("FUNDERMAPS_DB_PORT", "5432")),
            help="Database port (env: FUNDERMAPS_DB_PORT)",
        )

        s3_group = parser.add_argument_group("S3 Configuration")
        s3_group.add_argument(
            "--s3-bucket",
            default=os.environ.get("FUNDERMAPS_S3_BUCKET"),
            help="S3 bucket name (env: FUNDERMAPS_S3_BUCKET)",
        )
        s3_group.add_argument(
            "--s3-access-key",
            default=os.environ.get("FUNDERMAPS_S3_ACCESS_KEY"),
            help="S3 access key (env: FUNDERMAPS_S3_ACCESS_KEY)",
        )
        s3_group.add_argument(
            "--s3-secret-key",
            default=os.environ.get("FUNDERMAPS_S3_SECRET_KEY"),
            help="S3 secret key (env: FUNDERMAPS_S3_SECRET_KEY)",
        )
        s3_group.add_argument(
            "--s3-service-uri",
            default=os.environ.get("FUNDERMAPS_S3_SERVICE_URI"),
            help="S3 service URI (env: FUNDERMAPS_S3_SERVICE_URI)",
        )

        parser.add_argument(
            "--log-level",
            default=os.environ.get("FUNDERMAPS_LOG_LEVEL", "INFO"),
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help="Logging level (env: FUNDERMAPS_LOG_LEVEL)",
        )
        parser.add_argument(
            "--log-simple",
            action="store_true",
            default=os.environ.get("FUNDERMAPS_LOG_SIMPLE", False),
            help="Use simple log format (message only) for syslog/systemd (env: FUNDERMAPS_LOG_SIMPLE)",
        )

        # Allow subclasses to add their own arguments
        self.add_arguments(parser)

        return parser

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """Add command-specific arguments. Override in subclasses."""
        pass

    def _setup_logging(self, name: str) -> logging.Logger:
        """Set up logger with consistent formatting."""
        if self.args.log_simple:
            formatter = logging.Formatter("%(message)s")
        else:
            formatter = colorlog.ColoredFormatter(
                "%(thin_white)s%(asctime)s%(reset)s | "
                "%(bold_blue)s%(name)s%(reset)s | "
                "%(log_color)s%(levelname)-8s%(reset)s | "
                "%(message_log_color)s%(message)s%(reset)s",
                datefmt="%H:%M:%S",
                reset=True,
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "bold_green",
                    "WARNING": "bold_yellow",
                    "ERROR": "bold_red",
                    "CRITICAL": "bold_white,bg_red",
                },
                secondary_log_colors={
                    "message": {
                        "DEBUG": "cyan",
                        "INFO": "white",
                        "WARNING": "yellow",
                        "ERROR": "red",
                        "CRITICAL": "red",
                    }
                },
                style="%",
            )

        handler = colorlog.StreamHandler()
        handler.setFormatter(formatter)

        logger = colorlog.getLogger(name)
        logger.setLevel(getattr(logging, self.args.log_level))
        logger.handlers = []
        logger.addHandler(handler)
        logger.propagate = False

        return logger

    def _initialize_sdk(self) -> FunderMapsSDK:
        """Initialize FunderMapsSDK with configuration from args."""
        db_config = DatabaseConfig(
            database=self.args.db_name,
            host=self.args.db_host,
            user=self.args.db_user,
            password=self.args.db_password,
            port=self.args.db_port,
        )

        s3_config = S3Config(
            bucket=self.args.s3_bucket,
            access_key=self.args.s3_access_key,
            secret_key=self.args.s3_secret_key,
            service_uri=self.args.s3_service_uri,
        )

        return FunderMapsSDK(
            db_config=db_config, s3_config=s3_config, logger=self.logger
        )

    async def execute(self) -> None:
        """Execute the command. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement execute()")

    async def pre_execute(self) -> None:
        """Hook for subclasses to perform actions before execute.

        This method runs after initialization but before the main execute call.
        Override in subclasses to add custom pre-execution logic.
        """
        pass

    async def post_execute(self, success: bool) -> None:
        """Hook for subclasses to perform actions after execute.

        Args:
            success: Whether the execution was successful

        This method runs after execute completes, regardless of success or failure.
        Override in subclasses to add custom post-execution logic.
        """
        pass

    async def run(self) -> int:
        """Run the command with setup and error handling."""
        parser = self._setup_argument_parser()
        self.args = parser.parse_args()

        self.logger = self._setup_logging(self.__class__.__name__)
        self.fundermaps = self._initialize_sdk()

        self.start_time = time.time()
        self.logger.info(f"Starting {self.description.lower()}...")

        success = False
        try:
            await self.pre_execute()
            await self.execute()
            success = True

            total_elapsed = time.time() - self.start_time
            self.logger.info(f"Command completed successfully in {total_elapsed:.2f}s")
            return 0
        except Exception as e:
            self.logger.error(f"Error during execution: {e}")
            import traceback

            traceback.print_exc()
            return 1
        finally:
            await self.post_execute(success)
