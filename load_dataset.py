import os
import asyncio
import logging
import argparse
import colorlog
import time
from pathlib import Path
from dotenv import load_dotenv

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config


# TODO: Move inoto fundermaps-sdk/util
def load_env_files():
    dotenv_paths = [
        Path(".env"),
        Path(".env.local"),
        Path(os.path.dirname(os.path.abspath(__file__))) / ".env",
    ]

    for dotenv_path in dotenv_paths:
        if dotenv_path.exists():
            load_dotenv(dotenv_path=str(dotenv_path))


# TODO: Move inoto fundermaps-sdk/util
def parse_arguments() -> argparse.Namespace:
    load_env_files()

    parser = argparse.ArgumentParser(description="Load dataset into database")

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

    args = parser.parse_args()

    return args


async def load_dataset(
    fundermaps: FunderMapsSDK,
    logger: logging.Logger,
    dataset_input: str,
    dataset_layer: list[str] = [],
    delete_dataset: bool = False,
):
    dataset_path = dataset_input

    # TODO: This is a task
    if dataset_input.startswith("https://"):
        file_name = dataset_input.split("/")[-1]
        dataset_path = file_name

        logger.info(f"Downloading dataset from URL '{dataset_input}'")
        await util.http_download_file(dataset_input, file_name)

    # TODO: This is a task
    elif dataset_input.startswith("s3://"):
        file_name = dataset_input.split("/")[-1]
        dataset_path = file_name
        s3_path = dataset_input.replace(f"s3://", "")

        logger.info(f"Downloading dataset from S3 '{dataset_input}'")
        with fundermaps.s3 as s3:
            s3.download_file(file_name, s3_path)

    logger.info(f"Validating dataset: {dataset_path}")
    util.validate_file_size(dataset_path, util.FILE_MIN_SIZE)
    util.validate_file_extension(dataset_path, util.FILE_ALLOWED_EXTENSIONS)

    logger.info("Loading dataset into database")
    await fundermaps.gdal.to_postgis(dataset_path, *dataset_layer)

    if delete_dataset and dataset_input.startswith("s3://"):
        logger.info(f"Deleting dataset from S3 '{dataset_input}'")
        # TODO: Delete the file from S3
        pass


async def main() -> int:
    args = parse_arguments()

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

    logger = colorlog.getLogger("load dataset")
    logger.setLevel(getattr(logging, args.log_level))
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False

    db_config = DatabaseConfig(
        database=args.db_name,
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        port=args.db_port,
    )

    s3_config = S3Config(
        bucket=args.s3_bucket,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        service_uri=args.s3_service_uri,
    )

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config, logger=logger)

    start_time = time.time()
    logger.info("Starting dataset loading process...")

    try:
        dataset_input = "https://example.com/dataset.zip"  # Replace with your dataset URL or S3 path
        dataset_layer = ["layer_name"]  # Replace with your dataset layer name
        delete_dataset = (
            False  # Set to True if you want to delete the dataset after loading
        )
        await load_dataset(
            fundermaps=fundermaps,
            logger=logger,
            dataset_input=dataset_input,
            dataset_layer=dataset_layer,
            delete_dataset=delete_dataset,
        )

        total_elapsed = time.time() - start_time
        logger.info(f"Loading dataset completed successfully in {total_elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Error during dataset loading: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
