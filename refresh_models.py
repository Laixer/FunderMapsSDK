import os
import asyncio
import logging
import argparse
import colorlog
import time
from pathlib import Path
from dotenv import load_dotenv

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config


def db_calculate_risk(fundermaps: FunderMapsSDK, logger: logging.Logger):
    logger.info("Starting risk calculation...")
    start_time = time.time()
    with fundermaps.db as db:
        logger.info("Refreshing building_sample view...")
        db.refresh_materialized_view("data.building_sample")

        logger.info("Refreshing cluster_sample view...")
        db.refresh_materialized_view("data.cluster_sample")

        logger.info("Refreshing supercluster_sample view...")
        db.refresh_materialized_view("data.supercluster_sample")

        db.call("data.model_risk_manifest")
        db.reindex_table("data.model_risk_static")
    elapsed = time.time() - start_time
    logger.info(f"Risk calculation completed in {elapsed:.2f}s")


def db_refresh_statistics(fundermaps: FunderMapsSDK, logger: logging.Logger):
    logger.info("Starting statistics refresh...")
    start_time = time.time()
    with fundermaps.db as db:
        views = [
            "data.statistics_product_inquiries",
            "data.statistics_product_inquiry_municipality",
            "data.statistics_product_incidents",
            "data.statistics_product_incident_municipality",
            "data.statistics_product_foundation_type",
            "data.statistics_product_foundation_risk",
            "data.statistics_product_data_collected",
            "data.statistics_product_construction_years",
            "data.statistics_product_buildings_restored",
            "data.statistics_postal_code_foundation_type",
            "data.statistics_postal_code_foundation_risk",
        ]

        for view in views:
            logger.info(f"Refreshing materialized view: {view}")
            view_start = time.time()
            db.refresh_materialized_view(view)
            view_elapsed = time.time() - view_start
            logger.info(f"Refreshed {view} in {view_elapsed:.2f}s")

    elapsed = time.time() - start_time
    logger.info(f"Statistics refresh completed in {elapsed:.2f}s")


def load_env_files():
    dotenv_paths = [
        Path(".env"),
        Path(".env.local"),
        Path(os.path.dirname(os.path.abspath(__file__))) / ".env",
    ]

    for dotenv_path in dotenv_paths:
        if dotenv_path.exists():
            load_dotenv(dotenv_path=str(dotenv_path))


def parse_arguments() -> argparse.Namespace:
    load_env_files()

    parser = argparse.ArgumentParser(description="Process Mapset tilesets")

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

    logger = colorlog.getLogger("process mapset")
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
    logger.info("Starting model refresh process...")

    try:
        logger.info("Step 1: Calculating risk metrics...")
        db_calculate_risk(fundermaps, logger)

        logger.info("Step 2: Refreshing statistics...")
        db_refresh_statistics(fundermaps, logger)

        total_elapsed = time.time() - start_time
        logger.info(f"Model refresh completed successfully in {total_elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Error during model refresh: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
