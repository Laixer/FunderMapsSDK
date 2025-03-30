import csv
import os
import asyncio
import logging
import argparse
import colorlog
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config

# TODO: Get from the database
ORGANIZATIONS: list[str] = [
    "5c2c5822-6996-4306-96ba-6635ea7f90e2",
    "8a56e920-7811-47b7-9289-758c8fe346db",
    "c06a1fc6-6452-4b88-85fd-ba50016c578f",
    "58872000-cb69-433a-91ba-165a9d0b4710",
    "0ca4d02d-8206-4453-ba45-84f532c868f3",
    "3f48a4f9-0277-4c68-8be8-55df0fa2645c",
]


async def process_export(
    fundermaps: FunderMapsSDK, logger: logging.Logger, organization: str
):
    logger.info("Exporting product tracker data")

    with fundermaps.db as db:
        with db.db.cursor() as cur:
            query = """
                SELECT
                        pt.organization_id,
                        pt.product,
                        pt.building_id,
                        b.external_id,
                        pt.create_date,
                        pt.identifier AS request
                FROM    application.product_tracker AS pt
                JOIN    geocoder.building AS b ON b.id = pt.building_id
                WHERE   pt.organization_id = %s
                AND     pt.create_date >= date_trunc('month', CURRENT_DATE) - interval '1 month'
                AND     pt.create_date < date_trunc('month', CURRENT_DATE)"""

            cur.execute(query, (organization,))

            csv_file = f"{organization}.csv"

            # TODO: Maybe create a CSV writer helper function in the SDK
            column_names = [desc[0] for desc in cur.description]

            logger.info(f"Writing data to {csv_file}")
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)

                writer.writerow(column_names)

                data_written = False
                for row in cur:
                    writer.writerow(row)
                    data_written = True

    if data_written:
        with fundermaps.s3 as s3:
            current_date = datetime.now()
            formatted_date_year = current_date.strftime("%Y")
            formatted_date_month = current_date.strftime("%b").lower()

            logger.info(f"Uploading {csv_file} to S3")

            s3_path = f"product/{formatted_date_year}/{formatted_date_month}/{organization}.csv"
            s3.upload_file(csv_file, s3_path, bucket="fundermaps-data")
    else:
        logger.info("No data to export")


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

    logger = colorlog.getLogger("export product")
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
    logger.info("Starting product export process...")

    try:
        # TODO: Fetch the organization IDs from the database
        for organization in ORGANIZATIONS:
            logger.info(f"Processing organization: {organization}")
            await process_export(fundermaps, organization)

        total_elapsed = time.time() - start_time
        logger.info(f"Export product completed successfully in {total_elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Error during product export: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
