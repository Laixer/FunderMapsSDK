import os
import sys
import logging
import asyncio
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.util import find_config, http_download_file, remove_files
from fundermapssdk.config import DatabaseConfig, S3Config


BASE_URL_CBS: str = (
    "https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/downloads/wijkenbuurten_2023_v1.gpkg"
)
FILE_NAME: str = "wijkenbuurten_2023_v1.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024

logger = logging.getLogger("loadcbs")


task_registry = {}
task_registry_post = {}


def fundermaps_task(func):
    """Decorator to register a function."""
    task_registry[func.__name__] = func
    return func


def fundermaps_task_post(func):
    """Decorator to register a function."""
    task_registry_post[func.__name__] = func
    return func


async def clean_db(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        logger.info("Removing previous data")
        db.drop_table("public.wijken")
        db.drop_table("public.buurten")
        db.drop_table("public.gemeenten")


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Downloading CBS file")
    await http_download_file(BASE_URL_CBS, FILE_NAME)

    logger.info("Checking CBS file")
    if not os.path.exists(FILE_NAME):
        raise FileNotFoundError("CBS file not found")
    if os.path.getsize(FILE_NAME) < FILE_MIN_SIZE:
        raise ValueError("CBS file is below 1MB")

    logger.info("Cleaning database")
    await clean_db(fundermaps)

    logger.info("Loading CBS file into database")
    await fundermaps.gdal.convert(
        FILE_NAME,
        "PG:dbname=fundermaps",
    )

    # with fundermaps.db as db:
    #     logger.info("Loading buildings into geocoder")
    #     db.execute_script("load_building")
    #     db.reindex_table("geocoder.building")

    #     logger.info("Loading addressess into geocoder")
    #     db.execute_script("load_address")
    #     db.reindex_table("geocoder.address")

    #     logger.info("Loading residences into geocoder")
    #     db.execute_script("load_residence")
    #     db.reindex_table("geocoder.residence")


@fundermaps_task_post
async def run_post(fundermaps: FunderMapsSDK):
    remove_files(".", extension=".gpkg")


if __name__ == "__main__":
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config = find_config()

    try:
        db_config = DatabaseConfig(
            database=config.get("database", "database"),
            host=config.get("database", "host"),
            user=config.get("database", "username"),
            password=config.get("database", "password"),
            port=config.getint("database", "port"),
        )
        s3_config = S3Config(
            access_key=config.get("s3", "access_key"),
            secret_key=config.get("s3", "secret_key"),
            service_uri=config.get("s3", "service_uri"),
            bucket=config.get("s3", "bucket"),
        )
        fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

        async def run_tasks():
            try:
                for task_name, task_func in task_registry.items():
                    logger.debug(f"Running task '{task_name}'")
                    await task_func(fundermaps)
            finally:
                for task_name, task_func in task_registry_post.items():
                    logger.debug(f"Running post task '{task_name}'")
                    await task_func(fundermaps)

        logger.info("Starting 'loadcbs'")
        asyncio.run(run_tasks())
        logger.info("Finished 'loadcbs'")

    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)
