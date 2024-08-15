import os
import sys
import logging
import asyncio
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.util import find_config, http_download_file
from fundermapssdk.config import DatabaseConfig


BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
FILE_NAME: str = "bag-light.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024 * 1024

logger = logging.getLogger("loadbag")


async def clean_db(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        logger.info("Removing previous data")
        db.drop_table("public.woonplaats")
        db.drop_table("public.verblijfsobject")
        db.drop_table("public.pand")
        db.drop_table("public.ligplaats")
        db.drop_table("public.standplaats")
        db.drop_table("public.openbare_ruimte")
        db.drop_table("public.nummeraanduiding")


async def run(config):
    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fundermaps = FunderMapsSDK(db_config=db_config)

    logger.info("Downloading BAG file")
    await http_download_file(BASE_URL_BAG, FILE_NAME)

    logger.info("Checking BAG file")
    if not os.path.exists(FILE_NAME):
        raise FileNotFoundError("BAG file not found")
    if os.path.getsize(FILE_NAME) < FILE_MIN_SIZE:
        raise ValueError("BAG file is below 1GB")

    logger.info("Cleaning database")
    await clean_db(fundermaps)

    logger.info("Loading BAG file into database")
    await fundermaps.gdal.convert(
        FILE_NAME,
        "PG:dbname=fundermaps",
    )

    with fundermaps.db as db:
        logger.info("Loading buildings into geocoder")
        db.execute_script("load_building")
        db.reindex_table("geocoder.building")

        logger.info("Loading addressess into geocoder")
        db.execute_script("load_address")
        db.reindex_table("geocoder.address")

        logger.info("Loading residences into geocoder")
        db.execute_script("load_residence")
        db.reindex_table("geocoder.residence")

    logger.info("Cleaning database")
    await clean_db(fundermaps)


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
        logger.info("Starting 'loadbag'")
        asyncio.run(run(config))
        logger.info("Finished 'loadbag'")
    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)
