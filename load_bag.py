import os
import sys
import logging
import asyncio

from fundermapssdk import FunderMapsSDK
from fundermapssdk.mail import Email
from fundermapssdk.util import http_download_file
from fundermapssdk.config import MailConfig, DatabaseConfig
from fundermapssdk.gdal import convert

BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"

logger = logging.getLogger("loadbag")


async def download_bag_file():
    """
    Download the BAG file from the PDOK service.
    """

    await http_download_file(BASE_URL_BAG, "bag-light.gpkg")


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
    await download_bag_file()

    logger.info("Checking BAG file")
    if not os.path.exists("bag-light.gpkg"):
        raise FileNotFoundError("BAG file not found")
    if os.path.getsize("bag-light.gpkg") < 1024 * 1024 * 1024:
        raise ValueError("BAG file is below 1GB")

    with fundermaps.db as db:
        logger.info("Removing previous data")
        db.drop_table("public.woonplaats")
        db.drop_table("public.verblijfsobject")
        db.drop_table("public.pand")
        db.drop_table("public.ligplaats")
        db.drop_table("public.standplaats")
        db.drop_table("public.openbare_ruimte")
        db.drop_table("public.nummeraanduiding")

    logger.info("Loading BAG file into database")
    await convert(
        "bag-light.gpkg",
        "PG:dbname=fundermaps host=db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com user=fundermaps password=AVNS_CtcfLEuVWqRXiK__gKt port=25060",
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

    with fundermaps.db as db:
        logger.info("Cleaning up database")
        db.drop_table("public.woonplaats")
        db.drop_table("public.verblijfsobject")
        db.drop_table("public.pand")
        db.drop_table("public.ligplaats")
        db.drop_table("public.standplaats")
        db.drop_table("public.openbare_ruimte")
        db.drop_table("public.nummeraanduiding")


def main():
    import colorlog
    from configparser import ConfigParser

    config = ConfigParser()

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config.read("config.ini")

    try:
        logger.info("Starting 'loadbag'")
        asyncio.run(run(config))
        logger.info("Finished 'loadbag'")
    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)


if __name__ == "__main__":
    main()
