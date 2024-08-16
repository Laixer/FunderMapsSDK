import os
import logging
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import App, fundermaps_task, fundermaps_task_post
from fundermapssdk.util import find_config, http_download_file


BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
FILE_NAME: str = "bag-light.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024 * 1024

logger = logging.getLogger("loadbag")


async def clean_db(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        db.drop_table("public.woonplaats")
        db.drop_table("public.verblijfsobject")
        db.drop_table("public.pand")
        db.drop_table("public.ligplaats")
        db.drop_table("public.standplaats")
        db.drop_table("public.openbare_ruimte")
        db.drop_table("public.nummeraanduiding")


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
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


@fundermaps_task_post
async def run_post(fundermaps: FunderMapsSDK):
    logger.info("Cleaning database")
    await clean_db(fundermaps)


if __name__ == "__main__":
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    # Set up logging to console
    logging.basicConfig(level=logging.INFO, handlers=[handler])

    # Find and read the configuration file
    config = find_config()

    # Run the application
    App(config, logger).run()
