import os
import logging
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import App, fundermaps_task
from fundermapssdk.util import find_config, http_download_file

BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"

logger = logging.getLogger("loadbag3d")


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Downloading BAG file")
    await http_download_file(BASE_URL_BAG, "3dbag_nl.gpkg")

    logger.info("Checking BAG file")
    if not os.path.exists("3dbag_nl.gpkg"):
        raise FileNotFoundError("BAG file not found")
    if os.path.getsize("3dbag_nl.gpkg") < 1024 * 1024 * 1024:
        raise ValueError("BAG file is below 1GB")

    logger.info("Loading BAG file into database")
    await fundermaps.gdal.convert(
        "3dbag_nl.gpkg",
        "PG:dbname=fundermaps",
        "lod22_2d",
    )
    await fundermaps.gdal.convert(
        "3dbag_nl.gpkg",
        "PG:dbname=fundermaps",
        "pand",
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
