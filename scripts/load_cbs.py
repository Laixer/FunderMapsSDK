import os
import logging
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import App, fundermaps_task, fundermaps_task_post
from fundermapssdk.util import find_config, http_download_file, remove_files


BASE_URL_CBS: str = (
    "https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/downloads/wijkenbuurten_2023_v1.gpkg"
)
FILE_NAME: str = "wijkenbuurten_2023_v1.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024

logger = logging.getLogger("loadcbs")


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
    # await clean_db(fundermaps)
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

    App(config, logger).run()
