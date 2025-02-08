import os
import asyncio
import tempfile

from prefect import flow
from prefect.logging import get_run_logger

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config

from tasks import *


BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
FILE_NAME: str = "bag-light.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024 * 1024


@flow(name="Load BAG")
async def load_bag():
    db_config = DatabaseConfig(
        database="fundermaps",
        host="db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
        user="fundermaps",
        password="AVNS_CtcfLEuVWqRXiK__gKt",
        port=25060,
    )

    s3_config = S3Config(
        bucket="fundermaps-development",
        access_key="LOUSAQJLIXLMIXKTKDW5",
        secret_key="/edoJzt5h5hZok6AzuRzWF79EOzLRw3ywH0WzdbGjAU",
        service_uri="https://ams3.digitaloceanspaces.com",
    )

    logger = get_run_logger()

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

    url = BASE_URL_BAG

    with tempfile.TemporaryDirectory() as tmp_dir:
        os.chdir(tmp_dir)

        logger.info("Downloading BAG file")
        await util.http_download_file(url, FILE_NAME)

        logger.info("Checking BAG file")
        util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

        logger.info("Cleaning database")
        db_public_clean(fundermaps)

        logger.info("Loading BAG file into database")
        await fundermaps.gdal.to_postgis(FILE_NAME)

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
    asyncio.run(load_bag())
