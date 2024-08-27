import os
import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import app, util

FILE_NAME: str = "/data.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024

logger = logging.getLogger("load_subsidence")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Checking GPKG file")
    util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

    with fundermaps.db as db:
        db.drop_table("public.subsidence_building")

    logger.info("Loading GPKG file into database")
    await fundermaps.gdal.convert(FILE_NAME, "PG:dbname=fundermaps", "Panden")
