import logging

from fundermapssdk import FunderMapsSDK, util, app


BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
FILE_NAME: str = "3dbag_nl.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024 * 1024

logger = logging.getLogger("loadbag3d")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Downloading BAG file")
    await util.http_download_file(BASE_URL_BAG, FILE_NAME)

    logger.info("Checking BAG file")
    util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

    logger.info("Loading BAG file into database")
    await fundermaps.gdal.convert(
        FILE_NAME,
        "PG:dbname=fundermaps",
        "lod22_2d",
    )
    await fundermaps.gdal.convert(
        FILE_NAME,
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
