import logging

from fundermapssdk import FunderMapsSDK, app

FILE_NAME: str = "3dbag_nl.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024 * 1024

logger = logging.getLogger("loadbag3d")


# TODO: Define the arguments for the script in the decorator
@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    if len(args) < 1:
        logger.error("Missing URL argument")
        return

    url = args[0]

    logger.info("Downloading BAG file")
    await fundermaps.file.http_download(url, FILE_NAME, FILE_MIN_SIZE)

    logger.info("Checking BAG file")
    util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

    logger.info("Loading BAG file into database")
    await fundermaps.gdal.to_postgis(FILE_NAME, "lod22_2d")
    await fundermaps.gdal.to_postgis(FILE_NAME, "pand")

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
