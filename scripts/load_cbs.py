import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import util, app


# BASE_URL_CBS: str = (
#     "https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/downloads/wijkenbuurten_2023_v1.gpkg"
# )
FILE_NAME: str = "wijkenbuurten_2023_v1.gpkg"
FILE_MIN_SIZE: int = 1024 * 1024

logger = logging.getLogger("loadcbs")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    if len(args) < 1:
        logger.error("Missing URL argument")
        return

    url = args[0]

    logger.info("Downloading CBS file")
    await fundermaps.file.http_download(url, FILE_NAME, FILE_MIN_SIZE)

    logger.info("Checking CBS file")
    util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

    logger.info("Loading CBS file into database")
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
