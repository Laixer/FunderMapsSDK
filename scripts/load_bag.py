import logging

from fundermapssdk import FunderMapsSDK, util, app


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


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    logger.info("Downloading BAG file")
    await util.http_download_file(BASE_URL_BAG, FILE_NAME)

    logger.info("Checking BAG file")
    util.validate_file_size(FILE_NAME, FILE_MIN_SIZE)

    logger.info("Cleaning database")
    await clean_db(fundermaps)

    logger.info("Loading BAG file into database")
    await fundermaps.gdal.to_postgis(FILE_NAME)

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
