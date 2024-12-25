import logging


from fundermapssdk import FunderMapsSDK, app, util

# FILE_NAME: str = "/home/eve/Downloads/groningen-img.gpkg"
# FILE_NAME: str = "/home/eve/Downloads/steenwijkerland.gpkg"
FILE_MIN_SIZE: int = 1024 * 512  # 512 KB

logger = logging.getLogger("load_subsidence")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    if len(args) < 1:
        logger.error("Missing file path argument")
        return

    file_path = args[0]

    logger.info("Checking GPKG file")
    util.validate_file_size(file_path, FILE_MIN_SIZE)

    with fundermaps.db as db:
        db.drop_table("public.subsidence_building")

    logger.info("Loading GPKG file into database")
    await fundermaps.gdal.to_postgis(file_path, "Panden")
