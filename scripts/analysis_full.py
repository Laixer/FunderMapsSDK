import logging

from fundermapssdk import FunderMapsSDK, util, app


BUCKET: str = "fundermaps"
OUTPUT_FILE_NAME: str = "analysis_full.gpkg"
LAYER_NAME: str = "maplayer.analysis_full"

logger = logging.getLogger("analysis_full")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    logger.info(f"Dowloading data")
    await fundermaps.gdal.convert("PG:dbname=fundermaps", OUTPUT_FILE_NAME, LAYER_NAME)
    # await fundermaps.gdal.from_postgis(OUTPUT_FILE_NAME, LAYER_NAME)

    with fundermaps.s3 as s3:
        logger.info(f"Uploading {OUTPUT_FILE_NAME} to S3")
        s3_path = f"model/{util.date_path()}/{OUTPUT_FILE_NAME}"
        await s3.upload_file(BUCKET, OUTPUT_FILE_NAME, s3_path)
