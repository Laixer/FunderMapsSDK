import logging
from datetime import datetime

from fundermapssdk import FunderMapsSDK
from fundermapssdk import app


BUCKET: str = "fundermaps"
OUTPUT_FILE_NAME: str = "analysis_full.gpkg"
LAYER_NAME: str = "maplayer.analysis_full"

logger = logging.getLogger("analysis_full")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info(f"Dowloading data")
    await fundermaps.gdal.convert("PG:dbname=fundermaps", OUTPUT_FILE_NAME, LAYER_NAME)

    with fundermaps.s3 as s3:
        current_date = datetime.now()
        formatted_date_year = current_date.strftime("%Y")
        formatted_date_month = current_date.strftime("%b").lower()
        formatted_date_day = current_date.strftime("%d")

        logger.info(f"Uploading {OUTPUT_FILE_NAME} to S3")
        s3_path = f"model/{formatted_date_year}/{formatted_date_month}/{formatted_date_day}/{OUTPUT_FILE_NAME}"
        await s3.upload_file(BUCKET, OUTPUT_FILE_NAME, s3_path)
