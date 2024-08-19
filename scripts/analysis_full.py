import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import app


BUCKET: str = "fundermaps"
FILE_NAME: str = "analysis_full.gpkg"

logger = logging.getLogger("analysis_full")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info(f"Dowloading data")
    await fundermaps.gdal.convert(
        "PG:dbname=fundermaps",
        FILE_NAME,
        "maplayer.analysis_full",
    )

    with fundermaps.s3 as s3:
        from datetime import datetime

        current_date = datetime.now()
        formatted_date_year = current_date.strftime("%Y")
        formatted_date_month = current_date.strftime("%b").lower()
        formatted_date_day = current_date.strftime("%d")

        logger.info(f"Uploading {FILE_NAME} to S3")
        await s3.upload_file(
            BUCKET,
            FILE_NAME,
            f"model/{formatted_date_year}/{formatted_date_month}/{formatted_date_day}/{FILE_NAME}",
        )
