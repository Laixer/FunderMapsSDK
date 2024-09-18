import logging

from fundermapssdk import FunderMapsSDK, util, app


BUCKET: str = "fundermaps"

LAYERS: list[str] = [
    "maplayer.analysis_full",
    "maplayer.incident_district",
    "maplayer.incident_municipality",
    "maplayer.incident_neighborhood",
    "maplayer.facade_scan",
]

logger = logging.getLogger("export_models")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    for layer in LAYERS:
        output_file = f"{layer}.gpkg"

        logger.info(f"Dowloading {layer} from PostGIS")
        await fundermaps.gdal.from_postgis(output_file, layer)

        with fundermaps.s3 as s3:
            logger.info(f"Uploading {output_file} to S3")
            s3_path = f"model/{util.date_path()}/{output_file}"
            await s3.upload_file(BUCKET, output_file, s3_path)
