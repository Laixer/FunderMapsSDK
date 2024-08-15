import os
import sys
import logging
import asyncio
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config
from fundermapssdk.tippecanoe import tippecanoe
from fundermapssdk.util import find_config, remove_files


logger = logging.getLogger("map_gen")

task_registry = {}
task_registry_post = {}


def fundermaps_task(func):
    """Decorator to register a function."""
    task_registry[func.__name__] = func
    return func


def fundermaps_task_post(func):
    """Decorator to register a function."""
    task_registry_post[func.__name__] = func
    return func


class TileBundle:
    def __init__(self, name: str, tileset: str, min_zoom: int, max_zoom: int):
        self.name = name
        self.tileset = tileset
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom

    def __str__(self):
        return f"{self.name} ({self.tileset})"


async def process_tileset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger.info(f"Dowloading tileset '{tileset.tileset}'")
    await fundermaps.gdal.convert(
        "PG:dbname=fundermaps",
        f"{tileset.tileset}.gpkg",
        f"maplayer.{tileset.tileset}",
    )

    logger.info(f"Converting tileset '{tileset.tileset}' to GeoJSON")
    await fundermaps.gdal.convert(
        f"{tileset.tileset}.gpkg",
        f"{tileset.tileset}.geojson",
    )

    logger.info(f"Generating tileset '{tileset.tileset}'")
    # await tippecanoe(f"{tileset}.geojson", f"{tileset}.mbtiles", tileset, 16, 12)
    await tippecanoe(
        f"{tileset.tileset}.geojson",
        tileset.tileset,
        tileset.tileset,
        tileset.max_zoom,
        tileset.min_zoom,
    )

    # TODO: This is where we would upload the tileset to Mapbox

    with fundermaps.s3 as s3:
        # from datetime import datetime

        # current_date = datetime.now()
        # formatted_date = current_date.strftime("%Y-%m-%d")

        # logger.info(f"Archiving tileset '{tileset}'")
        # await s3.upload_file(
        #     f"{tileset}.gpkg", f"tileset/archive/{formatted_date}/{tileset}.gpkg"
        # )

        # logger.info(f"Storing tileset '{tileset}'")
        # await s3.upload_file(f"{tileset}.gpkg", f"tileset/{tileset}.gpkg")

        logger.info(f"Uploading tileset '{tileset.tileset}' to tileset bucket")
        for root, _, files in os.walk(tileset.tileset):
            for filename in files:
                file_ext = os.path.splitext(filename)[1]
                if file_ext != ".pbf":
                    continue

                local_path = os.path.join(root, filename)

                s3.client.upload_file(
                    local_path,
                    fundermaps.s3_config.bucket,
                    local_path,
                    ExtraArgs={
                        "CacheControl": "max-age=60",
                        "ContentType": "application/x-protobuf",
                        "ContentEncoding": "gzip",
                        "ACL": "public-read",
                    },
                )

                logger.debug(
                    f"Uploaded {local_path} to s3://{fundermaps.s3_config.bucket}/{local_path}"
                )


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    tile_bundles = [
        # TileBundle("Analysis Foundation", "analysis_foundation", 12, 16),
        # TileBundle("Analysis Report", "analysis_report", 12, 16),
        # TileBundle("Analysis Building", "analysis_building", 12, 16),
        # TileBundle("Analysis Risk", "analysis_risk", 12, 16),
        # TileBundle("Analysis Monitoring", "analysis_monitoring", 12, 16),
        #
        TileBundle("Facade Scan", "facade_scan", 12, 16),
        #
        TileBundle("Incidents", "incident", 10, 15),
        TileBundle("Incidents per neighborhood", "incident_neighborhood", 10, 16),
        TileBundle("Incidents per municipality", "incident_municipality", 7, 11),
        TileBundle("Incidents per district", "incident_district", 10, 16),
    ]

    for tileset in tile_bundles:
        logger.info(f"Processing tileset '{tileset.name}'")
        await process_tileset(fundermaps, tileset)


@fundermaps_task_post
async def run_post(fundermaps: FunderMapsSDK):
    remove_files(".", extension=".gpkg")
    remove_files(".", extension=".geojson")


if __name__ == "__main__":
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config = find_config()

    try:
        db_config = DatabaseConfig(
            database=config.get("database", "database"),
            host=config.get("database", "host"),
            user=config.get("database", "username"),
            password=config.get("database", "password"),
            port=config.getint("database", "port"),
        )
        s3_config = S3Config(
            access_key=config.get("s3", "access_key"),
            secret_key=config.get("s3", "secret_key"),
            service_uri=config.get("s3", "service_uri"),
            bucket=config.get("s3", "bucket"),
        )
        fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

        async def run_tasks():
            try:
                for task_name, task_func in task_registry.items():
                    logger.debug(f"Running task '{task_name}'")
                    await task_func(fundermaps)
            finally:
                for task_name, task_func in task_registry_post.items():
                    logger.debug(f"Running post task '{task_name}'")
                    await task_func(fundermaps)

        logger.info("Starting 'map_gen'")
        asyncio.run(run_tasks())
        logger.info("Finished 'map_gen'")

    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)
