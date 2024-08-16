import os
import logging
from concurrent.futures import ThreadPoolExecutor

from fundermapssdk import FunderMapsSDK
from fundermapssdk.tippecanoe import tippecanoe
from fundermapssdk import util, app


BUCKET: str = "tileset-test"

logger = logging.getLogger("map_gen")


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

        tile_files = []

        logger.info(f"Uploading tileset '{tileset.tileset}' to tileset bucket")
        for root, _, files in os.walk(tileset.tileset):
            for filename in files:
                file_ext = os.path.splitext(filename)[1]
                if file_ext != ".pbf":
                    continue

                local_path = os.path.join(root, filename)
                tile_files.append(local_path)

        def upload_file(local_path):
            s3.client.upload_file(
                local_path,
                BUCKET,
                local_path,
                ExtraArgs={
                    "CacheControl": "max-age=60",
                    "ContentType": "application/x-protobuf",
                    "ContentEncoding": "gzip",
                    "ACL": "public-read",
                },
            )

            logger.debug(f"Uploaded {local_path} to s3://{BUCKET}/{local_path}")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(upload_file, tile_files)


@app.fundermaps_task
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


@app.fundermaps_task_post
async def run_post(fundermaps: FunderMapsSDK):
    util.remove_files(".", extension=".gpkg")
    util.remove_files(".", extension=".geojson")
    util.remove_files(".", extension=".mbtiles")
