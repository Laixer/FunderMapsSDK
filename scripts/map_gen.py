import os
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor

from fundermapssdk import FunderMapsSDK, util, app
from fundermapssdk.tippecanoe import tippecanoe


class TileBundle:
    def __init__(self, tileset: str, min_zoom: int, max_zoom: int):
        self.tileset = tileset
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom

    def __str__(self):
        return f"{self.name} ({self.tileset})"


BUCKET: str = "fundermaps-tileset"
BUNDLES: list[TileBundle] = [
    TileBundle("analysis_foundation", 12, 16),
    TileBundle("analysis_report", 12, 16),
    TileBundle("analysis_building", 12, 16),
    TileBundle("analysis_risk", 12, 16),
    TileBundle("analysis_monitoring", 12, 16),
    TileBundle("facade_scan", 12, 16),
    TileBundle("incident", 10, 15),
    TileBundle("incident_neighborhood", 10, 16),
    TileBundle("incident_municipality", 7, 11),
    TileBundle("incident_district", 10, 16),
    TileBundle("building_cluster", 12, 16),
    TileBundle("building_supercluster", 12, 16),
]
TILE_CACHE_MAX_AGE: int = 60 * 60 * 24  # 24 hours

logger = logging.getLogger("map_gen")


async def process_tileset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger.info(f"Dowloading tileset '{tileset.tileset}'")
    await fundermaps.gdal.from_postgis(
        f"{tileset.tileset}.gpkg", f"maplayer.{tileset.tileset}"
    )

    logger.info(f"Converting tileset '{tileset.tileset}' to GeoJSON")
    await fundermaps.gdal.convert(
        f"{tileset.tileset}.gpkg",
        f"{tileset.tileset}.geojson",
    )

    logger.info(f"Generating tileset '{tileset.tileset}'")
    await tippecanoe(
        f"{tileset.tileset}.geojson",
        tileset.tileset,
        tileset.tileset,
        tileset.max_zoom,
        tileset.min_zoom,
    )

    with fundermaps.s3 as s3:
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
                    "CacheControl": f"max-age={TILE_CACHE_MAX_AGE}",
                    "ContentType": "application/x-protobuf",
                    "ContentEncoding": "gzip",
                    "ACL": "public-read",
                },
            )

            logger.debug(f"Uploaded {local_path} to s3://{BUCKET}/{local_path}")

        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(upload_file, tile_files)

    util.remove_files(".", extension=".gpkg")
    util.remove_files(".", extension=".geojson")
    util.remove_files(".", extension=".mbtiles")

    if os.path.isdir(tileset.tileset):
        shutil.rmtree(tileset.tileset)


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    for tileset in BUNDLES:
        logger.info(f"Processing tileset '{tileset.tileset}'")
        await process_tileset(fundermaps, tileset)
