import os
import asyncio
import tempfile
from dataclasses import dataclass

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config
from fundermapssdk.tippecanoe import tippecanoe


# TODO: Find a suitable name for the flow
# - Mapset, TileSet, TileBundle, MapBundle, MapTile, MapTileset


@dataclass
class TileBundle:
    tileset: str
    min_zoom: int = 12
    max_zoom: int = 16
    upload_dataset: bool = False
    generate_tiles: bool = True

    def __str__(self):
        return f"{self.tileset} ({self.tileset})"


TILE_CACHE: str = (
    "max-age=43200,s-maxage=300,stale-while-revalidate=300,stale-if-error=600"
)


@task(name="Downloading Dataset", retries=3, cache_policy=NO_CACHE)
async def download_dataset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger = get_run_logger()

    logger.info(f"Dowloading '{tileset.tileset}' from PostGIS")

    output_file = f"{tileset.tileset}.gpkg"
    maplayer = f"maplayer.{tileset.tileset}"
    await fundermaps.gdal.from_postgis(output_file, maplayer)


@task(name="Generate Tileset", cache_policy=NO_CACHE)
async def generate_tileset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger = get_run_logger()

    logger.info(f"Converting tileset '{tileset.tileset}' to GeoJSON")
    await fundermaps.gdal.ogr2ogr(
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


@task(name="Upload Dataset", retries=3, cache_policy=NO_CACHE)
def upload_dataset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger = get_run_logger()

    with fundermaps.s3 as s3:
        logger.info(f"Uploading {tileset.tileset} to S3")

        s3_path = f"mapset/{util.date_path()}/{tileset.tileset}.gpkg"
        s3.upload_file(f"{tileset.tileset}.gpkg", s3_path, bucket="fundermaps-data")


@task(name="Process Mapset", cache_policy=NO_CACHE)
async def process_mapset(fundermaps: FunderMapsSDK, tileset: TileBundle):
    logger = get_run_logger()

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        os.chdir(tmp_dir)

        await download_dataset(fundermaps, tileset)

        if tileset.upload_dataset:
            upload_dataset(fundermaps, tileset)

        if tileset.generate_tiles:
            await generate_tileset(fundermaps, tileset)

            # TODO: Move everything to a separate task
            logger.info(f"Uploading {tileset.tileset} to S3")

            tile_files = util.collect_files_with_extension(tileset.tileset, ".pbf")

            with fundermaps.s3 as s3:
                tile_headers = {
                    "CacheControl": TILE_CACHE,
                    "ContentType": "application/x-protobuf",
                    # "ContentEncoding": "gzip",
                    "ACL": "public-read",
                }

                s3.upload_bulk(
                    tile_files, bucket="fundermaps-tileset", extra_args=tile_headers
                )


@flow
async def extract_mapset(tilebundle: list[TileBundle]):
    db_config = DatabaseConfig(
        database="fundermaps",
        # host="db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
        host="private-db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
        user="fundermaps",
        password="AVNS_CtcfLEuVWqRXiK__gKt",
        port=25060,
    )

    s3_config = S3Config(
        bucket="fundermaps-development",
        access_key="LOUSAQJLIXLMIXKTKDW5",
        secret_key="/edoJzt5h5hZok6AzuRzWF79EOzLRw3ywH0WzdbGjAU",
        service_uri="https://ams3.digitaloceanspaces.com",
    )

    logger = get_run_logger()

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config, logger=logger)

    for tileset in tilebundle:
        logger.info(f"Processing tileset '{tileset.tileset}'")

        await process_mapset(fundermaps, tileset)


if __name__ == "__main__":
    asyncio.run(
        extract_mapset(
            [
                TileBundle("analysis_foundation", 12, 16),
                TileBundle("analysis_report", 12, 16),
                TileBundle("analysis_building", 12, 16),
                TileBundle("analysis_risk", 12, 16),
                TileBundle("analysis_monitoring", 12, 16),
                TileBundle("facade_scan", 12, 16, upload_dataset=True),
                TileBundle("incident", 12, 16, upload_dataset=True),
                TileBundle(
                    "incident_neighborhood",
                    10,
                    16,
                    upload_dataset=True,
                ),
                TileBundle("incident_municipality", 7, 11, upload_dataset=True),
                TileBundle("incident_district", 10, 16, upload_dataset=True),
                TileBundle(
                    "analysis_full", 10, 16, upload_dataset=True, generate_tiles=False
                ),
            ]
        )
    )
