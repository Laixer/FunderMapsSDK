import os
import asyncio
import tempfile

from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.cache_policies import NO_CACHE

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config
from fundermapssdk.tippecanoe import tippecanoe


class TileBundle:
    def __init__(
        self,
        tileset: str,
        min_zoom: int,
        max_zoom: int,
        upload_dataset: bool = False,
        generate_tiles: bool = True,
    ):
        self.tileset = tileset
        self.min_zoom = min_zoom
        self.max_zoom = max_zoom
        self.upload_dataset = upload_dataset
        self.generate_tiles = generate_tiles

    def __str__(self):
        return f"{self.name} ({self.tileset})"


# TODO: Get from the database
BUCKET: str = "fundermaps-tileset"
BUNDLES: list[TileBundle] = [
    TileBundle("analysis_foundation", 12, 16),
    TileBundle("analysis_report", 12, 16),
    TileBundle("analysis_building", 12, 16),
    TileBundle("analysis_risk", 12, 16),
    TileBundle("analysis_monitoring", 12, 16),
    TileBundle("facade_scan", 12, 16, upload_dataset=True),
    TileBundle("incident", 12, 16, upload_dataset=True),
    TileBundle("incident_neighborhood", 10, 16, upload_dataset=True),
    TileBundle("incident_municipality", 7, 11, upload_dataset=True),
    TileBundle("incident_district", 10, 16, upload_dataset=True),
    #
    # TileBundle("building_cluster", 12, 16), # TODO: Missing tileset?
    # TileBundle("building_supercluster", 12, 16), # TODO: Missing tileset?
    #
    # TileBundle("boundry_municipality", 7, 11),
    # TileBundle("boundry_district", 10, 16),
    # TileBundle("boundry_neighborhood", 10, 16),
    #
    TileBundle("analysis_full", 10, 16, upload_dataset=True, generate_tiles=False),
]
TILE_CACHE_MAX_AGE: int = 60 * 60 * 6  # 6 hours


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

        s3_path = f"model/{util.date_path()}/{tileset.tileset}.gpkg"
        s3.upload_file(f"{tileset.tileset}.gpkg", s3_path)


@flow(name="Process Mapset")
async def process_mapset():
    db_config = DatabaseConfig(
        database="fundermaps",
        host="db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
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

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

    # TODO: Fetch the tileset names from the database
    for tileset in BUNDLES:
        logger.info(f"Processing tileset '{tileset.tileset}'")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            os.chdir(tmp_dir)

            await download_dataset(fundermaps, tileset)

            if tileset.upload_dataset:
                upload_dataset(fundermaps, tileset)

            if tileset.generate_tiles:
                await generate_tileset(fundermaps, tileset)

                tile_files = util.collect_files_with_extension(tileset.tileset, ".pbf")

                with fundermaps.s3 as s3:
                    tile_headers = {
                        "CacheControl": f"max-age={TILE_CACHE_MAX_AGE}",
                        "ContentType": "application/x-protobuf",
                        "ContentEncoding": "gzip",
                        "ACL": "public-read",
                    }

                    s3.upload_bulk(tile_files, extra_args=tile_headers)


if __name__ == "__main__":
    asyncio.run(process_mapset())
