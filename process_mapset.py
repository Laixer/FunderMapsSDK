import os
import asyncio
import tempfile
import logging
import argparse
import colorlog
import time
import random
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from dataclasses import dataclass, field

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config
from fundermapssdk.tippecanoe import tippecanoe


@dataclass
class TileBundle:
    tileset: str
    min_zoom: int = 12
    max_zoom: int = 16
    upload_dataset: bool = False
    generate_tiles: bool = True
    processing_time: float = field(default=0.0, init=False)
    errors: List[str] = field(default_factory=list, init=False)

    def table_name(self) -> str:
        return f"maplayer.{self.tileset}"

    def __str__(self):
        return f"{self.tileset} ({self.tileset})"


@dataclass
class JobContext:
    tileset: TileBundle
    work_dir: str


TILE_CACHE: str = (
    "max-age=43200,s-maxage=300,stale-while-revalidate=300,stale-if-error=600"
)

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


async def download_dataset(
    fundermaps: FunderMapsSDK,
    context: JobContext,
    logger: logging.Logger,
) -> bool:
    logger.info(f"Downloading '{context.tileset.tileset}' from PostGIS")

    output_file = os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            maplayer = context.tileset.table_name()
            await fundermaps.gdal.from_postgis(output_file, maplayer)
            return True
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait_time = RETRY_DELAY * attempt
                logger.warning(
                    f"Download attempt {attempt} failed for {context.tileset.tileset}. Retrying in {wait_time}s. Error: {e}"
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    f"Failed to download {context.tileset.tileset} after {MAX_RETRIES} attempts: {e}"
                )
                # TODO: Write the error to the context
                context.tileset.errors.append(f"Download failed: {str(e)}")
                return False


async def generate_tileset(
    fundermaps: FunderMapsSDK,
    context: JobContext,
    logger: logging.Logger,
) -> bool:
    try:
        logger.info(f"Converting tileset '{context.tileset.tileset}' to GeoJSON")
        await fundermaps.gdal.ogr2ogr(
            os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg"),
            os.path.join(context.work_dir, f"{context.tileset.tileset}.geojson"),
        )

        logger.info(f"Generating tileset '{context.tileset.tileset}'")
        await tippecanoe(
            os.path.join(context.work_dir, f"{context.tileset.tileset}.geojson"),
            os.path.join(context.work_dir, context.tileset.tileset),
            context.tileset.tileset,
            context.tileset.max_zoom,
            context.tileset.min_zoom,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to generate tileset for {context.tileset.tileset}: {e}")
        context.tileset.errors.append(f"Tileset generation failed: {str(e)}")
        return False


def upload_dataset(
    fundermaps: FunderMapsSDK,
    context: JobContext,
    logger: logging.Logger,
) -> bool:
    try:
        logger.info(f"Uploading {context.tileset.tileset} to S3")

        with fundermaps.s3 as s3:
            s3_path = f"mapset/{util.date_path()}/{context.tileset.tileset}.gpkg"
            s3.upload_file(
                os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg"),
                s3_path,
                bucket="fundermaps-data",
            )
        return True
    except Exception as e:
        logger.error(f"Failed to upload dataset for {context.tileset.tileset}: {e}")
        context.tileset.errors.append(f"Dataset upload failed: {str(e)}")
        return False


async def process_mapset(
    fundermaps: FunderMapsSDK, tileset: TileBundle, logger: logging.Logger
) -> bool:
    start_time = time.time()
    success = True

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        ctx = JobContext(tileset, tmp_dir)

        logger.info(f"Starting processing for {tileset.tileset}")

        if not await download_dataset(fundermaps, ctx, logger):
            success = False
            tileset.processing_time = time.time() - start_time
            return success

        if tileset.upload_dataset:
            if not upload_dataset(fundermaps, ctx, logger):
                success = False

        if tileset.generate_tiles and success:
            if not await generate_tileset(fundermaps, ctx, logger):
                success = False
            else:
                # TODO: Move this to a separate function
                try:
                    logger.info(f"Uploading tiles for {tileset.tileset} to S3")
                    tile_files = util.collect_files_with_extension(
                        os.path.join(tmp_dir, tileset.tileset), ".pbf"
                    )

                    with fundermaps.s3 as s3:
                        tile_headers = {
                            "CacheControl": TILE_CACHE,
                            "ContentType": "application/x-protobuf",
                            "ACL": "public-read",
                        }

                        s3.upload_bulk(
                            tile_files,
                            bucket="fundermaps-tileset",
                            extra_args=tile_headers,
                        )
                except Exception as e:
                    logger.error(f"Failed to upload tiles for {tileset.tileset}: {e}")
                    tileset.errors.append(f"Tile upload failed: {str(e)}")
                    success = False

    tileset.processing_time = time.time() - start_time
    if success:
        logger.info(
            f"Successfully processed {tileset.tileset} in {tileset.processing_time:.2f}s"
        )
    else:
        logger.error(
            f"Failed to process {tileset.tileset} after {tileset.processing_time:.2f}s"
        )

    return success


def get_default_tilebundles() -> List[TileBundle]:
    return [
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
        TileBundle("analysis_full", 10, 16, upload_dataset=True, generate_tiles=False),
    ]


def load_env_files():
    dotenv_paths = [
        Path(".env"),
        Path(".env.local"),
        Path(os.path.dirname(os.path.abspath(__file__))) / ".env",
    ]

    for dotenv_path in dotenv_paths:
        if dotenv_path.exists():
            load_dotenv(dotenv_path=str(dotenv_path))


def parse_arguments() -> argparse.Namespace:
    load_env_files()

    parser = argparse.ArgumentParser(description="Process Mapset tilesets")

    db_group = parser.add_argument_group("Database Configuration")
    db_group.add_argument(
        "--db-host",
        default=os.environ.get("FUNDERMAPS_DB_HOST"),
        help="Database host (env: FUNDERMAPS_DB_HOST)",
    )
    db_group.add_argument(
        "--db-name",
        default=os.environ.get("FUNDERMAPS_DB_NAME"),
        help="Database name (env: FUNDERMAPS_DB_NAME)",
    )
    db_group.add_argument(
        "--db-user",
        default=os.environ.get("FUNDERMAPS_DB_USER"),
        help="Database user (env: FUNDERMAPS_DB_USER)",
    )
    db_group.add_argument(
        "--db-password",
        default=os.environ.get("FUNDERMAPS_DB_PASSWORD"),
        help="Database password (env: FUNDERMAPS_DB_PASSWORD)",
    )
    db_group.add_argument(
        "--db-port",
        type=int,
        default=int(os.environ.get("FUNDERMAPS_DB_PORT", "5432")),
        help="Database port (env: FUNDERMAPS_DB_PORT)",
    )

    s3_group = parser.add_argument_group("S3 Configuration")
    s3_group.add_argument(
        "--s3-bucket",
        default=os.environ.get("FUNDERMAPS_S3_BUCKET"),
        help="S3 bucket name (env: FUNDERMAPS_S3_BUCKET)",
    )
    s3_group.add_argument(
        "--s3-access-key",
        default=os.environ.get("FUNDERMAPS_S3_ACCESS_KEY"),
        help="S3 access key (env: FUNDERMAPS_S3_ACCESS_KEY)",
    )
    s3_group.add_argument(
        "--s3-secret-key",
        default=os.environ.get("FUNDERMAPS_S3_SECRET_KEY"),
        help="S3 secret key (env: FUNDERMAPS_S3_SECRET_KEY)",
    )
    s3_group.add_argument(
        "--s3-service-uri",
        default=os.environ.get("FUNDERMAPS_S3_SERVICE_URI"),
        help="S3 service URI (env: FUNDERMAPS_S3_SERVICE_URI)",
    )

    parser.add_argument(
        "--log-level",
        default=os.environ.get("FUNDERMAPS_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level (env: FUNDERMAPS_LOG_LEVEL)",
    )

    parser.add_argument(
        "--tileset",
        nargs="+",
        help="Specific tilesets to process, comma-separated",
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("FUNDERMAPS_MAX_WORKERS", "3")),
        help="Maximum number of worker threads when using concurrent mode",
    )

    args = parser.parse_args()

    return args


async def process_concurrent(
    fundermaps: FunderMapsSDK,
    tilebundles: List[TileBundle],
    logger: logging.Logger,
    max_workers: int,
) -> List[TileBundle]:
    logger.info(
        f"Processing {len(tilebundles)} tilesets concurrently with {max_workers} workers"
    )

    semaphore = asyncio.Semaphore(max_workers)

    async def bounded_process(tileset):
        async with semaphore:
            return await process_mapset(fundermaps, tileset, logger)

    random.shuffle(tilebundles)

    tasks = [bounded_process(tileset) for tileset in tilebundles]
    await asyncio.gather(*tasks)

    return tilebundles


async def main() -> int:
    args = parse_arguments()

    formatter = colorlog.ColoredFormatter(
        "%(thin_white)s%(asctime)s%(reset)s | "
        "%(bold_blue)s%(name)s%(reset)s | "
        "%(log_color)s%(levelname)-8s%(reset)s | "
        "%(message_log_color)s%(message)s%(reset)s",
        datefmt="%H:%M:%S",
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "bold_green",
            "WARNING": "bold_yellow",
            "ERROR": "bold_red",
            "CRITICAL": "bold_white,bg_red",
        },
        secondary_log_colors={
            "message": {
                "DEBUG": "cyan",
                "INFO": "white",
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red",
            }
        },
        style="%",
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(formatter)

    logger = colorlog.getLogger("process mapset")
    logger.setLevel(getattr(logging, args.log_level))
    logger.handlers = []
    logger.addHandler(handler)
    logger.propagate = False

    db_config = DatabaseConfig(
        database=args.db_name,
        host=args.db_host,
        user=args.db_user,
        password=args.db_password,
        port=args.db_port,
    )

    s3_config = S3Config(
        bucket=args.s3_bucket,
        access_key=args.s3_access_key,
        secret_key=args.s3_secret_key,
        service_uri=args.s3_service_uri,
    )

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config, logger=logger)

    tilebundles = get_default_tilebundles()

    if args.tileset:
        filtered_bundles = []
        requested_tilesets = set(args.tileset)

        for bundle in tilebundles:
            if bundle.tileset in requested_tilesets:
                filtered_bundles.append(bundle)

        if not filtered_bundles:
            logger.warning(
                "None of the specified tilesets were found. Available tilesets:"
            )
            for bundle in tilebundles:
                logger.warning(f"  - {bundle.tileset}")
            return 1

        tilebundles = filtered_bundles
        logger.info(f"Processing {len(tilebundles)} selected tilesets")
    else:
        logger.info(f"Processing all {len(tilebundles)} tilesets")

    start_time = time.time()
    logger.info("Starting Mapset processing...")

    try:
        await process_concurrent(fundermaps, tilebundles, logger, args.max_workers)

        total_elapsed = time.time() - start_time
        logger.info(f"Mapset processing completed successfully in {total_elapsed:.2f}s")
    except Exception as e:
        logger.error(f"Error processing Mapset: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
