import os
import asyncio
import tempfile
import argparse
import time
import random
from typing import List
from dataclasses import dataclass, field

from fundermapssdk import util
from fundermapssdk.cli import FunderMapsCommand
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


class ProcessMapsetCommand(FunderMapsCommand):
    """Command to refresh models in the database."""

    def __init__(self):
        super().__init__(description="Process Mapset tilesets")

    async def _download_dataset(
        self,
        context: JobContext,
    ) -> bool:
        self.logger.info(f"Downloading '{context.tileset.tileset}' from PostGIS")

        output_file = os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg")

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                maplayer = context.tileset.table_name()
                await self.fundermaps.gdal.from_postgis(output_file, maplayer)
                return True
            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait_time = RETRY_DELAY * attempt
                    self.logger.warning(
                        f"Download attempt {attempt} failed for {context.tileset.tileset}. Retrying in {wait_time}s. Error: {e}"
                    )
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error(
                        f"Failed to download {context.tileset.tileset} after {MAX_RETRIES} attempts: {e}"
                    )
                    # TODO: Write the error to the context
                    context.tileset.errors.append(f"Download failed: {str(e)}")
                    return False

    async def _generate_tileset(
        self,
        context: JobContext,
    ) -> bool:
        try:
            self.logger.info(
                f"Converting tileset '{context.tileset.tileset}' to GeoJSON"
            )
            await self.fundermaps.gdal.ogr2ogr(
                os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg"),
                os.path.join(context.work_dir, f"{context.tileset.tileset}.geojson"),
            )

            self.logger.info(f"Generating tileset '{context.tileset.tileset}'")
            await tippecanoe(
                os.path.join(context.work_dir, f"{context.tileset.tileset}.geojson"),
                os.path.join(context.work_dir, context.tileset.tileset),
                context.tileset.tileset,
                context.tileset.max_zoom,
                context.tileset.min_zoom,
            )
            return True
        except Exception as e:
            self.logger.error(
                f"Failed to generate tileset for {context.tileset.tileset}: {e}"
            )
            context.tileset.errors.append(f"Tileset generation failed: {str(e)}")
            return False

    def _upload_dataset(
        self,
        context: JobContext,
    ) -> bool:
        try:
            self.logger.info(f"Uploading {context.tileset.tileset} to S3")

            with self.fundermaps.s3 as s3:
                s3_path = f"mapset/{util.date_path()}/{context.tileset.tileset}.gpkg"
                s3.upload_file(
                    os.path.join(context.work_dir, f"{context.tileset.tileset}.gpkg"),
                    s3_path,
                    bucket="fundermaps-data",
                )
            return True
        except Exception as e:
            self.logger.error(
                f"Failed to upload dataset for {context.tileset.tileset}: {e}"
            )
            context.tileset.errors.append(f"Dataset upload failed: {str(e)}")
            return False

    async def _upload_tiles(self, tileset: TileBundle, tileset_dir: str) -> bool:
        """Upload generated tiles to S3."""
        try:
            self.logger.info(f"Uploading tiles for {tileset.tileset} to S3")

            non_tile_files = util.collect_files_with_extension(tileset_dir, ".json")
            if non_tile_files:
                self.logger.info(
                    f"Removing {len(non_tile_files)} non-tile files from directory"
                )
                for file_path in non_tile_files:
                    try:
                        os.remove(file_path)
                        self.logger.debug(f"Removed file: {file_path}")
                    except OSError as e:
                        self.logger.warning(f"Failed to remove file {file_path}: {e}")

            with self.fundermaps.s3 as s3:
                tile_headers = {
                    "CacheControl": TILE_CACHE,
                    "ContentType": "application/x-protobuf",
                    "ACL": "public-read",
                }

                s3.upload_directory(
                    tileset_dir,
                    tileset.tileset,
                    bucket="fundermaps-tileset",
                    extra_args=tile_headers,
                )
            return True
        except Exception as e:
            self.logger.error(f"Failed to upload tiles for {tileset.tileset}: {e}")
            tileset.errors.append(f"Tile upload failed: {str(e)}")
            return False

    async def _process_mapset(self, tileset: TileBundle) -> bool:
        start_time = time.time()
        success = True

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            ctx = JobContext(tileset, tmp_dir)

            self.logger.info(f"Starting processing for {tileset.tileset}")

            if not await self._download_dataset(ctx):
                success = False
                tileset.processing_time = time.time() - start_time
                return success

            if tileset.upload_dataset:
                if not self._upload_dataset(ctx):
                    success = False

            if tileset.generate_tiles and success:
                if not await self._generate_tileset(ctx):
                    success = False
                else:
                    success = await self._upload_tiles(
                        tileset, os.path.join(tmp_dir, tileset.tileset)
                    )

        tileset.processing_time = time.time() - start_time
        if success:
            self.logger.info(
                f"Successfully processed {tileset.tileset} in {tileset.processing_time:.2f}s"
            )
        else:
            self.logger.error(
                f"Failed to process {tileset.tileset} after {tileset.processing_time:.2f}s"
            )

        return success

    async def _process_concurrent(
        self, tilebundles: List[TileBundle]
    ) -> List[TileBundle]:
        max_workers = self.args.max_workers
        self.logger.info(
            f"Processing {len(tilebundles)} tilesets concurrently with {max_workers} workers"
        )

        semaphore = asyncio.Semaphore(max_workers)

        async def bounded_process(tileset):
            async with semaphore:
                return await self._process_mapset(tileset)

        random.shuffle(tilebundles)

        tasks = [bounded_process(tileset) for tileset in tilebundles]
        await asyncio.gather(*tasks)

        return tilebundles

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument(
            "--tileset",
            nargs="+",
            help="Specific tilesets to process, comma-separated",
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=3,
            help="Maximum number of worker threads when using concurrent mode",
        )

    async def execute(self):
        """Execute the process mapset command."""
        try:
            tilebundles = []

            with self.fundermaps.db as db:
                if self.args.tileset:
                    requested_tilesets = set(self.args.tileset)
                    self.logger.info(
                        f"Fetching specific tilesets from database: {', '.join(requested_tilesets)}"
                    )

                    # Build a parameterized query to fetch only the requested tilesets
                    placeholders = ", ".join(["%s"] * len(requested_tilesets))
                    query = f"""
                        SELECT
                            tileset, 
                            zoom_min_level, 
                            zoom_max_level, 
                            map_enabled,
                            upload_dataset
                        FROM maplayer.bundle
                        WHERE enabled = TRUE AND tileset IN ({placeholders})
                    """

                    with db.db.cursor() as cur:
                        cur.execute(query, list(requested_tilesets))

                        for row in cur.fetchall():
                            (
                                tileset,
                                zoom_min_level,
                                zoom_max_level,
                                map_enabled,
                                upload_dataset,
                            ) = row
                            tilebundles.append(
                                TileBundle(
                                    tileset=tileset,
                                    min_zoom=zoom_min_level,
                                    max_zoom=zoom_max_level,
                                    upload_dataset=upload_dataset,
                                    generate_tiles=map_enabled,
                                )
                            )

                        if not tilebundles:
                            self.logger.warning(
                                "None of the specified tilesets were found. Fetching available tilesets:"
                            )
                            # Fetch all available tilesets to show as options
                            cur.execute(
                                """
                                SELECT tileset 
                                FROM maplayer.bundle 
                                WHERE enabled = TRUE
                            """
                            )
                            available_tilesets = [row[0] for row in cur.fetchall()]
                            for tileset in available_tilesets:
                                self.logger.warning(f"  - {tileset}")
                            return 1

                    self.logger.info(f"Processing {len(tilebundles)} selected tilesets")
                else:
                    self.logger.info("Fetching all tilesets from database")
                    with db.db.cursor() as cur:
                        query = """
                            SELECT
                                tileset, 
                                zoom_min_level, 
                                zoom_max_level, 
                                map_enabled,
                                upload_dataset
                            FROM maplayer.bundle
                            WHERE enabled = TRUE
                        """
                        cur.execute(query)

                        for row in cur.fetchall():
                            (
                                tileset,
                                zoom_min_level,
                                zoom_max_level,
                                map_enabled,
                                upload_dataset,
                            ) = row
                            tilebundles.append(
                                TileBundle(
                                    tileset=tileset,
                                    min_zoom=zoom_min_level,
                                    max_zoom=zoom_max_level,
                                    upload_dataset=upload_dataset,
                                    generate_tiles=map_enabled,
                                )
                            )
                    self.logger.info(f"Processing all {len(tilebundles)} tilesets")

            results = await self._process_concurrent(tilebundles)

            # Report summary
            success_count = sum(1 for tb in results if not tb.errors)
            failure_count = len(results) - success_count

            self.logger.info(
                f"Processing complete: {success_count} succeeded, {failure_count} failed"
            )
            if failure_count > 0:
                self.logger.warning("Failed tilesets:")
                for tb in results:
                    if tb.errors:
                        self.logger.warning(f"  - {tb.tileset}: {'; '.join(tb.errors)}")
                return 1
            return 0
        except Exception as e:
            self.logger.error(f"An error occurred during mapset processing: {e}")
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(ProcessMapsetCommand().run())
    exit(exit_code)
