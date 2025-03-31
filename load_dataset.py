import asyncio
import argparse
import os
import tempfile
from typing import List, Optional

from fundermapssdk import util
from fundermapssdk.cli import FunderMapsCommand


class LoadDatasetCommand(FunderMapsCommand):
    """Command to load dataset into the database."""

    def __init__(self):
        super().__init__(description="Load dataset into the database")

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument(
            "dataset_input",
            type=str,
            help="Path, URL, or S3 URL of the dataset to load (e.g., file.geojson, https://example.com/file.zip, s3://bucket/path/file.gpkg)",
        )
        parser.add_argument(
            "--layer",
            nargs="+",
            default=[],
            help="Layer names to load from the dataset",
        )
        parser.add_argument(
            "--delete-after",
            action="store_true",
            help="Delete the dataset after loading",
        )
        parser.add_argument(
            "--tmp-dir",
            type=str,
            help="Temporary directory for downloaded files (default: system temp dir)",
        )

    async def _load_dataset(
        self,
        dataset_input: str,
        dataset_layer: List[str] = [],
        delete_dataset: bool = False,
        tmp_dir: Optional[str] = None,
    ) -> bool:
        """Load a dataset into the database.

        Args:
            dataset_input: Path, URL, or S3 URL of the dataset
            dataset_layer: Layer names to load
            delete_dataset: Whether to delete the dataset after loading
            tmp_dir: Temporary directory for downloaded files

        Returns:
            bool: True if successful, False otherwise
        """
        use_temp_dir = tmp_dir is None
        local_file_path = None
        success = False

        try:
            # Create temp dir if needed
            if use_temp_dir:
                temp_dir = tempfile.mkdtemp(prefix="fundermaps_load_")
                tmp_dir = temp_dir
            else:
                os.makedirs(tmp_dir, exist_ok=True)

            # Get the filename from the path
            file_name = os.path.basename(dataset_input)
            local_file_path = os.path.join(tmp_dir, file_name)

            # Handle different input sources
            if dataset_input.startswith("https://") or dataset_input.startswith(
                "http://"
            ):
                self.logger.info(f"Downloading dataset from URL '{dataset_input}'")
                try:
                    await util.http_download_file(dataset_input, local_file_path)
                except Exception as e:
                    self.logger.error(
                        f"Failed to download file from URL: {e}", exc_info=True
                    )
                    return False

            elif dataset_input.startswith("s3://"):
                self.logger.info(f"Downloading dataset from S3 '{dataset_input}'")
                try:
                    s3_path = dataset_input.replace("s3://", "")
                    with self.fundermaps.s3 as s3:
                        s3.download_file(local_file_path, s3_path)
                except Exception as e:
                    self.logger.error(
                        f"Failed to download file from S3: {e}", exc_info=True
                    )
                    return False

            else:
                # Local file - verify it exists
                if not os.path.exists(dataset_input):
                    self.logger.error(f"Local file not found: {dataset_input}")
                    return False
                local_file_path = dataset_input

            # Validate the file
            try:
                self.logger.info(f"Validating dataset: {local_file_path}")
                util.validate_file_size(local_file_path, util.FILE_MIN_SIZE)
                util.validate_file_extension(
                    local_file_path, util.FILE_ALLOWED_EXTENSIONS
                )
            except Exception as e:
                self.logger.error(f"File validation failed: {e}")
                return False

            # Load the dataset into PostGIS
            self.logger.info(f"Loading dataset into database from {local_file_path}")
            try:
                await self.fundermaps.gdal.to_postgis(local_file_path, *dataset_layer)
                success = True
            except Exception as e:
                self.logger.error(
                    f"Failed to load dataset into database: {e}", exc_info=True
                )
                return False

            # Delete S3 file if requested
            if success and delete_dataset and dataset_input.startswith("s3://"):
                try:
                    self.logger.info(f"Deleting dataset from S3 '{dataset_input}'")
                    s3_path = dataset_input.replace("s3://", "")
                    with self.fundermaps.s3 as s3:
                        s3.delete_file(s3_path)
                except Exception as e:
                    self.logger.warning(f"Failed to delete S3 file: {e}")
                    # Not critical if deletion fails

            return success

        except Exception as e:
            self.logger.error(f"Error processing dataset: {e}", exc_info=True)
            return False
        finally:
            # Clean up temp files
            if (
                success
                and delete_dataset
                and local_file_path
                and local_file_path != dataset_input
            ):
                try:
                    if os.path.exists(local_file_path):
                        os.remove(local_file_path)
                except Exception as e:
                    self.logger.warning(f"Failed to delete temporary file: {e}")

            # Clean up temp dir if we created it
            if use_temp_dir and tmp_dir:
                try:
                    os.rmdir(tmp_dir)
                except:
                    pass

    async def execute(self) -> int:
        """Execute the load dataset command."""
        try:
            success = await self._load_dataset(
                dataset_input=self.args.dataset_input,
                dataset_layer=self.args.layer,
                delete_dataset=self.args.delete_after,
                tmp_dir=self.args.tmp_dir,
            )

            if success:
                self.logger.info("Dataset loaded successfully")
                return 0
            else:
                self.logger.error("Failed to load dataset")
                return 1

        except Exception as e:
            self.logger.error(
                f"An error occurred during dataset loading: {e}", exc_info=True
            )
            return 1


if __name__ == "__main__":
    exit_code = asyncio.run(LoadDatasetCommand().run())
    exit(exit_code)
