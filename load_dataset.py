import asyncio

from fundermapssdk import util
from fundermapssdk.cli import FunderMapsCommand


class LoadDatasetCommand(FunderMapsCommand):
    """Command to load dataset into the database."""

    def __init__(self):
        super().__init__(description="Load dataset into the database")

    async def _load_dataset(
        self,
        dataset_input: str,
        dataset_layer: list[str] = [],
        delete_dataset: bool = False,
    ):
        dataset_path = dataset_input

        if dataset_input.startswith("https://"):
            file_name = dataset_input.split("/")[-1]
            dataset_path = file_name

            self.logger.info(f"Downloading dataset from URL '{dataset_input}'")
            await util.http_download_file(dataset_input, file_name)

        elif dataset_input.startswith("s3://"):
            file_name = dataset_input.split("/")[-1]
            dataset_path = file_name
            s3_path = dataset_input.replace(f"s3://", "")

            self.logger.info(f"Downloading dataset from S3 '{dataset_input}'")
            with self.fundermaps.s3 as s3:
                s3.download_file(file_name, s3_path)

        self.logger.info(f"Validating dataset: {dataset_path}")
        util.validate_file_size(dataset_path, util.FILE_MIN_SIZE)
        util.validate_file_extension(dataset_path, util.FILE_ALLOWED_EXTENSIONS)

        self.logger.info("Loading dataset into database")
        await self.fundermaps.gdal.to_postgis(dataset_path, *dataset_layer)

        if delete_dataset and dataset_input.startswith("s3://"):
            self.logger.info(f"Deleting dataset from S3 '{dataset_input}'")
            # TODO: Delete the file from S3
            pass

    async def execute(self):
        """Execute the load dataset command."""
        dataset_input = "https://example.com/dataset.zip"
        dataset_layer = ["layer_name"]
        delete_dataset = False

        await self._load_dataset(
            dataset_input=dataset_input,
            dataset_layer=dataset_layer,
            delete_dataset=delete_dataset,
        )


if __name__ == "__main__":
    exit_code = asyncio.run(LoadDatasetCommand().run())
    exit(exit_code)
