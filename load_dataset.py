import os
import asyncio
import tempfile


from prefect import flow
from prefect.logging import get_run_logger

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config

# FILE_NAME: str = "/home/eve/gouda.gpkg"
# FILE_NAME: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"
# FILE_NAME: str = "https://service.pdok.nl/cbs/wijkenbuurten/2023/atom/downloads/wijkenbuurten_2023_v1.gpkg"
# FILE_NAME: str = (
#     "https://service.pdok.nl/cbs/wijkenbuurten/2024/atom/downloads/wijkenbuurten_2024.gpkg"
# )
FILE_NAME: str = "s3://import/gouda.gpkg"
FILE_MIN_SIZE: int = 1024 * 512  # 512 KB


@flow(name="Load Dataset")
async def load_dataset():
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

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        os.chdir(tmp_dir)

        dataset_input = FILE_NAME
        dataset_path = dataset_input
        # layer_name = ["buildings"]
        layer_name = []

        if dataset_input.startswith("https://"):
            file_name = dataset_input.split("/")[-1]
            dataset_path = file_name

            logger.info("Downloading dataset")
            await util.http_download_file(dataset_input, file_name)

        elif dataset_input.startswith("s3://"):
            file_name = dataset_input.split("/")[-1]
            dataset_path = file_name
            s3_path = dataset_input.replace(f"s3://", "")

            logger.info("Downloading dataset")
            with fundermaps.s3 as s3:
                s3.download_file(file_name, s3_path)

        logger.info("Validating dataset")
        util.validate_file_size(dataset_path, FILE_MIN_SIZE)

        logger.info("Loading dataset into database")
        await fundermaps.gdal.to_postgis(dataset_path, *layer_name)


if __name__ == "__main__":
    asyncio.run(load_dataset())
