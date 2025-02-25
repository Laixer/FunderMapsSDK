from prefect import flow
from prefect.logging import get_run_logger

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config


@flow
async def load_dataset(
    dataset_input: str, dataset_layer: list[str] = [], delete_dataset: bool = False
):
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

    dataset_path = dataset_input

    # TODO: This is a task
    if dataset_input.startswith("https://"):
        file_name = dataset_input.split("/")[-1]
        dataset_path = file_name

        logger.info(f"Downloading dataset from URL '{dataset_input}'")
        await util.http_download_file(dataset_input, file_name)

    # TODO: This is a task
    elif dataset_input.startswith("s3://"):
        file_name = dataset_input.split("/")[-1]
        dataset_path = file_name
        s3_path = dataset_input.replace(f"s3://", "")

        logger.info(f"Downloading dataset from S3 '{dataset_input}'")
        with fundermaps.s3 as s3:
            s3.download_file(file_name, s3_path)

    logger.info(f"Validating dataset: {dataset_path}")
    util.validate_file_size(dataset_path, util.FILE_MIN_SIZE)
    util.validate_file_extension(dataset_path, util.FILE_ALLOWED_EXTENSIONS)

    logger.info("Loading dataset into database")
    await fundermaps.gdal.to_postgis(dataset_path, *dataset_layer)

    if delete_dataset and dataset_input.startswith("s3://"):
        logger.info(f"Deleting dataset from S3 '{dataset_input}'")
        # TODO: Delete the file from S3
        pass


# if __name__ == "__main__":
#     import asyncio

#     FILE_NAME: str = "s3://import/gouda.gpkg"
#     asyncio.run(load_dataset(FILE_NAME, ["buildings"], True))
