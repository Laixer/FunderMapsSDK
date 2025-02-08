import csv
import asyncio
from datetime import datetime

from prefect import flow, task
from prefect.logging import get_run_logger

from fundermapssdk import FunderMapsSDK, util
from fundermapssdk.config import DatabaseConfig, S3Config

# TODO: Get from the database
BUCKET: str = "fundermaps"
ORGANIZATIONS: list[str] = [
    "5c2c5822-6996-4306-96ba-6635ea7f90e2",
    "8a56e920-7811-47b7-9289-758c8fe346db",
    "c06a1fc6-6452-4b88-85fd-ba50016c578f",
    "58872000-cb69-433a-91ba-165a9d0b4710",
    "0ca4d02d-8206-4453-ba45-84f532c868f3",
    "3f48a4f9-0277-4c68-8be8-55df0fa2645c",
]


@task(name="Process Export", description="Process product tracker data export")
async def process_export(fundermaps: FunderMapsSDK, organization: str):
    logger = get_run_logger()
    logger.info("Exporting product tracker data")

    with fundermaps.db as db:
        with db.db.cursor() as cur:
            query = """
                SELECT
                        pt.organization_id,
                        pt.product,
                        pt.building_id,
                        b.external_id,
                        pt.create_date,
                        pt.identifier AS request
                FROM    application.product_tracker AS pt
                JOIN    geocoder.building AS b ON b.id = pt.building_id
                WHERE   pt.organization_id = %s
                AND     pt.create_date >= date_trunc('month', CURRENT_DATE) - interval '1 month'
                AND     pt.create_date < date_trunc('month', CURRENT_DATE)"""

            cur.execute(query, (organization,))

            csv_file = f"{organization}.csv"

            # TODO: Maybe create a CSV writer helper function in the SDK
            column_names = [desc[0] for desc in cur.description]

            logger.info(f"Writing data to {csv_file}")
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)

                writer.writerow(column_names)

                data_written = False
                for row in cur:
                    writer.writerow(row)
                    data_written = True

    util.compress_file(csv_file, f"{csv_file}.gz")

    if data_written:
        with fundermaps.s3 as s3:
            current_date = datetime.now()
            formatted_date_year = current_date.strftime("%Y")
            formatted_date_month = current_date.strftime("%b").lower()

            logger.info(f"Uploading {csv_file} to S3")
            s3_path = f"product/{formatted_date_year}/{formatted_date_month}/{organization}.csv.gz"
            await s3.upload_file2(f"{csv_file}.gz", s3_path)
    else:
        logger.info("No data to export")


@flow(name="Export Product")
async def export_product():
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

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

    # TODO: Fetch the organization IDs from the database
    for organization in ORGANIZATIONS:
        await process_export(fundermaps, organization)


if __name__ == "__main__":
    asyncio.run(export_product())
