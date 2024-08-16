import csv
import logging
from datetime import datetime

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import fundermaps_task


BUCKET: str = "fundermaps"
ORGANIZATION: list[str] = [
    "5c2c5822-6996-4306-96ba-6635ea7f90e2",
    "8a56e920-7811-47b7-9289-758c8fe346db",
    "c06a1fc6-6452-4b88-85fd-ba50016c578f",
    "58872000-cb69-433a-91ba-165a9d0b4710",
]

logger = logging.getLogger("product_export")


async def process_export(fundermaps: FunderMapsSDK, organization: str):
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

            column_names = [desc[0] for desc in cur.description]

            logger.info(f"Writing data to {csv_file}")
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)

                writer.writerow(column_names)

                data_written = False
                for row in cur:
                    writer.writerow(row)
                    data_written = True

    if data_written:
        with fundermaps.s3 as s3:
            current_date = datetime.now()
            formatted_date_year = current_date.strftime("%Y")
            formatted_date_month = current_date.strftime("%b").lower()

            logger.info(f"Uploading {csv_file} to S3")
            await s3.upload_file(
                BUCKET,
                csv_file,
                f"product/{formatted_date_year}/{formatted_date_month}/{organization}.csv",
            )
    else:
        logger.info("No data to export")


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    for organization in ORGANIZATION:
        await process_export(fundermaps, organization)
