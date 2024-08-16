import csv
import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import App, fundermaps_task, fundermaps_task_post
from fundermapssdk.util import find_config, http_download_file


ORGANIZATION: str = "5c2c5822-6996-4306-96ba-6635ea7f90e2"

logger = logging.getLogger("product_export")


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
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

            cur.execute(query, (ORGANIZATION,))

            csv_file = f"product_tracker_{ORGANIZATION}.csv"

            logger.info(f"Writing data to {csv_file}")
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)

                writer.writerow([desc[0] for desc in cur.description])
                writer.writerows(cur)
