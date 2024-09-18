import csv
import logging
from datetime import datetime

from fundermapssdk import FunderMapsSDK, util, app


BUCKET: str = "fundermaps"
OUTPUT_FILE_NAME: str = "analysis_full.gpkg"
LAYER_NAME: str = "maplayer.analysis_full"

logger = logging.getLogger("analysis_full")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    file_path = "/home/yorick/Downloads/SAM_2024_03.csv"
    with open(file_path, "r") as file:
        reader = csv.reader(file)

        with fundermaps.db as db:
            # TODO: Run everything in a transaction
            db.truncate_table("public.model_supply")

            with db.db.cursor() as cur:
                for row in reader:
                    if len(row[0]) == 16:
                        # TODO: Use copy
                        insert_query = (
                            f"INSERT INTO model_supply (building_id) VALUES (%s)"
                        )
                        building_id = f"NL.IMBAG.PAND.{row[0]}"
                        cur.execute(insert_query, (building_id,))

            with db.db.cursor() as cur:
                # Load from script
                query = """
                    SELECT *
                    FROM public.model_supply ms
                    JOIN data.model_risk_static mrs ON mrs.external_building_id = ms.building_id"""

                cur.execute(query)

                csv_file = f"model_supply.csv"

                column_names = [desc[0] for desc in cur.description]

                logger.info(f"Writing data to {csv_file}")
                with open(csv_file, mode="w", newline="") as file:
                    writer = csv.writer(file)

                    writer.writerow(column_names)

                    for row in cur:
                        writer.writerow(row)

        with fundermaps.s3 as s3:
            logger.info(f"Uploading {csv_file} to S3")
            s3_path = f"export/{csv_file}"
            await s3.upload_file(BUCKET, csv_file, s3_path)
