import logging

from fundermapssdk import FunderMapsSDK, util, app

logger = logging.getLogger("inquiry_file")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    with fundermaps.s3 as s3:
        with fundermaps.db as db:
            with db.db.cursor() as cur:
                query = """
                    SELECT id, document_name, document_file
                    FROM report.inquiry i
                    where i.id not in (select id from public.inquiry_missing_file)"""

                cur.execute(query)

                for row in cur:
                    try:
                        s3.client.head_object(
                            Bucket="fundermaps", Key=f"inquiry-report/{row[2]}"
                        )
                        logger.info(f"File {row[2]} exists")
                    except Exception as e:
                        logger.error(f"File {row[2]} does not exist", exc_info=e)
