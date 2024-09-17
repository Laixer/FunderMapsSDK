import os
import logging
import shutil
import boto3
import asyncio
from concurrent.futures import ThreadPoolExecutor

from fundermapssdk import FunderMapsSDK
from fundermapssdk.tippecanoe import tippecanoe
from fundermapssdk import util, app

logger = logging.getLogger("inquiry_file")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    with fundermaps.s3 as s3:
        with fundermaps.db as db:
            with db.db.cursor() as cur:
                query = """
                    SELECT id, document_name, document_file
                    FROM report.inquiry i
                    where i.id not in (select id from public.inquiry_missing_file)"""

                cur.execute(query)

                # data_written = False
                for row in cur:
                    print(row)

                    # def file_exists(bucket_name, file_key):
                    """Checks if a file exists in an S3 bucket.

                    Args:
                        bucket_name: The name of the S3 bucket.
                        file_key: The key (path) of the file in the bucket.

                    Returns:
                        True if the file exists, False otherwise.
                    """

                    # s3 = boto3.client('s3')
                    # try:
                    s3.client.head_object(
                        Bucket="fundermaps", Key=f"inquiry-report/{row[2]}"
                    )
                    logger.info(f"File {row[2]} exists")
                    # return True
                    # except Exception as e:
                    #     logger.error(f"File {row[2]} does not exist", exc_info=e)

                    # await asyncio.sleep(2)
                    # if e.response["Error"]["Code"] == "404":
                    #     # return False
                    # else:
                    #     # Something else has gone wrong.
                    #     raise e

                    # Example usage
                    # bucket_name = 'your-bucket-name'
                    # file_key = 'path/to/your/file.txt'

                    # if file_exists(bucket_name, file_key):
                    # print("File exists!")
                    # else:
                    # print("File does not exist.")

                    # s3.client.upload_file(
                    #     local_path,
                    #     BUCKET,
                    #     local_path,
                    #     ExtraArgs={
                    #         "CacheControl": f"max-age={TILE_CACHE_MAX_AGE}",
                    #         "ContentType": "application/x-protobuf",
                    #         "ContentEncoding": "gzip",
                    #         "ACL": "public-read",
                    #     },
                    # )
