import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import app


FILE_NAME: str = "example.txt"
FILE_MIN_SIZE: int = 1024

logger = logging.getLogger("test_task")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    if len(args) < 1:
        logger.error("Missing URL argument")
        return

    url = args[0]

    logger.info("Downloading test file")
    await fundermaps.file.http_download(url, FILE_NAME, FILE_MIN_SIZE)
