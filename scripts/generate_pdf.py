import logging

from fundermapssdk import FunderMapsSDK, app

BUCKET: str = "fundermaps"
BASE_URL: str = "https://whale-app-nm9uv.ondigitalocean.app"

logger = logging.getLogger("generate_pdf")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    if len(args) < 1:
        logger.error("Missing building ID argument")
        return

    building_id = args[0]
    source_url = f"{BASE_URL}/{building_id}"
    destination_file = f"{building_id}.pdf"

    logger.info("Running PDF conversion")
    result = await fundermaps.pdf.generate_pdf(source_url, destination_file)
    await fundermaps.file.http_download(result["url"], destination_file, min_size=1024)

    with fundermaps.s3 as s3:
        logger.info(f"Uploading {destination_file} to S3")
        s3_path = f"report/{destination_file}"
        await s3.upload_file(BUCKET, destination_file, s3_path)
