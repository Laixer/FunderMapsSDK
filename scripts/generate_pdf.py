import logging

from fundermapssdk import FunderMapsSDK, util, app

SCRIPT_NAME = "generate_pdf"


logger = logging.getLogger("generate_pdf")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK, args):
    logger.info("Running PDF conversion")

    building_id = args[0] or "NL.IMBAG.PAND.0513100011120181"
    source_url = f"https://whale-app-nm9uv.ondigitalocean.app/{building_id}"
    destination_file = f"{building_id}.pdf"

    # result = await fundermaps.pdf.generate_pdf(source_url, destination_file)
    # await util.http_download_file(result["url"], destination_file)
    logger.info("PDF generated")
    logger.info(f"Downloading to {destination_file}")
    logger.info(f"Downloaded to {destination_file}")
    logger.info("Done")
    logger.info(args)
