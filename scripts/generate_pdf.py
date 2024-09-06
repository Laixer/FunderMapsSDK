import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import util, app

logger = logging.getLogger("generate_pdf")


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Running PDF conversion")

    building_id = "NL.IMBAG.PAND.0513100011120181"
    source_url = f"https://whale-app-nm9uv.ondigitalocean.app/{building_id}"
    destination_file = f"{building_id}.pdf"

    result = await fundermaps.pdf.generate_pdf(source_url, destination_file)
    await util.http_download_file(result["url"], destination_file)
