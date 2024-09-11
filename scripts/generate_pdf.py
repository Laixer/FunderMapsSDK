import logging
import argparse
import colorlog
from systemd import journal
from configparser import ConfigParser
from fundermapssdk import util, app

logger = logging.getLogger("generate_pdf")


class GeneratePdfTask(app.FunderMapsTask):
    async def run(self):
        self.logger.info("Running PDF conversion")

        building_id = "NL.IMBAG.PAND.0513100011120181"
        source_url = f"https://whale-app-nm9uv.ondigitalocean.app/{building_id}"
        destination_file = f"{building_id}.pdf"

        result = await self.fundermaps.pdf.generate_pdf(source_url, destination_file)
        await util.http_download_file(result["url"], destination_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FunderMaps SDK Script Runner")

    parser.add_argument("-c", "--config", help="path to the configuration file")
    parser.add_argument("-l", "--log-level", help="log level", default="INFO")
    parser.add_argument("--systemd", help="log to systemd", action="store_true")

    args = parser.parse_args()

    if args.systemd:
        handler = journal.JournalHandler(SYSLOG_IDENTIFIER=args.script)
    else:
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(levelname)-8s %(name)s%(reset)s %(message)s"
            )
        )

    logging.basicConfig(level=args.log_level, handlers=[handler], format="%(message)s")

    if args.config:
        config = ConfigParser()
        config.read(args.config)
    else:
        config = util.find_config()

    GeneratePdfTask("generate_pdf", config, logger).asyncio_invoke()
