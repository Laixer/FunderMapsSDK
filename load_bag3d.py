import os
import logging
import asyncio

from fundermapssdk import FunderMapsSDK
from fundermapssdk.mail import Email
from fundermapssdk.util import http_download_file
from fundermapssdk.config import MailConfig, DatabaseConfig
from fundermapssdk.gdal import convert

BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"

logger = logging.getLogger("loadbag3d")


async def download_bag_file():
    """
    Download the BAG file from the PDOK service.
    """

    await http_download_file(BASE_URL_BAG, "3dbag_nl.gpkg")


async def main(config):
    mail_config = MailConfig(
        api_key=config.get("mail", "api_key"),
        domain=config.get("mail", "domain"),
        default_sender_name=config.get("mail", "default_sender_name"),
        default_sender_address=config.get("mail", "default_sender_address"),
    )

    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fm = FunderMapsSDK(db_config=db_config, mail_config=mail_config)

    try:
        logger.info("Starting BAG loading process")

        logger.info("Downloading BAG file")
        await download_bag_file()

        logger.info("Checking BAG 3D file")
        if not os.path.exists("3dbag_nl.gpkg"):
            raise FileNotFoundError("BAG 3D file not found")
        if os.path.getsize("3dbag_nl.gpkg") < 1024 * 1024 * 1024:
            raise ValueError("BAG 3D file is below 1GB")

        logger.info("Loading BAG file into database")
        await convert(
            "3dbag_nl.gpkg",
            "PG:dbname=fundermaps host=db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com user=fundermaps password=AVNS_CtcfLEuVWqRXiK__gKt port=25060",
            "lod22_2d",
        )
        await convert(
            "3dbag_nl.gpkg",
            "PG:dbname=fundermaps host=db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com user=fundermaps password=AVNS_CtcfLEuVWqRXiK__gKt port=25060",
            "pand",
        )

        # with fm.db as db:
        #     logger.info("Loading buildings into geocoder")
        #     db.execute_script("load_building")
        #     db.reindex_table("geocoder.building")

        #     logger.info("Loading addressess into geocoder")
        #     db.execute_script("load_address")
        #     db.reindex_table("geocoder.address")

        #     logger.info("Loading residences into geocoder")
        #     db.execute_script("load_residence")
        #     db.reindex_table("geocoder.residence")

        to = config.get("general", "report_to")
        await fm.mail.send_simple_message(
            Email(
                to=to.split(","),
                subject="BAG is loaded",
                text="BAG is loaded",
            )
        )
        logger.info("Email sent")

        logger.info("Finished")
    except Exception as e:

        # TODO: Cleanup db

        to = config.get("general", "report_to")
        await fm.mail.send_simple_message(
            Email(
                to=to.split(","),
                subject="Error loading BAG",
                text="An error occurred while loading the BAG file",
            )
        )

        logger.error("An error occurred", exc_info=e)


if __name__ == "__main__":
    import colorlog
    from configparser import ConfigParser

    config = ConfigParser()

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config.read("config.ini")

    asyncio.run(main(config))
