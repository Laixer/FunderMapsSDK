import os
import logging
import asyncio

from fundermapssdk import FunderMapsSDK
from fundermapssdk.mail import Email
from fundermapssdk.util import http_download_file
from fundermapssdk.config import MailConfig, DatabaseConfig
from fundermapssdk.gdal import convert

BASE_URL_BAG: str = "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg"

logger = logging.getLogger("loadbag")


async def run(config):
    # mail_config = MailConfig(
    #     api_key=config.get("mail", "api_key"),
    #     domain=config.get("mail", "domain"),
    #     default_sender_name=config.get("mail", "default_sender_name"),
    #     default_sender_address=config.get("mail", "default_sender_address"),
    # )

    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fm = FunderMapsSDK(db_config=db_config)

    try:
        logger.info("Starting BAG loading process")

        with fm.db as db:
            # TODO: Check if enough data has changed to refresh models

            logger.info("Refreshing models")
            db.refresh_materialized_view("data.building_sample")
            db.refresh_materialized_view("data.cluster_sample")
            db.refresh_materialized_view("data.supercluster_sample")
            db.call("data.model_risk_manifest")
            db.reindex_table("data.model_risk_static")

            logger.info("Refreshing statistics")
            db.refresh_materialized_view("data.statistics_product_inquiries")
            db.refresh_materialized_view("data.statistics_product_inquiry_municipality")
            db.refresh_materialized_view("data.statistics_product_incidents")
            db.refresh_materialized_view(
                "data.statistics_product_incident_municipality"
            )
            db.refresh_materialized_view("data.statistics_product_foundation_type")
            db.refresh_materialized_view("data.statistics_product_foundation_risk")
            db.refresh_materialized_view("data.statistics_product_data_collected")
            db.refresh_materialized_view("data.statistics_product_construction_years")
            db.refresh_materialized_view("data.statistics_product_buildings_restored")
            db.refresh_materialized_view("data.statistics_postal_code_foundation_type")
            db.refresh_materialized_view("data.statistics_postal_code_foundation_risk")

        logger.info("Finished")
    except Exception as e:

        # to = config.get("general", "report_to")
        # await fm.mail.send_simple_message(
        #     Email(
        #         to=to.split(","),
        #         subject="Error loading BAG",
        #         text="An error occurred while loading the BAG file",
        #     )
        # )

        logger.error("An error occurred", exc_info=e)


def main():
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

    asyncio.run(run(config))


if __name__ == "__main__":
    main()
