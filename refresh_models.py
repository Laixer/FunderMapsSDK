import sys
import asyncio
import logging
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig
from fundermapssdk.util import find_config


logger = logging.getLogger("refresh_models")


async def run(config):
    db_config = DatabaseConfig(
        database=config.get("database", "database"),
        host=config.get("database", "host"),
        user=config.get("database", "username"),
        password=config.get("database", "password"),
        port=config.getint("database", "port"),
    )

    fundermaps = FunderMapsSDK(db_config=db_config)

    with fundermaps.db as db:
        # TODO: Check if enough data has changed to refresh models

        logger.info("Refreshing building models")
        db.refresh_materialized_view("data.building_sample")
        db.refresh_materialized_view("data.cluster_sample")
        db.refresh_materialized_view("data.supercluster_sample")

        logger.info("Refreshing risk models")
        db.call("data.model_risk_manifest")
        db.reindex_table("data.model_risk_static")

        logger.info("Refreshing statistics")
        db.refresh_materialized_view("data.statistics_product_inquiries")
        db.refresh_materialized_view("data.statistics_product_inquiry_municipality")
        db.refresh_materialized_view("data.statistics_product_incidents")
        db.refresh_materialized_view("data.statistics_product_incident_municipality")
        db.refresh_materialized_view("data.statistics_product_foundation_type")
        db.refresh_materialized_view("data.statistics_product_foundation_risk")
        db.refresh_materialized_view("data.statistics_product_data_collected")
        db.refresh_materialized_view("data.statistics_product_construction_years")
        db.refresh_materialized_view("data.statistics_product_buildings_restored")
        db.refresh_materialized_view("data.statistics_postal_code_foundation_type")
        db.refresh_materialized_view("data.statistics_postal_code_foundation_risk")


def main():
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
    )

    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler],
    )

    config = find_config()

    try:
        logger.info("Starting 'refresh_models'")
        asyncio.run(run(config))
        logger.info("Finished 'refresh_models'")
    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)


if __name__ == "__main__":
    main()
