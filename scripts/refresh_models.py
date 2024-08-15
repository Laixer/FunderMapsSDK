import sys
import asyncio
import logging
import colorlog

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config
from fundermapssdk.util import find_config


logger = logging.getLogger("refresh_models")

task_registry = {}
task_registry_post = {}


def fundermaps_task(func):
    """Decorator to register a function."""
    task_registry[func.__name__] = func
    return func


def fundermaps_task_post(func):
    """Decorator to register a function."""
    task_registry_post[func.__name__] = func
    return func


@fundermaps_task
async def run(fundermaps: FunderMapsSDK):
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


if __name__ == "__main__":
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
        db_config = DatabaseConfig(
            database=config.get("database", "database"),
            host=config.get("database", "host"),
            user=config.get("database", "username"),
            password=config.get("database", "password"),
            port=config.getint("database", "port"),
        )
        s3_config = S3Config(
            access_key=config.get("s3", "access_key"),
            secret_key=config.get("s3", "secret_key"),
            service_uri=config.get("s3", "service_uri"),
            bucket=config.get("s3", "bucket"),
        )
        fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

        async def run_tasks():
            try:
                for task_name, task_func in task_registry.items():
                    logger.debug(f"Running task '{task_name}'")
                    await task_func(fundermaps)
            finally:
                for task_name, task_func in task_registry_post.items():
                    logger.debug(f"Running post task '{task_name}'")
                    await task_func(fundermaps)

        logger.info("Starting 'refresh_models'")
        asyncio.run(run_tasks())
        logger.info("Finished 'refresh_models'")

    except Exception as e:
        logger.error("An error occurred", exc_info=e)
        sys.exit(1)
