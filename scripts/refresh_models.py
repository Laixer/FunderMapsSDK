import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk.app import App, fundermaps_task
from fundermapssdk.util import find_config

logger = logging.getLogger("refresh_models")


@fundermaps_task
async def run_refresh_models(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        # TODO: Check if enough data has changed to refresh models

        logger.info("Refreshing building models")
        db.refresh_materialized_view("data.building_sample")
        db.refresh_materialized_view("data.cluster_sample")
        db.refresh_materialized_view("data.supercluster_sample")

        logger.info("Refreshing risk models")
        db.call("data.model_risk_manifest")
        db.reindex_table("data.model_risk_static")


@fundermaps_task
async def run_refresh_statistics(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
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
