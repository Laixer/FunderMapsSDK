from prefect import flow

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config

from tasks import *


@task(name="Calculate Risk", cache_policy=NO_CACHE)
def db_calculate_risk(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        db.refresh_materialized_view("data.building_sample")
        db.refresh_materialized_view("data.cluster_sample")
        db.refresh_materialized_view("data.supercluster_sample")

        db.call("data.model_risk_manifest")
        db.reindex_table("data.model_risk_static")


@task(name="Refresh Statistics", cache_policy=NO_CACHE)
def db_refresh_statistics(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
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


@flow(name="Refresh Models")
def refresh_models():
    db_config = DatabaseConfig(
        database="fundermaps",
        # host="db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
        host="private-db-pg-ams3-0-do-user-871803-0.b.db.ondigitalocean.com",
        user="fundermaps",
        password="AVNS_CtcfLEuVWqRXiK__gKt",
        port=25060,
    )

    s3_config = S3Config(
        bucket="fundermaps-development",
        access_key="LOUSAQJLIXLMIXKTKDW5",
        secret_key="/edoJzt5h5hZok6AzuRzWF79EOzLRw3ywH0WzdbGjAU",
        service_uri="https://ams3.digitaloceanspaces.com",
    )

    fundermaps = FunderMapsSDK(db_config=db_config, s3_config=s3_config)

    db_calculate_risk(fundermaps)
    db_refresh_statistics(fundermaps)


if __name__ == "__main__":
    refresh_models()
