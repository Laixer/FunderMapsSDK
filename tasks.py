from prefect import task
from prefect.cache_policies import NO_CACHE

from fundermapssdk import FunderMapsSDK


@task(name="Calculate Risk", cache_policy=NO_CACHE)
def db_calculate_risk(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        db.refresh_materialized_view("data.building_sample")
        db.refresh_materialized_view("data.cluster_sample")
        db.refresh_materialized_view("data.supercluster_sample")

        db.call("data.model_risk_manifest")
        db.reindex_table("data.model_risk_static")


@task(name="Clean Database", cache_policy=NO_CACHE)
def db_public_clean(fundermaps: FunderMapsSDK):
    with fundermaps.db as db:
        db.drop_table("public.woonplaats")
        db.drop_table("public.verblijfsobject")
        db.drop_table("public.pand")
        db.drop_table("public.ligplaats")
        db.drop_table("public.standplaats")
        db.drop_table("public.openbare_ruimte")
        db.drop_table("public.nummeraanduiding")


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
