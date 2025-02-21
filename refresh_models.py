from prefect import flow

from fundermapssdk import FunderMapsSDK
from fundermapssdk.config import DatabaseConfig, S3Config

from tasks import *


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
