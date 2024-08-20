import logging

from fundermapssdk import FunderMapsSDK
from fundermapssdk import app

logger = logging.getLogger("user_org")


async def add_user_to_org(
    fundermaps: FunderMapsSDK,
    first_name: str,
    last_name: str,
    email: str,
    organization_id: str,
):
    with fundermaps.db as db:
        with db.db.cursor() as cur:
            query = "CALL application.create_org_user(%s, %s, %s, %s)"

            cur.execute(
                query,
                (
                    first_name,
                    last_name,
                    email,
                    organization_id,
                ),
            )


async def attach_map_to_org(
    fundermaps: FunderMapsSDK,
    map_name: str,
    organization_id: str,
):
    with fundermaps.db as db:
        with db.db.cursor() as cur:
            query = "CALL application.attach_map_to_org(%s, %s)"

            cur.execute(
                query,
                (
                    map_name,
                    organization_id,
                ),
            )


@app.fundermaps_task
async def run(fundermaps: FunderMapsSDK):
    logger.info("Adding user to organization")
    await add_user_to_org(
        fundermaps,
        "Name",
        "Lastname",
        "email",
        "org_id",
    )
