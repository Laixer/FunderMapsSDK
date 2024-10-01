import logging

from fundermapssdk import FunderMapsSDK, app

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
            query = "CALL application.create_org_user(NULLIF(%s, ''), NULLIF(%s, ''), %s, %s)"

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
async def run(fundermaps: FunderMapsSDK, args):
    org_id = "c96e7b54-63ce-4017-82a1-dcdec054bfd1"

    map_set = [
        "Fundering",
        "Pand",
        "Rapportage",
        "Risico",
        "FunderScan",
        "Incidenten",
    ]

    # logger.info("Attaching maps to organization")
    # for map_name in map_set:
    #     await attach_map_to_org(fundermaps, map_name, org_id)

    logger.info("Adding user to organization")
    await add_user_to_org(
        fundermaps,
        "",
        "",
        "leeuwen.e@woerden.nl",
        org_id,
    )
