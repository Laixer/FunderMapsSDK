from prefect import flow


SOURCE_REPO = "git@github.com:Laixer/FunderMapsSDK.git"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="export_product.py:export_product",
    ).deploy(
        name="Export Product",
        parameters={},
        work_pool_name="fm-worker-1",
        cron="0 * * * *",  # Run every hour
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="refresh_models.py:refresh",
    ).deploy(
        name="Database Models Update",
        parameters={},
        work_pool_name="fm-worker-1",
        cron="05 23 * * *",
    )
