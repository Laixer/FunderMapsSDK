from prefect import flow


SOURCE_REPO = "git@github.com:Laixer/FunderMapsSDK.git"
WORK_POOL_NAME = "fm-worker-1"

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="export_product.py:export_product",
    ).deploy(
        name="Export Product",
        parameters={},
        work_pool_name=WORK_POOL_NAME,
        cron="55 * * * *",
        print_next_steps=False,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="refresh_models.py:refresh",
    ).deploy(
        name="Database Models Update",
        parameters={},
        work_pool_name=WORK_POOL_NAME,
        cron="05 20 * * *",
        print_next_steps=False,
        concurrency_limit=1,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="load_dataset.py:load_dataset",
    ).deploy(
        name="Load Dataset",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
    )
