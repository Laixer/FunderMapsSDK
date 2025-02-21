from prefect import flow


SOURCE_REPO = "git@github.com:Laixer/FunderMapsSDK.git"
WORK_POOL_NAME = "fm-worker-1"

# TODO: Move into /scripts folder

if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="export_product.py:export_product",
    ).deploy(
        name="export-product",
        work_pool_name=WORK_POOL_NAME,
        cron="0 0 1 * *",  # TODO: Check exactly when to run this flow
        print_next_steps=False,
        ignore_warnings=True,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="refresh_models.py:refresh_models",
    ).deploy(
        name="calculate-db-models-stats",
        description="Update database, calculate models and statistics",
        work_pool_name=WORK_POOL_NAME,
        cron="05 20 * * *",
        print_next_steps=False,
        concurrency_limit=1,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="load_dataset.py:load_dataset",
    ).deploy(
        name="load-dataset",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="load_dataset.py:load_dataset",
    ).deploy(
        name="load-bag",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
        parameters={
            "dataset_input": "https://service.pdok.nl/lv/bag/atom/downloads/bag-light.gpkg",
        },
    )

    # TODO: FILE_MIN_SIZE: int = 1024 * 1024 * 1024
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="load_dataset.py:load_dataset",
    ).deploy(
        name="load-bag3d",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
        parameters={
            "dataset_input": "https://data.3dbag.nl/v20241216/3dbag_nl.gpkg.zip",
            "dataset_layer": ["lod22_2d", "pand"],
        },
    )

    # TODO: FILE_MIN_SIZE: int = 1024 * 1024
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="load_dataset.py:load_dataset",
    ).deploy(
        name="load-cbs",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
        parameters={
            "dataset_input": "https://service.pdok.nl/cbs/wijkenbuurten/2024/atom/downloads/wijkenbuurten_2024.gpkg",
        },
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="process_mapset.py:extract_mapset",
    ).deploy(
        name="extract-mapset",
        work_pool_name=WORK_POOL_NAME,
        print_next_steps=False,
        parameters={
            "tilebundle": [
                {
                    "tileset": "analysis_foundation",
                    "min_zoom": 12,
                    "max_zoom": 16,
                },
                {
                    "tileset": "analysis_report",
                    "min_zoom": 12,
                    "max_zoom": 16,
                },
                {
                    "tileset": "analysis_building",
                    "min_zoom": 12,
                    "max_zoom": 16,
                },
                {
                    "tileset": "analysis_risk",
                    "min_zoom": 12,
                    "max_zoom": 16,
                },
                {
                    "tileset": "analysis_monitoring",
                    "min_zoom": 12,
                    "max_zoom": 16,
                },
                {
                    "tileset": "facade_scan",
                    "min_zoom": 12,
                    "max_zoom": 16,
                    "upload_dataset": True,
                },
                {
                    "tileset": "incident",
                    "min_zoom": 12,
                    "max_zoom": 16,
                    "upload_dataset": True,
                },
                {
                    "tileset": "incident_neighborhood",
                    "min_zoom": 10,
                    "max_zoom": 16,
                    "upload_dataset": True,
                },
                {
                    "tileset": "incident_municipality",
                    "min_zoom": 7,
                    "max_zoom": 11,
                    "upload_dataset": True,
                },
                {
                    "tileset": "incident_district",
                    "min_zoom": 10,
                    "max_zoom": 16,
                    "upload_dataset": True,
                },
                {
                    "tileset": "analysis_full",
                    "min_zoom": 10,
                    "max_zoom": 16,
                    "upload_dataset": True,
                    "generate_tiles": False,
                },
            ]
        },
    )
