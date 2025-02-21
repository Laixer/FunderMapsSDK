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
        # cron="55 * * * *",
        print_next_steps=False,
    )

    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="refresh_models.py:refresh_models",
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

    # extract_mapset(
    #     [
    #         TileBundle("analysis_foundation", 12, 16),
    #         TileBundle("analysis_report", 12, 16),
    #         TileBundle("analysis_building", 12, 16),
    #         TileBundle("analysis_risk", 12, 16),
    #         TileBundle("analysis_monitoring", 12, 16),
    #         TileBundle("facade_scan", 12, 16, upload_dataset=True),
    #         TileBundle("incident", 12, 16, upload_dataset=True),
    #         TileBundle(
    #             "incident_neighborhood",
    #             10,
    #             16,
    #             upload_dataset=True,
    #         ),
    #         TileBundle("incident_municipality", 7, 11, upload_dataset=True),
    #         TileBundle("incident_district", 10, 16, upload_dataset=True),
    #         # TileBundle(
    #         #     "analysis_full", 10, 16, upload_dataset=True, generate_tiles=False
    #         # ),
    #     ]
    # )
