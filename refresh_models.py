import time
import asyncio

from fundermapssdk.cli import FunderMapsCommand


class ModelRefreshCommand(FunderMapsCommand):
    """Command to refresh models in the database."""

    def __init__(self):
        super().__init__(description="Refresh models in the database")

    def _db_calculate_risk(self):
        self.logger.info("Starting risk calculation...")
        start_time = time.time()
        with self.fundermaps.db as db:
            self.logger.info("Refreshing building_sample view...")
            db.refresh_materialized_view("data.building_sample")

            self.logger.info("Refreshing cluster_sample view...")
            db.refresh_materialized_view("data.cluster_sample")

            self.logger.info("Refreshing supercluster_sample view...")
            db.refresh_materialized_view("data.supercluster_sample")

            db.call("data.model_risk_manifest")
            db.reindex_table("data.model_risk_static")
        elapsed = time.time() - start_time
        self.logger.info(f"Risk calculation completed in {elapsed:.2f}s")

    def _db_refresh_statistics(self):
        self.logger.info("Starting statistics refresh...")
        start_time = time.time()
        with self.fundermaps.db as db:
            views = [
                "data.statistics_product_inquiries",
                "data.statistics_product_inquiry_municipality",
                "data.statistics_product_incidents",
                "data.statistics_product_incident_municipality",
                "data.statistics_product_foundation_type",
                "data.statistics_product_foundation_risk",
                "data.statistics_product_data_collected",
                "data.statistics_product_construction_years",
                "data.statistics_product_buildings_restored",
                "data.statistics_postal_code_foundation_type",
                "data.statistics_postal_code_foundation_risk",
            ]

            for view in views:
                self.logger.info(f"Refreshing materialized view: {view}")
                view_start = time.time()
                db.refresh_materialized_view(view)
                view_elapsed = time.time() - view_start
                self.logger.info(f"Refreshed {view} in {view_elapsed:.2f}s")

        elapsed = time.time() - start_time
        self.logger.info(f"Statistics refresh completed in {elapsed:.2f}s")

    async def execute(self):
        """Execute the model refresh command."""
        self.logger.info("Step 1: Calculating risk metrics...")
        self._db_calculate_risk()

        self.logger.info("Step 2: Refreshing statistics...")
        self._db_refresh_statistics()


if __name__ == "__main__":
    exit_code = asyncio.run(ModelRefreshCommand().run())
    exit(exit_code)
