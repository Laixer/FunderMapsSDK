import time
import asyncio
import argparse

from fundermapssdk.command import FunderMapsCommand


class ModelRefreshCommand(FunderMapsCommand):
    """Command to refresh models in the database."""

    def __init__(self):
        super().__init__(description="Refresh models in the database")

    def add_arguments(self, parser: argparse.ArgumentParser):
        """Add command-line arguments for the command."""
        parser.add_argument(
            "--skip-risk", action="store_true", help="Skip risk calculation"
        )
        parser.add_argument(
            "--skip-statistics", action="store_true", help="Skip statistics refresh"
        )
        parser.add_argument(
            "--view", type=str, help="Refresh only a specific materialized view"
        )

    def _db_calculate_risk(self) -> bool:
        self.logger.info("Starting risk calculation...")
        start_time = time.time()
        try:
            with self.fundermaps.db as db:
                self.logger.info("Refreshing building_sample view...")
                db.refresh_materialized_view("data.building_sample")

                self.logger.info("Refreshing cluster_sample view...")
                db.refresh_materialized_view("data.cluster_sample")

                self.logger.info("Refreshing supercluster_sample view...")
                db.refresh_materialized_view("data.supercluster_sample")

                self.logger.info("Executing risk model calculation...")
                db.call("data.model_risk_manifest")

                self.logger.info("Reindexing risk model table...")
                db.reindex_table("data.model_risk_static")

            elapsed = time.time() - start_time
            self.logger.info(f"Risk calculation completed in {elapsed:.2f}s")
            return True
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(
                f"Risk calculation failed after {elapsed:.2f}s: {e}", exc_info=True
            )
            return False

    def _db_refresh_statistics(self, specific_view: str = None) -> bool:
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

        if specific_view:
            if specific_view not in views:
                self.logger.warning(
                    f"View {specific_view} not in known statistics views"
                )
                return False
            views = [specific_view]

        self.logger.info(f"Starting statistics refresh for {len(views)} views...")
        start_time = time.time()
        failure_count = 0

        try:
            with self.fundermaps.db as db:
                for view in views:
                    view_start = time.time()
                    try:
                        self.logger.info(f"Refreshing materialized view: {view}")
                        db.refresh_materialized_view(view)
                        view_elapsed = time.time() - view_start
                        self.logger.info(f"Refreshed {view} in {view_elapsed:.2f}s")
                    except Exception as e:
                        failure_count += 1
                        view_elapsed = time.time() - view_start
                        self.logger.error(
                            f"Failed to refresh {view} after {view_elapsed:.2f}s: {e}",
                            exc_info=True,
                        )

            elapsed = time.time() - start_time
            if failure_count == 0:
                self.logger.info(f"Statistics refresh completed in {elapsed:.2f}s")
                return True
            else:
                self.logger.warning(
                    f"Statistics refresh completed with {failure_count} failures in {elapsed:.2f}s"
                )
                return failure_count < len(
                    views
                )  # Success if at least some views refreshed
        except Exception as e:
            elapsed = time.time() - start_time
            self.logger.error(
                f"Statistics refresh failed after {elapsed:.2f}s: {e}", exc_info=True
            )
            return False

    async def execute(self) -> int:
        """Execute the model refresh command."""
        success = True

        if hasattr(self.args, "view") and self.args.view:
            self.logger.info(f"Refreshing single view: {self.args.view}")
            return 0 if self._db_refresh_statistics(self.args.view) else 1

        if not hasattr(self.args, "skip_risk") or not self.args.skip_risk:
            self.logger.info("Step 1: Calculating risk metrics...")
            if not self._db_calculate_risk():
                success = False
                self.logger.error("Risk calculation failed")

        if not hasattr(self.args, "skip_statistics") or not self.args.skip_statistics:
            self.logger.info("Step 2: Refreshing statistics...")
            if not self._db_refresh_statistics():
                success = False
                self.logger.error("Statistics refresh failed")

        return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(ModelRefreshCommand().run())
    exit(exit_code)
