import logging
import argparse
import colorlog
from systemd import journal
from configparser import ConfigParser
from fundermapssdk import util, app

SCRIPT_NAME = "refresh_models"


class GeneratePdfTask(app.FunderMapsTask):
    async def run(self):
        with self.fundermaps.db as db:
            # TODO: Check if enough data has changed to refresh models

            self.logger.info("Refreshing building models")
            db.refresh_materialized_view("data.building_sample")
            db.refresh_materialized_view("data.cluster_sample")
            db.refresh_materialized_view("data.supercluster_sample")

            self.logger.info("Refreshing risk models")
            db.call("data.model_risk_manifest")
            db.reindex_table("data.model_risk_static")

            self.logger.info("Refreshing statistics")
            db.refresh_materialized_view("data.statistics_product_inquiries")
            db.refresh_materialized_view("data.statistics_product_inquiry_municipality")
            db.refresh_materialized_view("data.statistics_product_incidents")
            db.refresh_materialized_view(
                "data.statistics_product_incident_municipality"
            )
            db.refresh_materialized_view("data.statistics_product_foundation_type")
            db.refresh_materialized_view("data.statistics_product_foundation_risk")
            db.refresh_materialized_view("data.statistics_product_data_collected")
            db.refresh_materialized_view("data.statistics_product_construction_years")
            db.refresh_materialized_view("data.statistics_product_buildings_restored")
            db.refresh_materialized_view("data.statistics_postal_code_foundation_type")
            db.refresh_materialized_view("data.statistics_postal_code_foundation_risk")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="FunderMaps SDK Script Runner")

    parser.add_argument("-c", "--config", help="path to the configuration file")
    parser.add_argument("-l", "--log-level", help="log level", default="INFO")
    parser.add_argument("--systemd", help="log to systemd", action="store_true")

    args = parser.parse_args()

    if args.systemd:
        handler = journal.JournalHandler(SYSLOG_IDENTIFIER=args.script)
    else:
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(levelname)-8s %(name)s%(reset)s %(message)s"
            )
        )

    logging.basicConfig(level=args.log_level, handlers=[handler], format="%(message)s")

    if args.config:
        config = ConfigParser()
        config.read(args.config)
    else:
        config = util.find_config()

    GeneratePdfTask(SCRIPT_NAME, config).asyncio_invoke()
