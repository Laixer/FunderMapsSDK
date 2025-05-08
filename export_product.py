import csv
import asyncio
from datetime import datetime

from fundermapssdk.cli import FunderMapsCommand

# TODO: Get from the database
ORGANIZATIONS: list[str] = [
    "5c2c5822-6996-4306-96ba-6635ea7f90e2",
    "8a56e920-7811-47b7-9289-758c8fe346db",
    "c06a1fc6-6452-4b88-85fd-ba50016c578f",
    "58872000-cb69-433a-91ba-165a9d0b4710",
    "0ca4d02d-8206-4453-ba45-84f532c868f3",
    "8a9db31a-1142-4e8e-b1c2-17bfa2b0f2c2",
    "0689db53-3fc7-4a7e-939e-26580c677ea0",
]


class ProductExportCommand(FunderMapsCommand):
    """Command to export product tracker data for organizations."""

    def __init__(self):
        super().__init__(description="Export product tracker data")

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            type=str,
            default=None,
            help="Reference date for export in YYYY-MM-DD format (defaults to current date)",
        )

    async def process_export(self, organization: str, reference_date: datetime):
        """Process export for a specific organization."""
        self.logger.info("Exporting product tracker data")

        with self.fundermaps.db as db:
            with db.db.cursor() as cur:
                query = """
                    SELECT
                            pt.organization_id,
                            pt.product,
                            pt.building_id,
                            b.external_id,
                            pt.create_date,
                            pt.identifier AS request
                    FROM    application.product_tracker AS pt
                    JOIN    geocoder.building AS b ON b.id = pt.building_id
                    WHERE   pt.organization_id = %s
                    AND     pt.create_date >= date_trunc('month', %s) - interval '1 month'
                    AND     pt.create_date < date_trunc('month', %s)"""

                cur.execute(query, (organization, reference_date, reference_date))

                csv_file = f"{organization}.csv"

                # TODO: Maybe create a CSV writer helper function in the SDK
                column_names = [desc[0] for desc in cur.description]

                self.logger.info(f"Writing data to {csv_file}")
                with open(csv_file, mode="w", newline="") as file:
                    writer = csv.writer(file)

                    writer.writerow(column_names)

                    data_written = False
                    for row in cur:
                        writer.writerow(row)
                        data_written = True

        if data_written:
            with self.fundermaps.s3 as s3:
                formatted_date_year = reference_date.strftime("%Y")
                formatted_date_month = reference_date.strftime("%b").lower()

                self.logger.info(f"Uploading {csv_file} to S3")

                s3_path = f"product/{formatted_date_year}/{formatted_date_month}/{organization}.csv"
                s3.upload_file(csv_file, s3_path, bucket="fundermaps-data")
        else:
            self.logger.info("No data to export")

    async def execute(self):
        """Execute the product export command."""

        reference_date = (
            datetime.strptime(self.args.date, "%Y-%m-%d")
            if self.args.date
            else datetime.now()
        )
        self.logger.info(f"Reference date: {reference_date}")

        # TODO: Fetch the organization IDs from the database
        for organization in ORGANIZATIONS:
            self.logger.info(f"Processing organization: {organization}")
            await self.process_export(organization, reference_date)


if __name__ == "__main__":
    exit_code = asyncio.run(ProductExportCommand().run())
    exit(exit_code)
