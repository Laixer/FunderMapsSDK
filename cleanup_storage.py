import asyncio

from fundermapssdk.command import FunderMapsCommand


class CleanupStorageCommand(FunderMapsCommand):
    def __init__(self):
        super().__init__(description="Clean up orphaned file resources")

    async def execute(self):
        """Execute the storage cleanup command."""

        deleted_count = 0
        failed_count = 0
        with self.fundermaps.db as db:
            with db.db.cursor() as cur:
                query = """SELECT * FROM application.file_resources_orphaned"""
                cur.execute(query)

                # Get column names from cursor description
                column_names = [desc[0] for desc in cur.description]
                # Find indices for the columns we need
                id_idx = column_names.index("id")
                key_idx = column_names.index("key")
                filename_idx = column_names.index("original_filename")

                orphaned_files = cur.fetchall()

                if not orphaned_files:
                    self.logger.info("No orphaned files found")
                    return 0

                self.logger.info(f"Found {len(orphaned_files)} orphaned files")

                with self.fundermaps.s3 as s3:
                    for file in orphaned_files:
                        file_id = file[id_idx]
                        file_key = file[key_idx]
                        file_name = file[filename_idx]
                        s3_path = f"user-data/{file_key}/{file_name}"

                        try:
                            self.logger.info(f"Deleting {s3_path} from S3")
                            s3.delete_file(s3_path, bucket="fundermaps")

                            # Delete from database too
                            delete_query = """DELETE FROM application.file_resources WHERE id = %s"""
                            cur.execute(delete_query, (file_id,))

                            deleted_count += 1
                        except Exception as e:
                            self.logger.error(f"Failed to delete {s3_path}: {str(e)}")
                            failed_count += 1

                # Commit the database changes
                db.db.commit()

        # Summary report
        self.logger.info(
            f"Cleanup complete: {deleted_count} files deleted, {failed_count} failed"
        )
        return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(CleanupStorageCommand().run())
    exit(exit_code)
