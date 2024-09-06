import os
import logging
import psycopg2

from fundermapssdk.config import DatabaseConfig


class DbProvider:
    def __init__(self, sdk, config: DatabaseConfig):
        self._sdk = sdk
        self.config = config
        self.db = None

        self.sql_directory = os.path.join(self._sdk.sdk_directory, "sql")

    def reindex_table(self, table: str):
        """
        Reindex the specified table.
        """

        self.__logger(logging.DEBUG, f"Reindexing table {table}")

        with self.db.cursor() as cur:
            cur.execute(f"REINDEX TABLE CONCURRENTLY {table};")

    def drop_table(self, table: str):
        """
        Drop the specified table.
        """

        self.__logger(logging.DEBUG, f"Dropping table {table}")

        with self.db.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table};")

    # TODO: Add support for schema
    def rename_table(self, old_table: str, new_table: str):
        """
        Rename the specified table.
        """

        self.__logger(logging.DEBUG, f"Renaming table {old_table} to {new_table}")

        with self.db.cursor() as cur:
            cur.execute(f"ALTER TABLE {old_table} RENAME TO {new_table};")

    def call(self, procedure: str):
        """
        Call the specified procedure.
        """

        self.__logger(logging.DEBUG, f"Calling procedure {procedure}")

        with self.db.cursor() as cur:
            cur.execute(f"CALL {procedure}();")

    def refresh_materialized_view(self, view: str):
        """
        Refresh the specified materialized view.
        """

        self.__logger(logging.DEBUG, f"Refreshing materialized view {view}")

        with self.db.cursor() as cur:
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};")

    def execute_script(self, script: str):
        """
        Execute the specified SQL script.
        """

        sql_file_path = f"{script}.sql"
        self.__logger(logging.DEBUG, f"Running SQL script: {sql_file_path}")

        file_path = os.path.join(self.sql_directory, sql_file_path)

        with open(file_path, "r") as sql_file:
            sql_script = sql_file.read()

            with self.db.cursor() as cur:
                cur.execute(sql_script)

    def __enter__(self):
        self.__logger(logging.DEBUG, "Connecting to database")

        self.db = psycopg2.connect(
            dbname=self.config.database,
            user=self.config.user,
            password=self.config.password,
            host=self.config.host,
            port=self.config.port,
        )
        self.db.autocommit = True

        self.__logger(logging.DEBUG, "Connected to database")

        return self

    def __exit__(self, type, value, traceback):
        self.__logger(logging.DEBUG, "Closing database connection")

        self.db.close()

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
