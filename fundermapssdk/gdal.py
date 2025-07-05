import os
import logging
import asyncio

from fundermapssdk.config import DatabaseConfig


class GDALProvider:
    """
    A class to interact with GDAL (Geospatial Data Abstraction Library) for various geospatial data operations.

    Attributes:
        _sdk: The SDK instance.
        config (DatabaseConfig): The database configuration.

    Methods:
        version() -> tuple[int, int, int]:
            Asynchronously retrieves the GDAL version.

        _pg_connection_string() -> str:
            Constructs the PostgreSQL connection string from the configuration.

        from_postgis(output: str, *args) -> bool:
            Asynchronously converts data from a PostGIS database to a specified output format.

        to_postgis(input: str, *args) -> bool:
            Asynchronously converts data from a specified input format to a PostGIS database.

        ogr2ogr(input: str, output: str, *args) -> bool:
            Asynchronously runs the ogr2ogr command to convert geospatial data between formats.

        __logger(level, message):
            Logs messages using the SDK's logger.
    """

    def __init__(self, sdk, config: DatabaseConfig):
        self._sdk = sdk
        self.config = config

    async def version(self) -> tuple[int, int, int]:
        process = await asyncio.create_subprocess_exec(
            "ogr2ogr", "--version", stdout=asyncio.subprocess.PIPE
        )

        stdout, _ = await process.communicate()
        version_output = stdout.decode().strip()
        version_number = version_output.split()[1].strip(",")

        major, minor, patch = map(int, version_number.split("."))
        return major, minor, patch

    def _pg_connection_string(self) -> str:
        return f"PG:dbname='{self.config.database}' host='{self.config.host}' port='{self.config.port}' user='{self.config.user}' password='{self.config.password}'"

    async def from_postgis(self, output: str, *args) -> bool:
        input = self._pg_connection_string()
        return await self.ogr2ogr(input, output, *args)

    async def to_postgis(self, input: str, *args) -> bool:
        output = self._pg_connection_string()
        return await self.ogr2ogr(input, output, *args)

    async def ogr2ogr(self, input: str, output: str, *args) -> bool:
        is_file = os.path.isfile(input)

        if is_file and not os.path.exists(input):
            raise FileNotFoundError("File not found")

        if os.path.splitext(input)[1] == ".zip":
            input = f"/vsizip/{input}"

        if output.startswith("PG:"):
            driver = "PostgreSQL"
        elif output.endswith(".gpkg"):
            driver = "GPKG"
        elif output.endswith(".geojson"):
            driver = "GeoJSONSeq"
        else:
            raise ValueError("Unsupported output format")

        cmd_args = ["-overwrite"]

        gdal_version = await self.version()
        if gdal_version < (3, 0, 0):
            raise Exception("GDAL version 3.0.0 or higher is required")

        if gdal_version > (3, 8, 0) and gdal_version < (3, 9, 0):
            cmd_args.extend(
                [
                    "--config",
                    "OGR2OGR_USE_ARROW_API",
                    "NO",
                ]
            )

        if os.path.splitext(input)[1] == ".csv":
            if "semicolon" in os.path.basename(input).lower():
                cmd_args.extend(
                    [
                        "-oo",
                        "SEPARATOR=SEMICOLON",
                    ]
                )
            elif "pipe" in os.path.basename(input).lower():
                cmd_args.extend(
                    [
                        "-oo",
                        "SEPARATOR=PIPE",
                    ]
                )

        process = await asyncio.create_subprocess_exec(
            "ogr2ogr",
            *cmd_args,
            "-f",
            driver,
            output,
            input,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            self.__logger(
                logging.DEBUG, f"Command succeeded: {stdout.decode().strip()}"
            )
        else:
            raise Exception(f"Command failed: {stderr.decode().strip()}")

        return process.returncode == 0

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
