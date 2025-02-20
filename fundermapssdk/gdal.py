import os
import logging
import asyncio

from fundermapssdk.config import DatabaseConfig


class GDALProvider:
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

        if output.startswith("PG:"):
            driver = "PostgreSQL"
        elif output.endswith(".gpkg"):
            driver = "GPKG"
        elif output.endswith(".geojson"):
            driver = "GeoJSONSeq"

        cmd_args = ["-overwrite"]

        gdal_version = await self.version()
        if gdal_version > (3, 8, 0) and gdal_version < (3, 9, 0):
            cmd_args.extend(
                [
                    "--config",
                    "OGR2OGR_USE_ARROW_API",
                    "NO",
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
            self.__logger(logging.ERROR, f"Command failed: {stderr.decode().strip()}")

        return process.returncode == 0

    def __logger(self, level, message):
        return self._sdk._logger.log(level, f"{self.__class__.__name__}: {message}")
