import os
import asyncio

from fundermapssdk.config import DatabaseConfig


class GDALProvider:
    def __init__(self, sdk, config: DatabaseConfig):
        self._sdk = sdk
        self.config = config

    async def convert(self, input: str, output: str, *args):
        is_file = os.path.isfile(input)

        if is_file and not os.path.exists(input):
            raise FileNotFoundError("File not found")

        if output.startswith("PG:"):
            driver = "PostgreSQL"
        elif output.endswith(".gpkg"):
            driver = "GPKG"
        elif output.endswith(".geojson"):
            driver = "GeoJSONSeq"

        if output.startswith("PG:"):
            output = f"PG:dbname='{self.config.database}' host='{self.config.host}' port='{self.config.port}' user='{self.config.user}' password='{self.config.password}'"

        if input.startswith("PG:"):
            input = f"PG:dbname='{self.config.database}' host='{self.config.host}' port='{self.config.port}' user='{self.config.user}' password='{self.config.password}'"

        process = await asyncio.create_subprocess_exec(
            "ogr2ogr",
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
            print(f"Command succeeded: {stdout.decode().strip()}")
        else:
            print(f"Command failed: {stderr.decode().strip()}")
