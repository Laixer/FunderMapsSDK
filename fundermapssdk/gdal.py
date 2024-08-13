import os
import asyncio


async def convert(input: str, output: str, *args):
    is_file = os.path.isfile(input)

    if is_file and not os.path.exists(input):
        raise FileNotFoundError("File not found")

    if output.startswith("PG:"):
        driver = "PostgreSQL"
    elif output.endswith(".gpkg"):
        driver = "GPKG"
    elif output.endswith(".geojson"):
        driver = "GeoJSONSeq"

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
