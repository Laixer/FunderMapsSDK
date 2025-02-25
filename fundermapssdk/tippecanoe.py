import logging
import asyncio

logger = logging.getLogger("tippecanoe")


async def tippecanoe(
    input: str,
    output: str,
    layer: str | None = None,
    max_zoom_level: int = 15,
    min_zoom_level: int = 10,
) -> bool:
    """
    Asynchronously runs the tippecanoe command to convert geospatial data to a vector tileset.

    Args:
        input (str): The input file path.
        output (str): The output directory path.
        layer (str): The layer name.
        max_zoom_level (int): The maximum zoom level. Defaults to 15.
        min_zoom_level (int): The minimum zoom level. Defaults to 10.

    Returns:
        bool: True if the command was successful, False otherwise.
    """

    cmd_args = [
        "--force",
        "--read-parallel",
        "--drop-densest-as-needed",
        "--quiet",
        "--no-tile-compression",
    ]

    process = await asyncio.create_subprocess_exec(
        "tippecanoe",
        *cmd_args,
        "-z",
        str(max_zoom_level),
        "-Z",
        str(min_zoom_level),
        "--output-to-directory",
        output,
        "-l",
        layer,
        input,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        logger.debug(f"Command succeeded: {stdout.decode().strip()}")
    else:
        raise Exception(f"Command failed: {stderr.decode().strip()}")

    return process.returncode == 0
