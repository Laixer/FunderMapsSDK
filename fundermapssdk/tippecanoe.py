import logging
import asyncio
import shutil
from typing import List, Optional

logger = logging.getLogger("tippecanoe")


# TODO: Template this structure for other commands
async def tippecanoe(
    input: str,
    output: str,
    layer: Optional[str] = None,
    max_zoom_level: int = 15,
    min_zoom_level: int = 10,
    additional_args: List[str] = None,
) -> bool:
    """
    Asynchronously runs the tippecanoe command to convert geospatial data to a vector tileset.

    Args:
        input (str): The input file path.
        output (str): The output directory path.
        layer (Optional[str]): The layer name. If None, tippecanoe will use a default name.
        max_zoom_level (int): The maximum zoom level. Defaults to 15.
        min_zoom_level (int): The minimum zoom level. Defaults to 10.
        additional_args (List[str]): Additional arguments to pass to tippecanoe.

    Returns:
        bool: True if the command was successful, False otherwise.

    Examples:
        >>> await tippecanoe("data.geojson", "output_tiles", "my_layer")
        True
        >>> await tippecanoe("data.geojson", "output_tiles", additional_args=["--simplification=10"])
        True

    Raises:
        FileNotFoundError: If the tippecanoe command is not found.
        Exception: If the command fails.
    """

    if not shutil.which("tippecanoe"):
        raise FileNotFoundError(
            "tippecanoe command not found. Please install it first."
        )

    cmd_args = [
        "--force",
        "--read-parallel",
        "--drop-densest-as-needed",
        "--quiet",
        "--no-tile-compression",
    ]

    if additional_args:
        cmd_args.extend(additional_args)

    command = [
        "tippecanoe",
        *cmd_args,
        "-z",
        str(max_zoom_level),
        "-Z",
        str(min_zoom_level),
        "--output-to-directory",
        output,
    ]

    if layer:
        command.extend(["-l", layer])

    command.append(input)

    logger.debug(f"Running command: {' '.join(command)}")

    try:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.debug(f"Command succeeded: {stdout.decode().strip()}")
        else:
            error_msg = stderr.decode().strip()
            logger.error(f"Command failed: {error_msg}")
            raise Exception(f"Tippecanoe command failed: {error_msg}")

        return process.returncode == 0
    except Exception as e:
        logger.exception(f"Error running tippecanoe: {str(e)}")
        raise
