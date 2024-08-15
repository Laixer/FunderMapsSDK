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
    process = await asyncio.create_subprocess_exec(
        "tippecanoe",
        "--force",
        "--read-parallel",
        "-z",
        str(max_zoom_level),
        "-Z",
        str(min_zoom_level),
        "--output-to-directory",
        # "-o",
        output,
        "--drop-densest-as-needed",
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
        logger.error(f"Command failed: {stderr.decode().strip()}")

    return process.returncode == 0
