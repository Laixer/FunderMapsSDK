import os
import glob
import httpx

from configparser import ConfigParser


async def http_download_file(url, dest_path):
    if os.path.exists(dest_path):
        os.remove(dest_path)

    async with httpx.AsyncClient() as client:
        with open(dest_path, "wb") as file:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes():
                    file.write(chunk)


def remove_files(directory, extension=".gpkg"):
    files = glob.glob(os.path.join(directory, f"*{extension}"))

    for file_path in files:
        os.remove(file_path)


def find_config() -> ConfigParser:
    """
    Finds and reads the configuration file.

    Returns:
        ConfigParser: The parsed configuration object.

    Raises:
        FileNotFoundError: If no configuration file is found in the specified paths.
    """

    config = ConfigParser()

    config_paths = [
        "/etc/fundermaps/config.ini",
        "./config.ini",
    ]

    for path in config_paths:
        if os.path.exists(path):
            config.read(path)
            break
    else:
        raise FileNotFoundError("No configuration file found in the specified paths.")

    return config
