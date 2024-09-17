import os
import glob
import httpx

from configparser import ConfigParser


async def http_download_file(url, dest_path):
    """
    Downloads a file from the given URL and saves it to the specified destination path.

    Args:
        url (str): The URL of the file to download.
        dest_path (str): The destination path where the downloaded file will be saved.

    Raises:
        httpx.HTTPError: If there is an error during the HTTP request.

    """

    if os.path.exists(dest_path):
        os.remove(dest_path)

    async with httpx.AsyncClient() as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            with open(dest_path, "wb") as file:
                async for chunk in response.aiter_bytes():
                    file.write(chunk)


def remove_files(directory, extension=".gpkg"):
    """
    Remove files with a specific extension from a directory.

    Args:
        directory (str): The directory to search for files.
        extension (str): The extension of the files to remove.
    """

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


def validate_file_size(file_path, min_size):
    """
    Validates the size of a file.

    Args:
        file_path (str): The path to the file.
        min_size (int): The minimum size the file must be.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file size is below the minimum size.
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError("File not found")

    if os.path.getsize(file_path) < min_size:
        raise ValueError("File is below the minimum")


def datetime_path():
    """
    Generates a datetime-based path for storing files.

    Returns:
        str: The generated path based on the current date.
    """

    from datetime import datetime

    current_date = datetime.now()
    formatted_date_year = current_date.strftime("%Y")
    formatted_date_month = current_date.strftime("%b").lower()
    formatted_date_day = current_date.strftime("%d")

    return f"{formatted_date_year}/{formatted_date_month}/{formatted_date_day}"
