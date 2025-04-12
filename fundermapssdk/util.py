import os
import gzip
import glob
import shutil
import httpx


FILE_ALLOWED_EXTENSIONS = [".geojson", ".gpkg", ".shp", ".zip", ".csv"]
FILE_MIN_SIZE: int = 1024  # 1 KB


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


# TODO: Maybe we do not need this function
def remove_files(directory, extension):
    """
    Remove files with a specific extension from a directory.

    Args:
        directory (str): The directory to search for files.
        extension (str): The extension of the files to remove.
    """

    files = glob.glob(os.path.join(directory, f"*{extension}"))

    for file_path in files:
        os.remove(file_path)


def collect_files_with_extension(directory, extension) -> list:
    """
    Collects files with a specific extension from a directory.

    Args:
        directory (str): The directory to search for files.
        extension (str): The extension of the files to collect.

    Returns:
        list: A list of file paths with the specified extension.
    """
    collected_files = []

    for root, _, files in os.walk(directory):
        for filename in files:
            file_ext = os.path.splitext(filename)[1]
            if file_ext != extension:
                continue

            local_path = os.path.join(root, filename)
            collected_files.append(local_path)

    return collected_files


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


def validate_file_extension(file_path, allowed_extensions):
    """
    Validates the extension of a file.

    Args:
        file_path (str): The path to the file.
        allowed_extensions (list): A list of allowed extensions.

    Raises:
        ValueError: If the file extension is not in the allowed extensions list.
    """

    file_extension = os.path.splitext(file_path)[1]

    if file_extension not in allowed_extensions:
        raise ValueError("File extension is not allowed")


def date_path(with_month=True, with_day=True):
    """
    Generates a date-based path for storing files.

    Returns:
        str: The generated path based on the current date.
    """

    from datetime import datetime

    current_date = datetime.now()
    formatted_date_year = current_date.strftime("%Y")
    formatted_date_month = current_date.strftime("%b").lower()
    formatted_date_day = current_date.strftime("%d")

    path = f"{formatted_date_year}"
    if with_month:
        path += f"/{formatted_date_month}"
    if with_day:
        path += f"/{formatted_date_day}"
    return path


def compress_file(file_path, output_path):
    """
    Compresses a file using gzip.

    Args:
        file_path (str): The path to the file to compress.
        output_path (str): The path where the compressed file will be saved.
    """

    with open(file_path, "rb") as f_in:
        with gzip.open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


def decompress_file(file_path, output_path):
    """
    Decompresses a gzip file.

    Args:
        file_path (str): The path to the gzip file to decompress.
        output_path (str): The path where the decompressed file will be saved.
    """
    with gzip.open(file_path, "rb") as f_in:
        with open(output_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
