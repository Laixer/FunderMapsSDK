import os
import glob
import httpx


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
