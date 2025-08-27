import asyncio
import io
import os
import zipfile
from pathlib import Path
from shutil import rmtree

import aiohttp

DL_FOLDER = Path("downloads")


# Remove the downloads directory
def setup_downloads_folder():
    # Remove the downloads directory and contents
    try:
        rmtree(DL_FOLDER)
    except FileNotFoundError:
        print(f"Directory '{DL_FOLDER}' does not exist, no need to remove.")

    try:
        os.mkdir(DL_FOLDER)
        print(f"Directory '{DL_FOLDER}' created successfully.")

    except FileExistsError:
        print(f"Directory '{DL_FOLDER}' already exists.")


domain = "https://divvy-tripdata.s3.amazonaws.com/{file}"
download_uris = [
    domain.format(file="Divvy_Trips_2018_Q4.zip"),
    domain.format(file="Divvy_Trips_2019_Q1.zip"),
    domain.format(file="Divvy_Trips_2019_Q2.zip"),
    domain.format(file="Divvy_Trips_2019_Q3.zip"),
    domain.format(file="Divvy_Trips_2019_Q4.zip"),
    domain.format(file="Divvy_Trips_2020_Q1.zip"),
    domain.format(file="Divvy_Trips_2220_Q1.zip"),
]


async def main():
    # your code here
    setup_downloads_folder()
    async with aiohttp.ClientSession() as session:  # noqa: SIM117
        async with asyncio.TaskGroup() as tg:
            for url in download_uris:
                tg.create_task(download_file(session, url))
        # TaskGroup waits and will raise on first exception


async def download_file(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            file_name = url.split("/")[-1]
            file_path = DL_FOLDER / file_name
            with open(file_path, "wb") as f:
                f.write(await response.read())
            print(f"Downloaded {file_name}")
            print(file_path)
            convert_zip(file_path)
        else:
            print(f"Failed to download {url}")


def convert_zip(file_path):
    # Get file name
    file_name = file_path.with_suffix(".csv")

    # Create a BytesIO object and read zip file
    with open(file_path, "rb") as f:
        content = f.read()
    zip_file = io.BytesIO(content)
    extract_path = file_name
    extract_zips(zip_file, extract_path)


def extract_zips(zip_file, extract_path):
    with zipfile.ZipFile(zip_file) as z:
        z.extractall(path=extract_path)


if __name__ == "__main__":
    asyncio.run(main())
