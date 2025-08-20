import os
import zipfile
import io
from shutil import rmtree
import asyncio
import aiohttp

# Remove the downloads directory
try:
    rmtree('downloads')
except:
    pass

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

dl_folder = 'downloads'

async def main():
    # your code here
    try:
        os.mkdir(dl_folder)
        print(f"Directory '{dl_folder}' created successfully.")

    except FileExistsError:
        print(f"Directory '{dl_folder}' already exists.")
    async with aiohttp.ClientSession() as session:
            tasks = [fetch_zip(session, url) for url in download_uris]
            await asyncio.gather(*tasks)
    


async def fetch_zip(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            #Get file name
            file_name = url.split("/")[-1].replace('.zip', '')

            #Create a BytesIO object
            content = await response.read()
            zip_file = io.BytesIO(content)

            #Extract ZIP to CSV
            with zipfile.ZipFile(zip_file) as z:
                z.extract(file_name + '.csv', dl_folder + '/' + file_name)
        
        else:
            print(f"Failed to download {url}")
            


if __name__ == "__main__":
    asyncio.run(main())
