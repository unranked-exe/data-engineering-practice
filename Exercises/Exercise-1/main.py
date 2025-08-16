import os
import requests
import zipfile
import io
from shutil import rmtree

# Remove the downloads directory
rmtree('downloads')

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

def main():
    # your code here
    try:
        os.mkdir(dl_folder)
        print(f"Directory '{dl_folder}' created successfully.")
    except FileExistsError:
        print(f"Directory '{dl_folder}' already exists.")

    try:
        for link in download_uris:
            response = requests.get(link)
            if response.status_code == 200:
                #Get file name
                file_name = link.split("/")[-1].replace('.zip', '')

                #Create a BytesIO object
                zip_file = io.BytesIO(response.content)

                #Extract ZIP to CSV
                with zipfile.ZipFile(zip_file) as z:
                    z.extract(file_name + '.csv', dl_folder + '/' + file_name)
            else:
                print(f"Failed to download {link}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
