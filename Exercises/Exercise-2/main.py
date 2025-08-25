import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from shutil import rmtree
import urllib.parse
import asyncio
import aiohttp


site = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
dl_folder = 'downloads'
check_time = '15:45'


def setup_downloads_folder():
    # Remove the downloads directory and contents
    try:
        rmtree(dl_folder)
    except FileNotFoundError:
        print(f"Directory '{dl_folder}' does not exist, no need to remove.")

    try:
        os.mkdir(dl_folder)
        print(f"Directory '{dl_folder}' created successfully.")

    except FileExistsError:
        print(f"Directory '{dl_folder}' already exists.")


def main():
    setup_downloads_folder()
    # your code here
    matched_name = find_file_name(check_time)
    print(matched_name)
    if matched_name is not None and len(matched_name) > 0:
        list_of_dfs = [req_file(file_name) for file_name in matched_name]
        combined_df = pd.concat(list_of_dfs, ignore_index=True)
        print(combined_df.head())
        print(combined_df['STATION'].value_counts())
        #Attempted to implement a parquet save of combined_df but failed due to data not being tidy(differing column types)
        #combined_df.to_parquet('combined.parquet', engine='pyarrow', index=False, compression='snappy')

    else:
        print("File not found.")


def find_file_name(lookup_time):
    try:
        response = requests.get(site)
        soup = BeautifulSoup(response.content, 'html.parser')
        entries = soup.find_all('tr')

        #links = [str(entry.find('a').text) for entry in entries]
        links = [entry.find('a')['href'] for entry in entries if entry.find('a')]
        
        #dates = [str(entry.find('td', align='right')) for entry in entries]
        dates = [entry.find('td', align='right').text for entry in entries if entry.find('td', align='right')]

        data = {'links': links, 'dates': dates}

        df = pd.DataFrame(data)
        
        match = df[df['dates'].str.contains(lookup_time)]['links'].values
        return match
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def req_file(file_name):
    site_url = urllib.parse.urljoin(site, file_name)
    file_path = os.path.join(dl_folder, file_name)

    try:
        response = requests.get(site_url)
        if response.status_code == 200:
            with open(file_path, 'wb') as file:
                file.write(response.content)
            print(f"File '{file_path}' downloaded successfully.")
            df = pd.read_csv(file_path)
            # For Data lineage
            df['source_file'] = file_path
            df['download_time'] = pd.Timestamp.now()
            return df
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error downloading file: {e}")
        return None


if __name__ == "__main__":
    main()
