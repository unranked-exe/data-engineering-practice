import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from shutil import rmtree


# Remove the downloads directory
try:
    rmtree('downloads')
except:
    pass


site = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
dl_folder = 'downloads'
check_time = '15:27'

def main():
    # your code here
    file_name = find_file_name(check_time)

    if file_name:
        df = req_file(file_name)
        print(df.head())
        print(df.info())
        print(df[df['HourlyDryBulbTemperature'] == df['HourlyDryBulbTemperature'].max()])
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
        #print(dates)
        raw_date = [*map(html_tag_strip, dates)]
        #print(raw_date)
        data = {'links': links, 'dates': raw_date}

        
        df = pd.DataFrame(data)
        
        match = df[df['dates'].str.contains(lookup_time)]['links'].values[0]
        return match
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def html_tag_strip(text):
    text = text.replace('<td align="right">', '')
    text = text.replace('</td>', '')
    return text.strip()


def req_file(file_name):
    site_url = site + file_name
    file_name = dl_folder + '/' + file_name

    try:
        os.mkdir(dl_folder)
        print(f"Directory '{dl_folder}' created successfully.")

    except FileExistsError:
        print(f"Directory '{dl_folder}' already exists.")

    try:
        response = requests.get(site_url)
        if response.status_code == 200:
            with open(file_name, 'wb') as file:
                file.write(response.content)
            print(f"File '{file_name}' downloaded successfully.")
            df = pd.read_csv(file_name)
            return df
        else:
            print(f"Failed to download file. Status code: {response.status_code}")
    except requests.RequestException as e:
        print(f"Error downloading file: {e}")

    
    

if __name__ == "__main__":
    main()
