import requests
from bs4 import BeautifulSoup
import warnings
import pandas as pd
import datetime
from sqlalchemy import create_engine
import psycopg2
warnings.filterwarnings("ignore")

# Create an engine instance

# Connect to PostgreSQL server

dbConnection = alchemyEngine.connect();

# URL from which pdfs to be downloaded
url = "https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/"

# Requests URL and get response object
response = requests.get(url)

# Parse text obtained
soup = BeautifulSoup(response.text, 'html.parser')

# Find all hyperlinks present on webpage
links = soup.find_all('a')

i = 0
# From all links check for CSV link and
# if present download file
for link in links:
    download_link = url + link.get('href')
    if ('.csv' in link.get('href', [])):
        i += 1
        #print("Downloading file: ", i, 'from link: ', link)

        # Get response object for link
        #response = requests.get(download_link)
        print('Pandas: Reading File No.: ', i, 'From Link: ',  download_link)
        df = pd.read_csv(download_link)
        df['last_updated'] = datetime.datetime.now()
        df['donwnload_link'] = download_link
        df.to_sql(name='stg_monthly_aqhi_data', con=conn)
        # Write content in CSV file
        #csv = open("csv" + str(i) + ".csv", 'wb')
        #csv.write(response.content)
        #csv.close()

print("All CSV files downloaded")