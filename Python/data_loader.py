import requests
from bs4 import BeautifulSoup
import warnings
import pandas as pd
import datetime
import psycopg2 as pg
import sqlalchemy
import os
from rpy2 import robjects as ro
import rpy2.robjects.packages as rpackages
from rpy2.robjects.packages import importr, data
from rpy2.robjects import pandas2ri


warnings.filterwarnings("ignore")

# To write temp files into the Parent ./Data/ Folder to
# to keep the Python folder clean of csv and temp files
def get_parent_dir(directory):
    import os
    return os.path.dirname(directory)

current_dirs_parent = get_parent_dir(os.getcwd())
parent_dir = os.path.split(os.getcwd())[0]

print('Working Directory: ', current_dirs_parent)
print('Parent Directory: ', parent_dir)

pg_engine = pg.connect(host='localhost', port='5433', dbname='postgres', user='postgres', password='postgres')
# Connection String is of the form: ‘postgresql://username:password@databasehost:port/databasename’
sqlalchemy_engine = sqlalchemy.create_engine('postgresql://postgres:postgres@localhost:5433/postgres')


def load_monthly_data(save_locally):
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
        if ('.csv' in link.get('href', [])):
            download_link = url + link.get('href')
            print('Download Link: ', download_link)
            filename = parent_dir + '/Data/' + link.get('href')
            if save_locally == True:
                print("Filename to be Written: ", filename)
            i += 1

            # Get response object for link
            response = requests.get(download_link)
            df = pd.read_csv(download_link)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df.rename(columns={'Date':'date'})
            df.rename(columns={'Hour (UTC)':'hours_utc'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df['donwnload_link'] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            df.to_sql(name='stg_monthly_air_data', con=sqlalchemy_engine, if_exists='append', schema='stage')
            # Write content in CSV file
            if save_locally == True:
                csv = open(filename, 'wb')
                csv.write(response.content)
                csv.close()
    print("All CSV files downloaded")

def load_monthly_forecasts(save_locally):
    # URL from which pdfs to be downloaded
    url = "https://dd.weather.gc.ca/air_quality/aqhi/ont/forecast/monthly/csv/"

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
        if ('.csv' in link.get('href', [])):
            download_link = url + link.get('href')
            print('Download Link: ', download_link)
            filename = parent_dir + '/Data/Forecasts_' + link.get('href')
            if save_locally == True:
                print("Filename to be Written: ", filename)
            i += 1

            # Get response object for link
            response = requests.get(download_link)
            df = pd.read_csv(download_link, parse_dates=True)
            df['validity date'] = pd.to_datetime(df['validity date']).dt.date
            df.rename(columns={'validity time (UTC)':'validity_time_utc', 'cgndb code':'cgndb_code', 'amended?':'amended','validity date':'validity_date','community name':'community_name'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df['donwnload_link'] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            df.to_sql(name='stg_monthly_forecasts', con=sqlalchemy_engine, if_exists='append', schema='stage')
            # Write content in CSV file
            if save_locally == True:
                csv = open(filename, 'wb')
                csv.write(response.content)
                csv.close()
    print("All CSV files downloaded")


def load_traffic_volumes(save_locally):
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)

    utils.install_packages('opendatatoronto')
    utils.install_packages('dplyr')
    ro.r('''
        library(opendatatoronto)
        library(dplyr)
        package <- show_package("traffic-volumes-at-intersections-for-all-modes")
        resources <- list_package_resources("traffic-volumes-at-intersections-for-all-modes")
        datastore_resources <- filter(resources, tolower(format) %in% c('csv'))
        df_r <- filter(datastore_resources, row_number()==1) %>% get_resource()
        print(summary(df_r))
        write.csv(df_r, '/Users/amr/PycharmProjects/ggr473/Data/traffic_volume.csv', row.names = FALSE)
        ''')
    df = pd.read_csv('/Users/amr/PycharmProjects/ggr473/Data/traffic_volume.csv', parse_dates=True)
    df['last_updated'] = datetime.datetime.now()
    df.to_sql(name='stg_traffic_volume', con=sqlalchemy_engine, if_exists='append', schema='stage')
    if save_locally == False:
        os.remove('/Users/amr/PycharmProjects/ggr473/Data/traffic_volume.csv')

# to execute loading the monthly data into staging layer
#load_monthly_data(save_locally=False)
## OR to save the CSV files locally to ./Data/XXXXX.CSV  ##
#load_and_ingest(save_locally=True)

# to execute loading the monthly forecasts into the staging layer
#load_monthly_forecasts(save_locally=False)
## OR to save the CSV files locally to ./Data/Forecasts_XXXXX.CSV  ##
#load_monthly_forecasts(save_locally=True)

# to execute loading the traffic volume dataset into the staging layer
load_traffic_volumes(save_locally=False)
## OR to save the CSV files locally to ./Data/traffic_volume.CSV  ##
#load_traffic_volumes(save_locally=True)




