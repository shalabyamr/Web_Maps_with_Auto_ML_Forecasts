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
import zipfile
import wget
warnings.filterwarnings("ignore")

# To write temp files into the Parent ./Data/ Folder to
# to keep the Python folder clean of csv and temp files
def get_parent_dir(directory):
    return os.path.dirname(directory)

current_dirs_parent = get_parent_dir(os.getcwd())
parent_dir = os.path.split(os.getcwd())[0]
print('Working Directory: ', current_dirs_parent)
print('Parent Directory: ', parent_dir, '\n*****************************\n')

# Define Database Connector :
host = 'localhost'
port = '5433'
dbname = 'postgres'
user = 'postgres'
password = 'postgres'

pg_engine = pg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
# Connection String is of the form: ‘postgresql://username:password@databasehost:port/databasename’
sqlalchemy_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port,dbname))


def extract_monthly_data(save_locally):
    a = datetime.datetime.now()
    # URL from which pdfs to be downloaded
    print('Started Downloading Monthly Data as of {}'.format(a))
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
            df.rename(columns={'Date':'the_date', 'Hour (UTC)':'hours_utc'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df['download_link'] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            df.to_sql(name='stg_monthly_air_data', con=sqlalchemy_engine, if_exists='append', schema='stage', index_label=False, index=False)
            # Write content in CSV file
            if save_locally == True:
                csv = open(filename, 'wb')
                csv.write(response.content)
                csv.close()

    # Transposing the Monthly Air Quality Data #
    print('*** Transposing the Monthly Air Quality Data ***')
    df = pd.read_sql_table(table_name='stg_monthly_air_data', con=sqlalchemy_engine, schema='stage', parse_dates=True)
    df_columns = ['FAFFD', 'FALIF', 'FALJI', 'FAMXK', 'FAYJG',
                  'FAZKI', 'FBKKK', 'FBLJL', 'FBLKS', 'FCAEN', 'FCCOT', 'FCFUU', 'FCGKZ',
                  'FCIBD', 'FCKTB', 'FCNJT', 'FCTOV', 'FCWFX', 'FCWOV', 'FCWYG', 'FDATE',
                  'FDCHU', 'FDEGT', 'FDGED', 'FDGEJ', 'FDGEM', 'FDJFN', 'FDMOP', 'FDQBU',
                  'FDQBX', 'FDSUS', 'FDZCP', 'FEAKO', 'FEARV', 'FEBWC', 'FEUTC', 'FEUZB',
                  'FEVJR', 'FEVJS', 'FEVJV', 'FEVNS', 'FEVNT']
    df_out = pd.DataFrame()
    for column in df_columns:
        print('Transposing Column: ', column)
        df2 = pd.DataFrame()
        df2['the_date'] = df['the_date'].dt.date
        df2['hours_utc'] = df['hours_utc']
        df2['cgndb_id'] = str(column)
        df2['air_quality_value'] = df[column]
        df2['download_link'] = df['download_link']
        df2['src_filename'] = df['src_filename']
        df2['last_updated'] = df['last_updated']
        df_out = pd.concat([df_out, df2])
    df_out.to_sql(name='stg_monthly_air_data_transpose', con=sqlalchemy_engine, if_exists='replace', schema='stage', index_label=False, index=False)

    if save_locally == True:
        transposed_filename = parent_dir+'/Data/'+'monthly_air_data_transposed.csv'
        print('Saving Transposed Monthly Air Data to: ', transposed_filename)
        df_out.to_csv(transposed_filename, index_label=False, index=False)

    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("********************************\n",'Loaded Monthly Air Data Done in {} seconds.'.format(delta_seconds), "\n********************************\n")

def extract_monthly_forecasts(save_locally):
    a = datetime.datetime.now()
    print('Loading Monthly Forecasts as of: {}'.format(a))
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
            filename = parent_dir + '/Data/Forecast_' + link.get('href')
            if save_locally == True:
                print("Filename to be Written: ", filename)
            i += 1

            # Get response object for link
            response = requests.get(download_link)
            df = pd.read_csv(download_link, parse_dates=True)
            df['validity date'] = pd.to_datetime(df['validity date']).dt.date
            df.rename(columns={'validity time (UTC)':'validity_time_utc', 'cgndb code':'cgndb_code', 'amended?':'amended','validity date':'validity_date','community name':'community_name'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df['download_link'] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            df.to_sql(name='stg_monthly_forecasts', con=sqlalchemy_engine, if_exists='append', schema='stage', index_label=False, index=False)
            # Write content in CSV file
            if save_locally == True:
                csv = open(filename, 'wb')
                csv.write(response.content)
                csv.close()
    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("********************************\n",'Loaded Daily Forecasts Done in {} Seconds.'.format(delta_seconds),"\n********************************\n")


def extract_traffic_volumes(save_locally):
    a = datetime.datetime.now()
    print('Loading Traffic Data as of: {}'.format(a))
    download_link = 'https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/'
    filename = parent_dir + '/Data/' + 'traffic_volume.csv'
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
        write.csv(df_r, '{}traffic_volume.csv', row.names = FALSE)
        '''.format(parent_dir + '/Data/'))
    df = pd.read_csv(parent_dir + '/Data/' + 'traffic_volume.csv', parse_dates=True)
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_traffic_volume', con=sqlalchemy_engine, if_exists='append', schema='stage', index_label=False, index=False)
    if save_locally == False:
        os.remove(parent_dir + '/Data/'+'traffic_volume.csv')

    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("********************************\n",'Loaded Toronto Traffic Volume Done in {} Seconds'.format(delta_seconds),"\n********************************\n")


def extract_geo_names_data(save_locally):
    a = datetime.datetime.now()
    print('Downloading Geographical Names Data as of: ', a)
    # URL from which pdfs to be downloaded
    download_link = "https://ftp.cartes.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_canada_csv_eng.zip"
    csv_filename = parent_dir+'/Data/'+'cgn_canada_csv_eng.csv'
    zip_filename = parent_dir+'/Data/'+'cgn_canada_csv_eng.zip'
    wget.download(url=download_link, out=parent_dir + '/Data/')
    print('Unzipping File: {} to: {}'.format(zip_filename, parent_dir+'/Data/'))
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(path=parent_dir+'/Data/')
    # delete the downloaded Zip File
    os.remove(zip_filename)

    df = pd.read_csv(csv_filename, parse_dates=True)
    df.columns = map(str.lower, df.columns)
    df.columns = df.columns.str.replace(' ', '_')
    df.rename(columns={'province_-_territory':'province_territory'}, inplace=True)
    df['decision_date'] = pd.to_datetime(df['decision_date']).dt.date
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = csv_filename
    df.to_sql(name='stg_geo_names', con=sqlalchemy_engine, if_exists='append', schema='stage', index_label=False, index=False)
    if save_locally != True:
        os.remove(csv_filename)

    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("********************************\n",'Loaded Geo Data Names Done in {} Seconds'.format(delta_seconds),"\n********************************\n")


def extract_gta_traffic_arcgis(save_locally):
    a = datetime.datetime.now()
    print('Loading ArcGIS Traffic from ArcGIS as of: ',a)
    download_link = 'https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da'
    filename = parent_dir+'/Data/'+'ArcGIS_Toronto_and_Peel_Traffic.txt'
    df = pd.read_csv(filename, sep=',', parse_dates=True)
    df.columns = map(str.lower, df.columns)
    df['activation_date'] = pd.to_datetime(df['activation_date']).dt.date
    df['count_date'] = pd.to_datetime(df['count_date']).dt.date
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_gta_traffic_arcgis', con=sqlalchemy_engine, if_exists='append', schema='stage', index_label=False, index=False)

    if save_locally == True:
        df.to_csv(parent_dir+'/Data/'+'ArcGIS_Toronto_and_Peel_Traffic.csv', index=False, index_label=False)

    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print('Loaded ArcGIS Toronto and Peel Traffic Count Done in {} Seconds'.format(delta_seconds),"\n********************************\n")


# to execute loading the monthly data into staging layer
extract_monthly_data(save_locally=False)
## OR to save the CSV files locally to ./Data/XXXXX.CSV  ##
#extract_and_ingest(save_locally=True)

# to execute loading the monthly forecasts into the staging layer
extract_monthly_forecasts(save_locally=False)
## OR to save the CSV files locally to ./Data/Forecasts_XXXXX.CSV  ##
#extract_monthly_forecasts(save_locally=True)

# to execute loading the traffic volume dataset into the staging layer
extract_traffic_volumes(save_locally=False)
## OR to save the CSV files locally to ./Data/traffic_volume.CSV  ##
#extract_traffic_volumes(save_locally=True)

# to execute loading the geographilca database names  into the staging layer
extract_geo_names_data(save_locally=False)
## OR to save the CSV files locally to ./Data/cgn_canada_csv_eng.csv  ##
#extract_geo_names_data(save_locally=True)

# to execute loading the loading ArcGIS Toronto and Peel Traffic into the staging layer
extract_gta_traffic_arcgis(save_locally=False)
## OR to save the CSV files locally to ./Data/ArcGIS_Toronto_and_Peel_Traffic.csv  ##
#extract_gta_traffic_arcgis(save_locally=True)