import requests
import pandas as pd
import datetime
import psycopg2 as pg
import sqlalchemy
import os
import zipfile
import wget
import configparser
import sys
from rpy2 import robjects as ro
from bs4 import BeautifulSoup
import rpy2.robjects.packages as rpackages
from google_drive_downloader import GoogleDriveDownloader as gdd
import warnings
warnings.filterwarnings("ignore")

# Generic Class is used to instantiate the Configurations Object.
class GenericClass:
    def __init__(self):
        self.auto_ml = {}
        self.database = {}
        self.run_conditions = {}
        self.access_tokens = {}
        self.parent_dir = str
    pass


configs_obj = GenericClass()

# Reads the  config.ini file and stores the settings in configurations object
def read_configs():
    start = datetime.datetime.now()
    print('*****************************\nReading Configuration File Started: {}'.format(start))
    # To write temp files into the Parent ./Data/ Folder
    # to keep the Pipeline folder clean of csv and temp files
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Saving Local Files and Parent Directory
    check_save_locally = str(config['run_conditions']['save_locally']).title()
    if check_save_locally not in ['True', 'False']:
        raise Exception(
            f"save_locally_flag in Config.ini is set as {check_save_locally}. Only True or False is accepted.")
    del check_save_locally
    configs_obj.run_conditions['save_locally'] = eval(config['run_conditions']['save_locally'])
    configs_obj.run_conditions['parent_dir'] = os.path.abspath(os.path.join(os.getcwd(), os.pardir))

    # Access Tokens needed for Mapbox. Here Access Tokens are a dictionary to allow for more
    # Tokens to be added later with better readability. It is stored as
    # " Platform : Token "
    try:
        mapbox_access_token = str(config['api_tokens']['mapbox'])
        configs_obj.access_tokens = {'mapbox': mapbox_access_token}
    except Exception as e:
        print('Error! Config.ini does not contain Mapbox Access Token!')
        sys.exit(1)

    # AutoML Configurations setting Machine Learning Duration (Seconds), Forecast Horizon, and Forecast Frequency
    # that can be hourly, daily, monthly, quarterly, or yearly.
    try:
        configs_obj.auto_ml['run_time_seconds'] = int(config['auto_ml']['run_time_seconds'])
        if configs_obj.auto_ml['run_time_seconds'] < 0:
            print(f"Error! Config.ini run_time_seconds needs to be >= 0. The value given is {config['auto_ml']['run_time_seconds']}")
            SystemExit(1)
    except Exception as e:
        print(f"Config.ini Error Reading H2O Runtime Settings: {e}")
        print(f"['auto_ml']['run_time_seconds'] Needs to be an Integer >= 0 instead of {config['auto_ml']['run_time_seconds']}.")
        sys.exit(1)
    try:
        configs_obj.auto_ml['forecast_horizon'] = int(config['auto_ml']['forecast_horizon'])
    except Exception as e:
        print('Config.ini Error Reading H2O Runtime Settings: {}'.format(e))
        print(f"['auto_ml']['forecast_horizon'] Needs to be an Integer > 0 instead of {config['auto_ml']['forecast_horizon']}.")
        sys.exit(1)
    try:
        configs_obj.auto_ml['forecast_frequency'] = str(config['auto_ml']['forecast_frequency']).upper()
        if ('HOUR' in configs_obj.auto_ml['forecast_frequency']) or ('HOURLY' in configs_obj.auto_ml['forecast_frequency']):
            configs_obj.auto_ml['forecast_frequency'] = 'h'
            configs_obj.auto_ml['forecast_description'] = 'Hourly'
        if ('DAY' in configs_obj.auto_ml['forecast_frequency']) or ('DAILY' in configs_obj.auto_ml['forecast_frequency']):
            configs_obj.auto_ml['forecast_frequency'] = 'D'
            configs_obj.auto_ml['forecast_description'] = 'Daily'
        elif 'MONTH' in configs_obj.auto_ml['forecast_frequency']:
            configs_obj.auto_ml['forecast_frequency'] = 'MS'
            configs_obj.auto_ml['forecast_description'] = 'Monthly'
        elif ('YEAR' in configs_obj.auto_ml['forecast_frequency']) or ('ANNUAL' in configs_obj.auto_ml['forecast_frequency']):
            configs_obj.auto_ml['forecast_frequency'] = 'YS'
            configs_obj.auto_ml['forecast_description'] = 'Yearly'
        elif 'QUARTER' in configs_obj.auto_ml['forecast_frequency']:
            configs_obj.auto_ml['forecast_frequency'] = 'QS'
            configs_obj.auto_ml['forecast_description'] = 'Quarterly'
        else:
            raise Exception('Forecast Frequency needs to be Daily, Monthly, Yearly, or Annually instead of {}'.format(
                config['auto_ml']['forecast_frequency']))

    except Exception as e:
        print(f"Config.ini Error Reading H2O Runtime Settings: {e}")
        print(
            f"['auto_ml']['forecast_frequency'] Needs to be Daily, Monthly, Yearly, or Quarterly instead of {config['auto_ml']['forecast_frequency']}.")
        sys.exit(1)

    for platform, token in configs_obj.access_tokens.items(): print(f"Platform: {platform}: Token: {token}")
    print(f"Parent Directory: {configs_obj.run_conditions['parent_dir']}")
    print(f"Save Locally Flag is set to: {configs_obj.run_conditions['save_locally']}")
    print(f"AutoML Runtime is set to: {configs_obj.auto_ml['run_time_seconds']} Seconds")
    print(f"AutoML Forecast Horizon is set to: {configs_obj.auto_ml['forecast_horizon']}")
    print(f"AutoML Forecast Frequency is set to: '{configs_obj.auto_ml['forecast_frequency']}' - {configs_obj.auto_ml['forecast_description']}")

    # Run Conditions to be used in Maion() to execute the program.
    try:
        configs_obj.run_conditions['create_tables'] = eval(config['run_conditions']['create_tables'].title())
    except Exception as e:
        print(f"Config.ini: Error Runtime Condition create_tables: {e}")
        print(f"Config.ini: create_tables needs to be a boolean instead of : {config['run_conditions']['create_tables']}")
        sys.exit(1)
    try:
        configs_obj.run_conditions['run_auto_ml'] = eval(config['run_conditions']['run_auto_ml'].title())
    except Exception as e:
        print(f"Config.ini: Error Runtime Condition run_auto_ml: {e}")
        print(f"Config.ini: run_auto_ml needs to be a boolean instead of : {config['run_conditions']['run_auto_ml']}")
        sys.exit(1)
    try:
        configs_obj.run_conditions['show_maps'] = eval(config['run_conditions']['show_maps'].title())
    except Exception as e:
        print(f"Config.ini: Error Runtime Condition show_maps: {e}")
        print(f"Config.ini: show_maps needs to be a boolean instead of : {config['run_conditions']['show_maps']}")
        sys.exit(1)
    try:
        configs_obj.run_conditions['map_types'] = list(config['run_conditions']['map_types'].replace(' ','').split(','))
    except Exception as e:
        print(f"Config.ini: Error Runtime Condition map_types: {e}")
        print(f"Config.ini: map_types needs to be a list instead of : {config['run_conditions']['map_types']}")
        sys.exit(1)

    print(f"Done Configuring Run Conditions with create_tables: '{configs_obj.run_conditions['create_tables']}' - "
          f"run_auto_ml:  {configs_obj.run_conditions['run_auto_ml']} - "
          f"map_types: {configs_obj.run_conditions['map_types']} - show_maps: {configs_obj.run_conditions['show_maps']}")

    # End of Configuration
    end = datetime.datetime.now()
    read_configs_total_seconds = (end - start).total_seconds()
    print(
        f"*****************************\nDone Reading Configuration File in: {read_configs_total_seconds} Seconds.\n*****************************")
    return configs_obj

# Creates the database connectors and stores the needed engines
# this step drastically reduced execution time as the database connection is
# initialized only once.
def initialize_database():
    config = configparser.ConfigParser()
    config.read('config.ini')
    try:
        # Define Database Connector :
        print('Reading Database Configuration.')
        config.read('config.ini')
        configs_obj.database['host'] = config['postgres_db']['host']
        configs_obj.database['port'] = config['postgres_db']['port']
        configs_obj.database['dbname'] = config['postgres_db']['db_name']
        configs_obj.database['user'] = config['postgres_db']['user']
        configs_obj.database['password'] = config['postgres_db']['password']
        print(
            f"""Database Configuration: Host: {configs_obj.database['host']}\nPort: {configs_obj.database['port']}\nDatabase Name: {configs_obj.database['dbname']}\nUser: {configs_obj.database['user']}\nPassword: {configs_obj.database['password']}""")
        configs_obj.database['pg_engine'] = pg.connect(host=configs_obj.database['host'], port=configs_obj.database['port']
                                           , dbname=configs_obj.database['dbname']
                                           , user=configs_obj.database['user'], password=configs_obj.database['password'])
        # Connection String is of the form: ‘postgresql://username:password@databasehost:port/databasename’
        configs_obj.database['sqlalchemy_engine'] = sqlalchemy.create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(configs_obj.database['user'], configs_obj.database['password'], configs_obj.database['host'],
                                                 configs_obj.database['port'], configs_obj.database['dbname']))
        cursor = configs_obj.database['pg_engine'].cursor()
        try:
            stage_query = """CREATE SCHEMA IF NOT EXISTS stage; 
               CREATE SCHEMA IF NOT EXISTS public; 
               CREATE EXTENSION IF NOT EXISTS postgis;"""
            cursor.execute(stage_query)
            configs_obj.database['pg_engine'].commit()
            del stage_query
            print('*****************************\nDone. Initialized Database and Created Publilc and Stage Schemas. Installed PostGIS Extension.\n*****************************')
        except Exception as exception:
            print('Failed to create schema!', exception)
            sys.exit(1)
        return configs_obj
    except Exception as exception:
        print(f"Error thrown by initialize_database()!, {exception}")
        sys.exit(1)


# Reads the monthly Air Quality Data from Government of Canada.
def extract_monthly_data(sqlalchemy_engine):
    a = datetime.datetime.now()
    # URL from which pdfs to be downloaded
    print(f"Started Downloading Monthly Data as of {a}")
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
            filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + link.get('href')
            if configs_obj.run_conditions['save_locally']:
                print("Filename to be Written: ", filename)
            i += 1
            # Get response object for link
            response = requests.get(download_link)
            df = pd.read_csv(download_link)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df.rename(columns={'Date': 'the_date', 'Hour (UTC)': 'hours_utc'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df['download_link'] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            if i == 1:
                df.to_sql(name='stg_monthly_air_data', con=configs_obj.database['sqlalchemy_engine'], if_exists='replace',
                      schema='stage', index_label=False, index=False)

            df.to_sql(name='stg_monthly_air_data', con=configs_obj.database['sqlalchemy_engine'], if_exists='append',
                      schema='stage', index_label=False, index=False)

            # Write content in CSV file
            if configs_obj.run_conditions['save_locally']:
                if i == 1:
                    df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/monthly_air_data.csv', index=False, index_label=False,
                              header=True)
                else:
                    df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/monthly_air_data.csv', mode='a', index=False,
                              index_label=False, header=False)
    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("*****************************\n", 'Loaded Monthly Air Data Done in {} seconds.'.format(delta_seconds),
          "\n*****************************\n")
    return 'extract_monthly_data', delta_seconds, a, b, i

# This is the official one-year weather forecast.  However, there is no reported
# prediction error associated with the forecasts.
def extract_monthly_forecasts(configs_obj):
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
            filename = configs_obj.run_conditions['parent_dir'] + '/Data/Forecast_' + link.get('href')
            if configs_obj.run_conditions['save_locally']:
                print("Filename to be Appended to: ", configs_obj.run_conditions['parent_dir'] + '/Data/monthly_forecasts.csv')
            i += 1
            # Get response object for link
            response = requests.get(download_link)
            df = pd.read_csv(download_link, parse_dates=True)
            df['validity date'] = pd.to_datetime(df['validity date']).dt.date
            df.rename(
                columns={'validity time (UTC)': 'validity_time_utc', 'cgndb code': 'cgndb_code', 'amended?': 'amended',
                         'validity date': 'validity_date', 'community name': 'community_name'}, inplace=True)
            df['last_updated'] = datetime.datetime.now()
            df["download_link"] = download_link
            df['src_filename'] = filename
            print('Run: , ', i, 'Inserting File: ', filename, 'Into Database.')
            if i == 1:
                df.to_sql(name='stg_monthly_forecasts', con=configs_obj.database['sqlalchemy_engine'],
                          if_exists='replace',
                          schema='stage', index_label=False, index=False)

            df.to_sql(name='stg_monthly_forecasts', con=configs_obj.database['sqlalchemy_engine'], if_exists='append',
                      schema='stage', index_label=False, index=False)
            df.to_sql(name='stg_monthly_forecasts', con=configs_obj.database['sqlalchemy_engine'], if_exists='append',
                      schema='stage', index_label=False, index=False)
            # Write ALL Forecasts into one file
            if configs_obj.run_conditions['save_locally']:
                if i == 1:
                    df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/monthly_forecasts.csv', index=False, index_label=False,
                              header=True)
                else:
                    df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/monthly_forecasts.csv', mode='a', index=False,
                              index_label=False, header=False)
    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("********************************\n", 'Loaded Daily Forecasts Done in {} Seconds.'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_monthly_forecasts', delta_seconds, a, b, i

# Extracts the Traffic Data from Toronto Open Data Portal.  Due to issues with the official
# Python Connector, this spins up an R-thread to extract the traffic data from OpenDataToronot R Library.
def extract_traffic_volume(configs_obj):
    a = datetime.datetime.now()
    print('Loading Traffic Data as of: {}'.format(a))
    download_link = 'https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/'
    filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + 'traffic_volume.csv'
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    utils.install_packages('opendatatoronto')
    utils.install_packages('dplyr')
    ro.r("""
        library(opendatatoronto)
        library(dplyr)
        package = show_package("traffic-volumes-at-intersections-for-all-modes")
        resources = list_package_resources("traffic-volumes-at-intersections-for-all-modes")
        datastore_resources = filter(resources, tolower(format) %in% c('csv'))
        df_r = filter(datastore_resources, row_number()==1) %>% get_resource()
        write.csv(df_r, '{}traffic_volume.csv', row.names = FALSE)""".format(configs_obj.run_conditions['parent_dir'] + '/Data/'))
    df = pd.read_csv(configs_obj.run_conditions['parent_dir'] + '/Data/' + 'traffic_volume.csv', parse_dates=True)
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_traffic_volume', con=configs_obj.database['sqlalchemy_engine'], if_exists='replace', schema='stage',
              index_label=False, index=False)
    if not configs_obj.run_conditions['save_locally']:
        os.remove(configs_obj.run_conditions['parent_dir'] + '/Data/' + 'traffic_volume.csv')

    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print(f"********************************\n'Loaded Toronto Traffic Volume Done in {delta_seconds} Seconds.\n********************************\n")
    return 'extract_traffic_volume', delta_seconds, a, b, 1

# Prior Government of Canada Weather Source does not provide any information
# on the weather stations coordinates, locations, activation, or decommission dates.
def extract_geo_names_data(configs_obj):
    a = datetime.datetime.now()
    print(f"Downloading Geographical Names Data as of: {a}")
    # URL from which pdfs to be downloaded
    download_link = "https://ftp.cartes.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_canada_csv_eng.zip"
    csv_filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + 'cgn_canada_csv_eng.csv'
    zip_filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + 'cgn_canada_csv_eng.zip'
    wget.download(url=download_link, out=configs_obj.run_conditions['parent_dir'] + '/Data/')
    print('Unzipping File: {} to: {}'.format(zip_filename, configs_obj.run_conditions['parent_dir'] + '/Data/'))
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(path=configs_obj.run_conditions['parent_dir'] + '/Data/')
    # delete the downloaded Zip File
    os.remove(zip_filename)

    df = pd.read_csv(csv_filename, parse_dates=True)
    df.columns = map(str.lower, df.columns)
    df.columns = df.columns.str.replace(' ', '_')
    df.rename(columns={'province_-_territory': 'province_territory'}, inplace=True)
    df['decision_date'] = pd.to_datetime(df['decision_date']).dt.date
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = csv_filename
    df.to_sql(name='stg_geo_names', con=configs_obj.database['sqlalchemy_engine'], if_exists='replace', schema='stage',
              index_label=False, index=False)
    if not configs_obj.run_conditions['save_locally']:
        os.remove(csv_filename)

    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("********************************\n", 'Loaded Geo Data Names Done in {} Seconds'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_geo_names_data', delta_seconds, a, b, 2


def extract_gta_traffic_arcgis(configs_obj):
    a = datetime.datetime.now()
    print('Loading ArcGIS Traffic from ArcGIS as of: ', a)
    filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + 'ArcGIS_Toronto_and_Peel_Traffic.txt'
    download_link = 'https://drive.google.com/file/d/1hKbdt9d92B_U-tPGkjnbJlb88L59YS3a/view?usp=drive_link'
    gdd.download_file_from_google_drive(file_id='1knjCNxRDIXqqF1gq9TB0yjBNP75BFQK9',
                                        dest_path=filename,
                                        unzip=False, showsize=True, overwrite=True)
    df = pd.read_csv(filename, sep=',', parse_dates=True)
    df.columns = map(str.lower, df.columns)
    df['activation_date'] = pd.to_datetime(df['activation_date']).dt.date
    df['count_date'] = pd.to_datetime(df['count_date']).dt.date
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_gta_traffic_arcgis', con=configs_obj.database['sqlalchemy_engine'], if_exists='replace', schema='stage',
              index_label=False, index=False)
    if configs_obj.run_conditions['save_locally']:
        df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/' + 'ArcGIS_Toronto_and_Peel_Traffic.csv', index=False,
                  index_label=False)
    os.remove(filename)
    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print(f"Loaded ArcGIS Toronto and Peel Traffic Count Done in {delta_seconds} Seconds.\n********************************\n")
    return 'extract_gta_traffic_arcgis', delta_seconds, a, b, 1
