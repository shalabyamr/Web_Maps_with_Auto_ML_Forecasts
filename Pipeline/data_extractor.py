import requests
import warnings
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

warnings.filterwarnings("ignore")


class GenericClass:
    pass


configs_obj = GenericClass()


def read_configs():
    start = datetime.datetime.now()
    print('*****************************\nReading Configuration File Started: {}'.format(start))
    # To write temp files into the Parent ./Data/ Folder
    # to keep the Pipeline folder clean of csv and temp files
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Saving Local Files and Parent Directory
    check_save_locally = str(config['save_files']['save_locally_flag']).title()
    if check_save_locally not in ['True', 'False']:
        raise Exception(
            "save_locally_flag in Config.ini is set as {}. Only True or False is accepted.".format(check_save_locally))
    del check_save_locally
    configs_obj.save_locally = eval(config['save_files']['save_locally_flag'])
    configs_obj.parent_dir = str(config['save_files']['parent_dir'])

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
        configs_obj.run_time_seconds = int(config['auto_ml']['run_time_seconds'])
    except Exception as e:
        print('Config.ini Error Reading H2O Runtime Settings: {}'.format(e))
        print("['auto_ml']['run_time_seconds'] Needs to be an Integer > 0 instead of {}.".format(
            config['auto_ml']['run_time_seconds']))
        sys.exit(1)

    try:
        configs_obj.forecast_horizon = int(config['auto_ml']['forecast_horizon'])
    except Exception as e:
        print('Config.ini Error Reading H2O Runtime Settings: {}'.format(e))
        print("['auto_ml']['forecast_horizon'] Needs to be an Integer > 0 instead of {}.".format(
            config['auto_ml']['forecast_horizon']))
        sys.exit(1)

    try:
        configs_obj.forecast_frequency = str(config['auto_ml']['forecast_frequency']).upper()
        if ('HOUR' in configs_obj.forecast_frequency) or ('HOURLY' in configs_obj.forecast_frequency):
            configs_obj.forecast_frequency = 'h'
            configs_obj.forecast_description = 'Hourly'
        if ('DAY' in configs_obj.forecast_frequency) or ('DAILY' in configs_obj.forecast_frequency):
            configs_obj.forecast_frequency = 'D'
            configs_obj.forecast_description = 'Daily'
        elif 'MONTH' in configs_obj.forecast_frequency:
            configs_obj.forecast_frequency = 'MS'
            configs_obj.forecast_description = 'Monthly'
        elif ('YEAR' in configs_obj.forecast_frequency) or ('ANNUAL' in configs_obj.forecast_frequency):
            configs_obj.forecast_frequency = 'YS'
            configs_obj.forecast_description = 'Yearly'
        elif 'QUARTER' in configs_obj.forecast_frequency:
            configs_obj.forecast_frequency = 'QS'
            configs_obj.forecast_description = 'Quarterly'
        else:
            raise Exception('Forecast Frequency needs to be Daily, Monthly, Yearly, or Annually instead of {}'.format(
                config['auto_ml']['forecast_frequency']))

    except Exception as e:
        print('Config.ini Error Reading H2O Runtime Settings: {}'.format(e))
        print(
            "['auto_ml']['forecast_frequency'] Needs to be Daily, Monthly, Yearly, or Quarterly instead of {}.".format(
                config['auto_ml']['forecast_frequency']))
        sys.exit(1)

    for platform, token in configs_obj.access_tokens.items(): print(f'Platform: {platform}: Token: {token}')
    print(f'Parent Directory: {configs_obj.parent_dir}')
    print(f'Save Locally Flag is set to: {configs_obj.save_locally}')
    print(f'AutoML Runtime is set to: {configs_obj.run_time_seconds} Seconds')
    print(f'AutoML Forecast Horizon is set to: {configs_obj.forecast_horizon}')
    print(f"AutoML Forecast Frequency is set to: '{configs_obj.forecast_frequency}' - {configs_obj.forecast_description}")
    end = datetime.datetime.now()
    read_configs_total_seconds = (end - start).total_seconds()
    print(
        '*****************************\nDone Reading Configuration File in: {} seconds'.format(
            read_configs_total_seconds), '\n*****************************')


def initialize_database():
    config = configparser.ConfigParser()
    config.read('config.ini')
    try:
        # Define Database Connector :
        print('Reading Database Configuration.')
        config.read('config.ini')
        configs_obj.host = config['postgres_db']['host']
        configs_obj.port = config['postgres_db']['port']
        configs_obj.dbname = config['postgres_db']['db_name']
        configs_obj.user = config['postgres_db']['user']
        configs_obj.password = config['postgres_db']['password']
        print('Database Configuration: Host: {}\nPort: {}\nDatabase Name: {}\nUser: {}\nPassword: {}'.format(
            configs_obj.host, configs_obj.port, configs_obj.dbname, configs_obj.user, configs_obj.password))
        configs_obj.pg_engine = pg.connect(host=configs_obj.host, port=configs_obj.port, dbname=configs_obj.dbname,
                                           user=configs_obj.user, password=configs_obj.password)
        # Connection String is of the form: ‘postgresql://username:password@databasehost:port/databasename’
        configs_obj.sqlalchemy_engine = sqlalchemy.create_engine(
            'postgresql://{}:{}@{}:{}/{}'.format(configs_obj.user, configs_obj.password, configs_obj.host,
                                                 configs_obj.port, configs_obj.dbname))
        cursor = configs_obj.pg_engine.cursor()
        try:
            stage_query = "CREATE SCHEMA IF NOT EXISTS stage;"
            cursor.execute(stage_query)
            configs_obj.pg_engine.commit()
            del stage_query
            print('*****************************\nDone Initializing Database and Created Schema Stage.\n*****************************')
        except BaseException as exception:
            print('Failed to create schema!', exception)
            sys.exit()
        return configs_obj.sqlalchemy_engine, configs_obj.pg_engine
    except BaseException as exception:
        print('Error thrown by initialize_database()!, {} '.format(exception))
        return exception


def extract_monthly_data(sqlalchemy_engine):
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
            filename = configs_obj.parent_dir + '/Data/' + link.get('href')
            if configs_obj.save_locally:
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
            df.to_sql(name='stg_monthly_air_data', con=configs_obj.sqlalchemy_engine, if_exists='append',
                      schema='stage', index_label=False, index=False)
            # Write content in CSV file
            if configs_obj.save_locally:
                if i == 1:
                    df.to_csv(configs_obj.parent_dir + '/Data/monthly_air_data.csv', index=False, index_label=False,
                              header=True)
                else:
                    df.to_csv(configs_obj.parent_dir + '/Data/monthly_air_data.csv', mode='a', index=False,
                              index_label=False, header=False)
    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("*****************************\n", 'Loaded Monthly Air Data Done in {} seconds.'.format(delta_seconds),
          "\n*****************************\n")
    return 'extract_monthly_data', delta_seconds, a, b, i


def extract_monthly_forecasts(sqlalchemy_engine):
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
            filename = configs_obj.parent_dir + '/Data/Forecast_' + link.get('href')
            if configs_obj.save_locally:
                print("Filename to be Written: ", filename)
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
            df.to_sql(name='stg_monthly_forecasts', con=configs_obj.sqlalchemy_engine, if_exists='append',
                      schema='stage', index_label=False, index=False)
            # Write ALL Forecasts into one file
            if configs_obj.save_locally:
                if i == 1:
                    df.to_csv(configs_obj.parent_dir + '/Data/monthly_forecasts.csv', index=False, index_label=False,
                              header=True)
                else:
                    df.to_csv(configs_obj.parent_dir + '/Data/monthly_forecasts.csv', mode='a', index=False,
                              index_label=False, header=False)
    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("********************************\n", 'Loaded Daily Forecasts Done in {} Seconds.'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_monthly_forecasts', delta_seconds, a, b, i


def extract_traffic_volumes():
    a = datetime.datetime.now()
    print('Loading Traffic Data as of: {}'.format(a))
    download_link = 'https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/'
    filename = configs_obj.parent_dir + '/Data/' + 'traffic_volume.csv'
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
        write.csv(df_r, '{}traffic_volume.csv', row.names = FALSE)""".format(configs_obj.parent_dir + '/Data/'))
    df = pd.read_csv(configs_obj.parent_dir + '/Data/' + 'traffic_volume.csv', parse_dates=True)
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_traffic_volume', con=configs_obj.sqlalchemy_engine, if_exists='replace', schema='stage',
              index_label=False, index=False)
    if not configs_obj.save_locally:
        os.remove(configs_obj.parent_dir + '/Data/' + 'traffic_volume.csv')

    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("********************************\n",
          'Loaded Toronto Traffic Volume Done in {} Seconds'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_traffic_volumes', delta_seconds, a, b, 1


def extract_geo_names_data(sqlalchemy_engine):
    a = datetime.datetime.now()
    print('Downloading Geographical Names Data as of: ', a)
    # URL from which pdfs to be downloaded
    download_link = "https://ftp.cartes.canada.ca/pub/nrcan_rncan/vector/geobase_cgn_toponyme/prov_csv_eng/cgn_canada_csv_eng.zip"
    csv_filename = configs_obj.parent_dir + '/Data/' + 'cgn_canada_csv_eng.csv'
    zip_filename = configs_obj.parent_dir + '/Data/' + 'cgn_canada_csv_eng.zip'
    wget.download(url=download_link, out=configs_obj.parent_dir + '/Data/')
    print('Unzipping File: {} to: {}'.format(zip_filename, configs_obj.parent_dir + '/Data/'))
    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
        zip_ref.extractall(path=configs_obj.parent_dir + '/Data/')
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
    df.to_sql(name='stg_geo_names', con=configs_obj.sqlalchemy_engine, if_exists='append', schema='stage',
              index_label=False, index=False)
    if not configs_obj.save_locally:
        os.remove(csv_filename)

    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print("********************************\n", 'Loaded Geo Data Names Done in {} Seconds'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_geo_names_data', delta_seconds, a, b, 2


def extract_gta_traffic_arcgis(sqlalchemy_engine):
    a = datetime.datetime.now()
    print('Loading ArcGIS Traffic from ArcGIS as of: ', a)
    download_link = 'https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da'
    filename = configs_obj.parent_dir + '/Data/' + 'ArcGIS_Toronto_and_Peel_Traffic.txt'
    df = pd.read_csv(filename, sep=',', parse_dates=True)
    df.columns = map(str.lower, df.columns)
    df['activation_date'] = pd.to_datetime(df['activation_date']).dt.date
    df['count_date'] = pd.to_datetime(df['count_date']).dt.date
    df['last_updated'] = datetime.datetime.now()
    df['download_link'] = download_link
    df['src_filename'] = filename
    df.to_sql(name='stg_gta_traffic_arcgis', con=configs_obj.sqlalchemy_engine, if_exists='append', schema='stage',
              index_label=False, index=False)
    if configs_obj.save_locally:
        df.to_csv(configs_obj.parent_dir + '/Data/' + 'ArcGIS_Toronto_and_Peel_Traffic.csv', index=False,
                  index_label=False)

    b = datetime.datetime.now()
    delta_seconds = (b - a).total_seconds()
    print('Loaded ArcGIS Toronto and Peel Traffic Count Done in {} Seconds'.format(delta_seconds),
          "\n********************************\n")
    return 'extract_gta_traffic_arcgis', delta_seconds, a, b, 1
