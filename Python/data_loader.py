import warnings

import pandas as pd
import psycopg2 as pg
import os
import sqlalchemy
from pandasql import sqldf

# from data_extractor import load_monthly_data, load_monthly_forecasts, load_traffic_volumes, load_geo_names_data, load_gta_traffic_arcgis
warnings.filterwarnings("ignore")


def get_parent_dir(directory):
    return os.path.dirname(directory)


current_dirs_parent = get_parent_dir(os.getcwd())
parent_dir = os.path.split(os.getcwd())[0]
print('Working Directory: ', current_dirs_parent)
print('Parent Directory: ', parent_dir, '\n*****************************\n')

host = 'localhost'
port = '5433'
dbname = 'postgres'
user = 'postgres'
password = 'postgres'

conn = pg.connect(host=host, port=port, dbname=dbname, user=user, password=password)
cursor = conn.cursor()

sqlalchemy_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port,dbname))
sa_connect = sqlalchemy_engine.connect()
