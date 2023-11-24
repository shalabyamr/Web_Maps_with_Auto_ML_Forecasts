import warnings
import pandas as pd
import psycopg2 as pg
import os
import glob
import sqlalchemy
from sqlalchemy import text
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
cur = conn.cursor()

sqlalchemy_engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port,dbname))
sa_connect = sqlalchemy_engine.connect()

sql_files = glob.glob(parent_dir+'/SQL/*')
print("SQL Queries to execute: ", sql_files)

for file in sql_files:
    print('Processing Query File: ', file)
    query = str(open(file).read())
    cur.execute(query)
    conn.commit()
    print('Done Creating Table for the Query: ', file)
