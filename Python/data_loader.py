import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import glob
import datetime
from data_extractor import (save_locally_flag, initialize_database, parent_dir, extract_monthly_data, extract_monthly_forecasts, extract_traffic_volumes, extract_geo_names_data, extract_gta_traffic_arcgis)
from data_transformation import transform_monthly_data


def create_staging_tables(save_locally):
    master_list = []
    # to execute loading the monthly data into staging layer
    monthly_date_step = extract_monthly_data(save_locally=save_locally_flag)
    master_list.append(monthly_date_step)

    # to execute loading the monthly forecasts into the staging layer
    monthly_forecasts_step = extract_monthly_forecasts(save_locally=save_locally_flag)
    master_list.append(monthly_forecasts_step)

    # to execute loading the traffic volume dataset into the staging layer
    traffic_volume_step = extract_traffic_volumes(save_locally=save_locally_flag)
    master_list.append(traffic_volume_step)

    # to execute loading the geographical database names  into the staging layer
    geo_names_step = extract_geo_names_data(save_locally=save_locally_flag)
    master_list.append(geo_names_step)


    # to execute loading the loading ArcGIS Toronto and Peel Traffic into the staging layer
    traffic_arcgis_step = extract_gta_traffic_arcgis(save_locally=save_locally_flag)
    master_list.append(traffic_arcgis_step)

    # Transposes monthly Air Data from Column Names to Rows
    transform_monthly_step = transform_monthly_data(save_locally=save_locally_flag)
    master_list.append(transform_monthly_step)
    return master_list

def create_production_tables():
    a1 = datetime.datetime.now()
    master_list = []
    sql_files = glob.glob(parent_dir + '/SQL/*.sql')

    # Combine_Air_Data Table needs to be created after all staging tables
    for i in sql_files:
        if 'combine_air_data.sql' in i:
            sql_files.remove(i)
        sql_files.append(i)

    print("SQL Queries to execute: ", sql_files)
    conn = initialize_database()[1]
    cur = conn.cursor()
    for file in sql_files:
        a = datetime.datetime.now()

        if 'combine_air_data.sql' not in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            conn.commit()
            print('Executed Query: ', file)

        elif 'combine_air_data.sql' in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            conn.commit()
            print('Executed Query: ', file)

        b = datetime.datetime.now()
        delta_seconds = (b - a).total_seconds()
        master_list.append([file, delta_seconds, a, b, 1])

    b1 = datetime.datetime.now()
    delta_seconds_1 = (b1-a1).total_seconds()
    master_list.append(['create_production_tables', a1, b1, len(sql_files)])
    print('Done Creating ALL Production Tables in {} seconds as of: {}'.format(delta_seconds_1, b1))
    return master_list