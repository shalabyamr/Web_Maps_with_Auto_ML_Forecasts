import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import glob
import datetime
import data_extractor as data_extractor
from data_transformation import transform_monthly_data


def create_staging_tables():
    master_list = []
    # to execute loading the monthly data into staging layer
    monthly_date_step = data_extractor.extract_monthly_data(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(monthly_date_step)

    # to execute loading the monthly forecasts into the staging layer
    monthly_forecasts_step = data_extractor.extract_monthly_forecasts(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(monthly_forecasts_step)

    # to execute loading the traffic volume dataset into the staging layer
    traffic_volume_step = data_extractor.extract_traffic_volumes(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(traffic_volume_step)

    # to execute loading the geographical database names  into the staging layer
    geo_names_step = data_extractor.extract_geo_names_data(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(geo_names_step)


    # to execute loading the loading ArcGIS Toronto and Peel Traffic into the staging layer
    traffic_arcgis_step = data_extractor.extract_gta_traffic_arcgis(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(traffic_arcgis_step)

    # Transposes monthly Air Data from Column Names to Rows
    transform_monthly_step = transform_monthly_data(sqlalchemy_engine=data_extractor.sqlalchemy_engine)
    master_list.append(transform_monthly_step)
    return master_list

def create_production_tables():
    a1 = datetime.datetime.now()
    master_list = []
    sql_files = glob.glob(data_extractor.parent_dir + '/SQL/*.sql')

    # Combine_Air_Data Table needs to be created after all staging tables
    for i in sql_files:
        if 'combine_air_data.sql' in i:
            sql_files.remove(i)
        sql_files.append(i)

    print("SQL Queries to execute: ", sql_files)
    cur = data_extractor.pg_engine.cursor()
    for file in sql_files:
        a = datetime.datetime.now()

        if 'combine_air_data.sql' not in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            data_extractor.pg_engine.commit()
            print('Executed Query: ', file)

        elif 'combine_air_data.sql' in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            data_extractor.pg_engine.commit()
            print('Executed Query: ', file)

        b = datetime.datetime.now()
        delta_seconds = (b - a).total_seconds()
        master_list.append([file, delta_seconds, a, b, 1])

    b1 = datetime.datetime.now()
    delta_seconds_1 = (b1-a1).total_seconds()
    master_list.append(['create_production_tables', a1, b1, len(sql_files)])
    print('Done Creating ALL Production Tables in {} seconds as of: {}'.format(delta_seconds_1, b1))

    if data_extractor.save_locally == True:
        public_tables = ['data_model_performance_tbl', 'dim_geo_names', 'fact_combined_air_data','fact_gta_traffic_arcgis', 'fact_monthly_air_data', 'fact_monthly_air_data_transpose', 'fact_monthly_forecasts', 'fact_traffic_volume']
        for public_table in public_tables:
            df = pd.read_sql_table(table_name=public_table, con=data_extractor.sqlalchemy_engine, schema='public')
            filename = data_extractor.parent_dir+'/Data/'+'Public_'+public_table
            print("Writing PUBLIC.{} locally to file: {}".format(public_table, filename))
            df.to_csv(filename, index_label=False, index=False)

    return master_list