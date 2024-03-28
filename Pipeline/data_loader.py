import warnings
import pandas as pd
import glob
import datetime
import data_extractor
from data_extractor import configs_obj
from data_transformer import transform_monthly_data, create_postgis_proj_tables
warnings.filterwarnings("ignore")

# This function creates the staging layer of the extracted data. There are no
# data filters injected into Staging Layer as it only grabs the scraped data from
# the web.
def create_staging_tables(configs_obj):
    master_list = []
    # to execute loading the monthly data into staging layer
    monthly_date_step = data_extractor.extract_monthly_data(sqlalchemy_engine=configs_obj.database['sqlalchemy_engine'])
    master_list.append(monthly_date_step)

    # to execute loading the monthly forecasts into the staging layer
    monthly_forecasts_step = data_extractor.extract_monthly_forecasts(configs_obj=configs_obj)
    master_list.append(monthly_forecasts_step)

    # to execute loading the traffic volume dataset into the staging layer
    traffic_volume_step = data_extractor.extract_traffic_volume(configs_obj=configs_obj)
    master_list.append(traffic_volume_step)

    # to execute loading the geographical database names  into the staging layer
    geo_names_step = data_extractor.extract_geo_names_data(configs_obj=configs_obj)
    master_list.append(geo_names_step)


    # to execute loading the loading ArcGIS Toronto and Peel Traffic into the staging layer
    traffic_arcgis_step = data_extractor.extract_gta_traffic_arcgis(configs_obj=configs_obj)
    master_list.append(traffic_arcgis_step)

    # Transposes monthly Air Data from Column Names to Rows
    transform_monthly_step = transform_monthly_data(configs_obj=configs_obj)
    master_list.append(transform_monthly_step)
    return master_list

# Processes the intermediate SQL Queries that eliminate data redundacy and 3:1 duplication
# rate (3 duplicated rows to 1 unique record) as there are 4 measures taken
# per UTC Hour from the web sources.
def create_production_tables(configs_obj):
    master_list = []
    a1 = datetime.datetime.now()
    sql_files = glob.glob(configs_obj.run_conditions['parent_dir'] + '/SQL/*.sql')

    # Combine_Air_Data Table needs to be created after all staging tables
    for i in sql_files:
        if 'combine_air_data.sql' in i:
            sql_files.remove(i)
        sql_files.append(i)

    print("SQL Queries to execute: ", sql_files)
    cur = configs_obj.database['pg_engine'].cursor()
    for file in sql_files:
        a = datetime.datetime.now()
        if 'combine_air_data.sql' and 'postgis' not in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            configs_obj.database['pg_engine'].commit()

        elif 'combine_air_data.sql' in file:
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            configs_obj.database['pg_engine'].commit()

    for file in sql_files:
        if 'postgis' in file:
            print('---- Creating Projection Tables ----')
            postgis_proj_list = create_postgis_proj_tables(configs_obj.database['sqlalchemy_engine'], configs_obj.database['pg_engine'])
            master_list.append(postgis_proj_list)
            print('Processing Query File: ', file)
            query = str(open(file).read())
            cur.execute(query)
            configs_obj.database['pg_engine'].commit()

        b = datetime.datetime.now()
        delta_seconds = (b-a).total_seconds()
        master_list.append([file, delta_seconds, a, b, 1])

    if configs_obj.run_conditions['save_locally']:
        query_get_tables = """SELECT table_name FROM information_schema.tables
            WHERE (table_schema = 'public') and (table_name not in('spatial_ref_sys','geography_columns','geometry_columns'))"""
        cur.execute(query_get_tables)
        del query_get_tables
        public_tables = [item[0] for item in cur.fetchall()]
        for public_table in public_tables:
            df = pd.read_sql_table(table_name=public_table, con=configs_obj.database['sqlalchemy_engine'], schema='public')
            filename = configs_obj.run_conditions['parent_dir']+'/Data/'+'Public_'+public_table+'.csv'
            print("Writing PUBLIC.{} locally to file: {}".format(public_table, filename))
            df.to_csv(filename, index_label=False, index=False)

    b1 = datetime.datetime.now()
    delta_seconds_1 = (b1-a1).total_seconds()
    master_list.append(['create_production_tables', delta_seconds_1, a1, b1, len(sql_files)])
    print('*****************************\nDone Creating ALL Production Tables in {} seconds as of: {}'.format(delta_seconds_1, b1))
    return master_list
