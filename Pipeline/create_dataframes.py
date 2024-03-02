from execute_pipeline import execute_pipeline, sqlalchemy_engine, pg_engine, parent_dir
import pandas as pd
import datetime
import gc
import geopandas as gpd
import h2o

# Important ::::
# Comment out this line after the initial setup of the tables.
execute_pipeline()

# A Generic Class to store the needed dataframes
class GenericClass():
    pass

def create_dataframes():
    dfs_start = datetime.datetime.now()
    query_get_tables = """SELECT table_name FROM information_schema.tables
            WHERE (table_schema = 'public') and (table_name not in(
            'spatial_ref_sys','geography_columns','geometry_columns','data_model_performance_tbl'))"""
    cur = pg_engine.cursor()
    cur.execute(query_get_tables)
    del query_get_tables
    public_tables = [item[0] for item in cur.fetchall()]
    print('Public Tables in Production Schema : ', public_tables, '\n*****************************')
    print('Creating Dataframes From Public Schema Tables as of: {}\n'.format(datetime.datetime.now()))
    exec('obj_dfs = GenericClass()', globals())
    i = 1
    for public_table in public_tables:
        print("{} of {}: Processing Public Table '{}':".format(i, len(public_tables), public_table, public_table))

        if 'proj' not in public_table:
            print("\tCreating Dataframe 'df_{}' from Table '{}' :".format(public_table, public_table))
            exec_statement = "obj_dfs.df_{} = pd.read_sql_table(table_name='{}', con=sqlalchemy_engine, schema='public')".format(public_table, public_table)
            exec(exec_statement, globals())

        if 'proj' in public_table:
            print("\tCreating Projected Dataframe 'gpdf_{}' from Table '{}' :".format(public_table, public_table))
            gpd_statement = "obj_dfs.gpdf_{} = gpd.read_postgis('SELECT * FROM public.{}', con=sqlalchemy_engine, geom_col='geom', crs='EPSG:26917')".format(public_table, public_table)
            exec(gpd_statement, globals())
        i = i+1

    dfs_end = datetime.datetime.now()
    obj_dfs.temp_df = obj_dfs.df_fact_traffic_volume.dropna()
    obj_dfs.temp_df['latest_count_date'] = pd.to_datetime(obj_dfs.temp_df['latest_count_date'])
    obj_dfs.temp_df.sort_values(by=['latest_count_date'], inplace=True)
    obj_dfs.temp_df.set_index('latest_count_date', inplace=True)

    obj_dfs.data = []
    for _, d in obj_dfs.temp_df.groupby('latest_count_date'):
        obj_dfs.data.append([[row['lat'], row['lng'], row['px']] for _, row in d.iterrows()])

    dfs_total_seconds = (dfs_end - dfs_start).total_seconds()
    print("\n****************************\nDone Storing Public Tables in df_objs in {} Total Seconds.\n****************************\n".format(dfs_total_seconds))

    del dfs_start, dfs_end
    gc.collect()
    return obj_dfs

# Create the Object Containing the Dataframes to avoid running create_dfs() function repeatedly in auto_ml() and
# create_maps().  Also H2O Auto ML needs to save and insert Prediction Dataframes into object.
obj_dfs = create_dataframes()
def auto_ml():
    automl_start = datetime.datetime.now()
    print("Starting AutoML as of: {}".format(automl_start))
    h2o.init()

    h2o.cluster().shutdown()

auto_ml()