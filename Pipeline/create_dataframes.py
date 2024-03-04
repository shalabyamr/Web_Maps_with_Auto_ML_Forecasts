from execute_pipeline import execute_pipeline, sqlalchemy_engine, pg_engine, parent_dir
import pandas as pd
import datetime
import gc
import geopandas as gpd
import h2o
from h2o.automl import H2OAutoML

# Important ::::
# Comment out this line after the initial setup of the tables.
#execute_pipeline()

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
    for i in dir(obj_dfs):
        if (not i.startswith('__')) and (not 'data' in i):
            if 'df_fact_traffic_volume' in i:
                exec('obj_dfs.{}.dropna(inplace=True)'.format(i))
            exec_statement = 'h_{} = h2o.h2o.H2OFrame(obj_dfs.{})'.format(i,i)
            print('exec_statement: {}'.format(exec_statement))
            exec(exec_statement, globals())
    obj_dfs.df_fact_traffic_volume['latest_count_date'] = pd.to_datetime(obj_dfs.df_fact_traffic_volume['latest_count_date'])
    h_df_fact_traffic_volume['_id'] = h_df_fact_traffic_volume['_id'].asfactor()
    h_df_fact_traffic_volume['location_id'] = h_df_fact_traffic_volume['location_id'].asfactor()
    h_df_fact_traffic_volume['location'] = h_df_fact_traffic_volume['location'].asfactor()
    X = ['location_id', '_id', 'latest_count_date']
    y = 'px'
    aml = H2OAutoML(max_runtime_secs=60)  # should be 600. However the longer is the better.
    aml.train(x=X, y=y, training_frame=h_df_fact_traffic_volume, leaderboard_frame=h_df_fact_traffic_volume)
    leader_model = aml.leader

    for location_id in obj_dfs.df_fact_traffic_volume['location_id'].unique():
        df_preds = pd.DataFrame()
        df_location = obj_dfs.df_fact_traffic_volume[obj_dfs.df_fact_traffic_volume['location_id'] == location_id]
        df_location['latest_count_date'] = pd.to_datetime(obj_dfs.df_fact_traffic_volume['latest_count_date'])
        start = pd.to_datetime(obj_dfs.df_fact_traffic_volume['latest_count_date'].max() + pd.Timedelta(days=7))
        future_dates = pd.date_range(start=start, freq='D', periods=365)
        df_preds['latest_count_date'] = future_dates
        df_preds['location_id'] = location_id
        df_preds['_id'] = obj_dfs.df_fact_traffic_volume['_id'].unique()[0]
        df_preds = df_preds[['location_id', '_id', 'latest_count_date']]
        df_preds_h = h2o.H2OFrame(df_preds)
        df_location.reset_index(drop=True, inplace=True)
        df_location_h = h2o.H2OFrame(df_location)
        predicted_traffic = leader_model.predict(df_preds_h[['location_id', '_id', 'latest_count_date']])
        df_preds_h['predicted_traffic'] = predicted_traffic
        df_preds = df_preds_h.as_data_frame()
        df_preds['latest_count_date'] = pd.to_datetime(df_preds['latest_count_date'], unit='ms')
        df_preds.to_sql(name='predicted_traffic', con=sqlalchemy_engine, if_exists='append', schema='stage')
        print('Saved Forecasts to Database')
        #df_global_forecasts = df_global_forecasts.append(df_preds)

    h2o.cluster().shutdown()

auto_ml()