import pandas as pd
import datetime
import gc
import geopandas as gpd
import h2o
from h2o.automl import H2OAutoML
from data_extractor import configs_obj

# A Generic Class to store the needed dataframes
class GenericClass():
    pass
dfs_obj = GenericClass()


def create_dataframes(configs_obj):
    dfs_start = datetime.datetime.now()
    query_get_tables = """SELECT table_name FROM information_schema.tables
            WHERE (table_schema = 'public') and (table_name not in(
            'spatial_ref_sys','geography_columns','geometry_columns','data_model_performance_tbl'))"""
    cur = configs_obj.pg_engine.cursor()
    cur.execute(query_get_tables)
    del query_get_tables
    public_tables = [item[0] for item in cur.fetchall()]
    print('Public Tables in Production Schema : ', public_tables, '\n*****************************')
    print('Creating Dataframes From Public Schema Tables as of: {}\n'.format(datetime.datetime.now()))
    i = 1
    for public_table in public_tables:
        print("{} of {}: Processing Public Table '{}':".format(i, len(public_tables), public_table, public_table))

        if 'proj' not in public_table:
            print("\tCreating Dataframe 'df_{}' from Table '{}' :".format(public_table, public_table))
            exec_statement = "dfs_obj.df_{} = pd.read_sql_table(table_name='{}', con=configs_obj.sqlalchemy_engine, schema='public')".format(public_table, public_table)
            exec(exec_statement, globals())

        if 'proj' in public_table:
            print("\tCreating Projected Dataframe 'gpdf_{}' from Table '{}' :".format(public_table, public_table))
            gpd_statement = "dfs_obj.gpdf_{} = gpd.read_postgis('SELECT * FROM public.{}', con=configs_obj.sqlalchemy_engine, geom_col='geom', crs='EPSG:26917')".format(public_table, public_table)
            exec(gpd_statement, globals())
        i = i+1

    dfs_end = datetime.datetime.now()
    dfs_obj.temp_df = dfs_obj.df_fact_traffic_volume.dropna()
    dfs_obj.temp_df['latest_count_date'] = pd.to_datetime(dfs_obj.temp_df['latest_count_date'])
    dfs_obj.temp_df.sort_values(by=['latest_count_date'], inplace=True)
    dfs_obj.temp_df.set_index('latest_count_date', inplace=True)

    dfs_obj.data = []
    for _, d in dfs_obj.temp_df.groupby('latest_count_date'):
        dfs_obj.data.append([[row['lat'], row['lng'], row['px']] for _, row in d.iterrows()])

    dfs_total_seconds = (dfs_end - dfs_start).total_seconds()
    print("\n****************************\nDone Storing Public Tables in df_objs in {} Total Seconds.\n****************************\n".format(dfs_total_seconds))

    del dfs_start, dfs_end
    gc.collect()
    return dfs_obj

## Auto Machine Learning Step using the previously-created dataframesl. ##
def auto_ml():
    automl_start = datetime.datetime.now()
    print("Starting AutoML as of: {}".format(automl_start))
    h2o.init()
    for i in dir(dfs_obj):
        if (not i.startswith('__')) and (not 'data' in i):
            if i == 'df_fact_traffic_volume':
                exec('dfs_obj.{}.dropna(inplace=True)'.format(i))
            exec_statement = 'h_{} = h2o.h2o.H2OFrame(dfs_obj.{})'.format(i,i)
            print('exec_statement: {}'.format(exec_statement))
            exec(exec_statement, globals())
    dfs_obj.df_fact_traffic_volume['latest_count_date'] = pd.to_datetime(dfs_obj.df_fact_traffic_volume['latest_count_date'])
    h_df_fact_traffic_volume['_id'] = h_df_fact_traffic_volume['_id'].asfactor()
    h_df_fact_traffic_volume['location_id'] = h_df_fact_traffic_volume['location_id'].asfactor()
    h_df_fact_traffic_volume['location'] = h_df_fact_traffic_volume['location'].asfactor()
    X = ['location_id', '_id', 'latest_count_date']
    y = 'px'
    aml = H2OAutoML(max_runtime_secs=60)  # should be 600. However the longer is the better.
    aml.train(x=X, y=y, training_frame=h_df_fact_traffic_volume, leaderboard_frame=h_df_fact_traffic_volume)
    leader_model = aml.leader
    df_global_forecasts = pd.DataFrame()
    for location_id in dfs_obj.df_fact_traffic_volume['location_id'].unique():
        df_preds = pd.DataFrame()
        df_location = dfs_obj.df_fact_traffic_volume[dfs_obj.df_fact_traffic_volume['location_id'] == location_id]
        df_location['latest_count_date'] = pd.to_datetime(dfs_obj.df_fact_traffic_volume['latest_count_date'])
        start = pd.to_datetime(dfs_obj.df_fact_traffic_volume['latest_count_date'].max() + pd.Timedelta(days=7))
        future_dates = pd.date_range(start=start, freq='W', periods=52)
        df_preds['latest_count_date'] = future_dates
        df_preds['location_id'] = location_id
        df_preds['_id'] = dfs_obj.df_fact_traffic_volume['_id'].unique()[0]
        df_preds = df_preds[['location_id', '_id', 'latest_count_date']]
        df_preds_h = h2o.H2OFrame(df_preds)
        df_location.reset_index(drop=True, inplace=True)
        predicted_traffic = leader_model.predict(df_preds_h[['location_id', '_id', 'latest_count_date']])
        df_preds_h['predicted_traffic'] = predicted_traffic
        df_preds = df_preds_h.as_data_frame()
        df_preds['latest_count_date'] = pd.to_datetime(df_preds['latest_count_date'], unit='ms')
        df_preds.to_sql(name='predicted_traffic', con=configs_obj.sqlalchemy_engine, if_exists='append', schema='stage')
        print('Saved Forecasts to Database')
        df_global_forecasts = df_global_forecasts._append(df_preds)

    dfs_obj.df_global_forecasts = df_global_forecasts
    gc.collect()
    h2o.cluster().shutdown()