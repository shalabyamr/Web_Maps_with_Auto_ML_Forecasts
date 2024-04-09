import sys
import platform
from data_extractor import configs_obj, read_configs, initialize_database
import data_loader
import datetime
import pandas as pd
import dataframes_creator
from dataframes_creator import dfs_obj
import maps_creator
from maps_tester import test_maps
import os
import warnings
warnings.filterwarnings("ignore")

# Initial Step is to read run-time configurations.
main_start_time = datetime.datetime.now()
read_configs()

# First Step is to create Staging and Production Data.  This can be bypassed if the Tables are created
if configs_obj.run_conditions['create_tables']:
    read_configs()
    initialize_database()
    start = datetime.datetime.now()
    print('Executing Pipeline as of ' + str(start))
    staging_tables_list = data_loader.create_staging_tables(configs_obj=configs_obj)
    production_tables_list = data_loader.create_production_tables(configs_obj=configs_obj)
    df_production = pd.DataFrame(production_tables_list,
                                 columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
    df_production['phase'] = 'production'
    df_production = df_production[
        ['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
    df_stage = pd.DataFrame(staging_tables_list,
                            columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
    df_stage['phase'] = 'stage'
    df_stage = df_stage[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
    pipeline_df = pd.concat([df_production, df_stage])
    pipeline_df.drop(pipeline_df.tail(1).index, inplace=True)
    del df_stage, df_production
    if configs_obj.run_conditions['save_locally']:
        print('Saving Data Model Performance {} in: {}'.format('data_model_performance.csv',
                                                               configs_obj.run_conditions[
                                                                   'parent_dir'] + '/Data/'))
        pipeline_df.to_csv(configs_obj.run_conditions['parent_dir'] + '/Data/data_model_performance.csv',
                           index=False,
                           index_label=False)

    pipeline_df['step_name'] = pipeline_df['step_name'].apply(lambda x: os.path.basename(x))
    pipeline_df.to_sql(name='data_model_performance_tbl', con=configs_obj.database['sqlalchemy_engine'],
                       if_exists='replace',
                       schema='public', index_label=False, index=False)

    # Track Performance of WebMaps and AutoML Steps.
    print('Inserting WebMaps Performance Logs Entries into database.')
    webmaps_query = f"""insert into public.data_model_performance_tbl (phase, step_name, duration_seconds, start_time, end_time, files_processed) 
         VALUES   
         ('WebMaps', 'folium', -1, '2002-05-01 00:00:00.0000', '2002-03-15 13:59:12.498894', -1)
       , ('WebMaps', 'mapbox', -1, '2002-05-01 00:00:00.0000', '2002-03-15 13:59:12.498894', -1)
       , ('WebMaps', 'turf', -1, '2002-05-01 00:00:00.0000', '2002-03-15 13:59:12.498894', -1)
       , ('WebMaps', 'auto_ml', -1, '2002-05-01 00:00:00.0000', '2002-03-15 13:59:12.498894', -1)
       , ('WebMaps', 'create_dataframes', -1, '2002-05-01 00:00:00.0000', '2024-03-15 13:59:12.498894', -1)
       , ('WebMaps', 'test_maps', -1, '2002-05-01 00:00:', '2024-03-15 13:59:12.498894',-1);"""
    cur = configs_obj.database['pg_engine'].cursor()
    cur.execute(webmaps_query)
    configs_obj.database['pg_engine'].commit()
    end = datetime.datetime.now()
    total_seconds = (end - start).total_seconds()
    print(f"Done Executing Pipeline as of {end} in {total_seconds} Seconds")
    print('*****************************\n')
# End of the First Step #

# Second Step : Not optional.  Create Dataframes needed for the AutoML #
# Create the Object Containing the Dataframes to avoid running create_dfs() function repeatedly in auto_ml() and
# create_maps().  Also H2O Auto ML needs to save and insert Prediction Dataframes into object.
if not configs_obj.run_conditions['create_tables']:
    read_configs()
    initialize_database()
dataframes_creator.create_dataframes(configs_obj)

# Auto Machine Learning can be omitted.
if configs_obj.run_conditions['run_auto_ml']:
    dataframes_creator.auto_ml(dfs_obj)

# Third Step: Create HTML Maps.  It Cannot be skipped.
# If AutoML was requested, the forecast layer will not be added to the map.
# If AutoML was skipped, only the descriptive (actual) are added to the map without any forecasts.
maps_creator.create_maps(dfs_obj=dfs_obj, configs_obj=configs_obj)

# Fourth and Last Step: Test Load the Created HTML Maps
# Depending on run_conditions['show_maps'] attributed Boolean Value
# Each map type will launch in its own optimal browser with the minimum loading time.
if ('MACOS' in platform.platform().upper()) and (configs_obj.run_conditions['show_maps']):
    print(f"Running on Platform: {platform.platform().upper()}. Make sure Safari Test Automation is Enabled: https://developer.apple.com/documentation/webkit/testing_with_webdriver_in_safari")
    test_maps(configs_obj=configs_obj)

elif ('MACOS' not in platform.platform().upper()) and (configs_obj.run_conditions['show_maps']):
    print('Sorry. Map Tester is only supported on MacOS.')
    sys.exit(0)

elif not configs_obj.run_conditions['show_maps']:
    print('Show Maps Disabled. Maps were created without testing.')

print(f"*****************************\nDone Executing Pipeline as in {(datetime.datetime.now()-main_start_time).total_seconds()} Seconds.\n*****************************")
