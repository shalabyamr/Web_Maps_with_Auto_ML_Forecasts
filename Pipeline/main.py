from data_extractor import configs_obj, read_configs, initialize_database
import data_loader as l
import datetime
import pandas as pd
import create_dataframes
from create_dataframes import dfs_obj
import create_maps

## First Step is to create Staging and Production Data ##
if False:
    read_configs()
    initialize_database()
    start = datetime.datetime.now()
    print('Executing Pipeline as of ' + str(start))
    staging_tables_list = l.create_staging_tables(sqlalchemy_engine=configs_obj.sqlalchemy_engine)
    production_tables_list = l.create_production_tables()
    df_production = pd.DataFrame(production_tables_list,
                                      columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
    df_production['phase'] = 'production'
    df_production = df_production[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
    df_stage = pd.DataFrame(staging_tables_list,
                                columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
    df_stage['phase'] = 'stage'
    df_stage = df_stage[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
    pipeline_df = pd.concat([df_production, df_stage])
    pipeline_df.drop(pipeline_df.tail(1).index, inplace=True)
    del df_stage, df_production
    if configs_obj.save_locally:
            print('Saving Data Model Performance {} in: {}'.format('data_model_performance.csv', configs_obj.parent_dir + '/Analytics/'))
            pipeline_df.to_csv(configs_obj.parent_dir + '/Analytics/data_model_performance.csv', index=False, index_label=False)

    pipeline_df.to_sql(name='data_model_performance_tbl', con=configs_obj.sqlalchemy_engine, if_exists='replace', schema='public',
                           index_label=False, index=False)
    end = datetime.datetime.now()
    total_seconds = (end - start).total_seconds()
    print('Done Executing Pipeline as of {} in {} seconds'.format(end, str(total_seconds)))
    print('*****************************\n')
## End of the First Step ##

## Second Step : Create Dataframes needed for the AutoML ##
# Create the Object Containing the Dataframes to avoid running create_dfs() function repeatedly in auto_ml() and
# create_maps().  Also H2O Auto ML needs to save and insert Prediction Dataframes into object.
read_configs()
initialize_database()
create_dataframes.create_dataframes(configs_obj)
create_dataframes.auto_ml()

## Third Step: Create HTML Maps
create_maps.create_maps(dfs_obj=dfs_obj, configs_obj=configs_obj, map_type='ALL', show=False)

## Fourth Step: Test Load the Created HTML Maps
