from data_loader import create_staging_tables, create_production_tables
from data_extractor import sqlalchemy_engine, parent_dir, save_locally
import pandas as pd

staging_tables_list = create_staging_tables()
production_tables_list = create_production_tables()
df_production = pd.DataFrame(production_tables_list, columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
df_production['phase'] = 'production'
df_production = df_production[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
df_stage = pd.DataFrame(staging_tables_list, columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
df_stage['phase'] = 'stage'
df_stage = df_stage[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
pipeline_df = pd.concat([df_production, df_stage])
pipeline_df.drop(pipeline_df.tail(1).index, inplace=True)  # drop last row

if save_locally:
    print('Saving Data Model Performance {} in: {}'.format('data_model_performance.csv', parent_dir+'/Analytics/'))
    pipeline_df.to_csv(parent_dir + '/Analytics/data_model_performance.csv', index=False, index_label=False)

pipeline_df.to_sql(name='data_model_performance_tbl', con=sqlalchemy_engine, if_exists='replace', schema='public', index_label=False, index=False)