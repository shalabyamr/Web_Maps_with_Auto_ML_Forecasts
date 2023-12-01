from data_loader import parent_dir, create_staging_tables, create_production_tables, save_locally, sqlalchemy_engine
import pandas as pd

staging_tables_list = create_staging_tables()
production_tables_list = create_production_tables()
pipeline_df = pd.DataFrame(production_tables_list, columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
pipeline_df['phase'] = 'production'
pipeline_df = pipeline_df[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
pipeline_df2 = pd.DataFrame(staging_tables_list, columns=['step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed'])
pipeline_df2['phase'] = 'stage'
pipeline_df2 = pipeline_df2[['phase', 'step_name', 'duration_seconds', 'start_time', 'end_time', 'files_processed']]
pipeline_df = pd.concat([pipeline_df2, pipeline_df])
pipeline_df.drop(pipeline_df.tail(1).index,inplace=True) # drop last row

if save_locally == True:
    print('Saving Data Model Performance {} in: {}'.format('data_model_performance.csv', parent_dir+'/Analytics/'))
    pipeline_df.to_csv(parent_dir + '/Analytics/data_model_performance.csv', index=False, index_label=False)


pipeline_df.to_sql(name='data_model_performance_tbl', con=sqlalchemy_engine, if_exists='replace', schema='public', index_label=False, index=False)