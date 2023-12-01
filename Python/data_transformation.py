import datetime
import warnings
import pandas as pd
from data_extractor import initialize_database, parent_dir
warnings.filterwarnings("ignore")

def transform_monthly_data(save_locally):

    # Transposing the Monthly Air Quality Data #
    a = datetime.datetime.now()
    print('*** Transposing the Monthly Air Quality Data as of: {}***'.format(a))
    sqlalchemy_engine = initialize_database()[0]
    df = pd.read_sql_table(table_name='stg_monthly_air_data', con=sqlalchemy_engine, schema='stage', parse_dates=True)
    df_out = pd.DataFrame()
    for column in df.columns:
        print('Transposing Column: ', column)
        df_temp = pd.DataFrame()
        df_temp['the_date'] = df['the_date'].dt.date
        df_temp['hours_utc'] = df['hours_utc']
        df_temp['cgndb_id'] = str(column)
        df_temp['air_quality_value'] = df[column]
        df_temp['download_link'] = df['download_link']
        df_temp['src_filename'] = df['src_filename']
        df_temp['last_updated'] = df['last_updated']
        df_out = pd.concat([df_out, df_temp])
    df_out.to_sql(name='stg_monthly_air_data_transpose', con=sqlalchemy_engine, if_exists='replace', schema='stage',
                  index_label=False, index=False)

    if save_locally:
        transposed_filename = parent_dir + '/Data/' + 'monthly_air_data_transposed.csv'
        print('Saving Transposed Monthly Air Data to: ', transposed_filename)
        df_out.to_csv(transposed_filename, index_label=False, index=False)

    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("********************************\n",'Transposed Monthly Air Data Done in {} seconds.'.format(delta_seconds), "\n********************************\n")
    return 'transform_monthly_data', delta_seconds, a, b, 1