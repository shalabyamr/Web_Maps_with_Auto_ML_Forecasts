import datetime
import warnings
import geopandas
import pandas as pd
from data_extractor import configs_obj
warnings.filterwarnings("ignore")


# Transposing the Monthly Air Quality Data #
def transform_monthly_data(configs_obj):
    a = datetime.datetime.now()
    print('*** Transposing the Monthly Air Quality Data as of: {}***'.format(a))
    df = pd.read_sql_table(table_name='stg_monthly_air_data', con=configs_obj.database['sqlalchemy_engine'], schema='stage', parse_dates=True)
    non_cgndb_id_cols = ['download_link', 'hours_utc', 'last_updated', 'src_filename', 'the_date']
    df_out = pd.DataFrame()
    for column in df.columns:
        print('Transposing Column: ', column)
        df_temp = pd.DataFrame()
        df_temp['the_date'] = df['the_date'].dt.date
        df_temp['hours_utc'] = df['hours_utc']
        if column not in non_cgndb_id_cols:
            df_temp['cgndb_id'] = str(column)
            df_temp['air_quality_value'] = df[column]
        if column in non_cgndb_id_cols:
            df_temp['cgndb_id'] = None
            df_temp['air_quality_value'] = None
        df_temp['download_link'] = df['download_link']
        df_temp['src_filename'] = df['src_filename']
        df_temp['last_updated'] = df['last_updated']
        df_out = pd.concat([df_out, df_temp])
    df_out.dropna(inplace=True)
    df_out.to_sql(name='stg_monthly_air_data_transpose', con=configs_obj.database['sqlalchemy_engine'], if_exists='replace', schema='stage',
                  index_label=False, index=False)
    if configs_obj.run_conditions['save_locally']:
        transposed_filename = configs_obj.run_conditions['parent_dir'] + '/Data/' + 'monthly_air_data_transposed.csv'
        print('Saving Transposed Monthly Air Data to: ', transposed_filename)
        df_out.to_csv(transposed_filename, index_label=False, index=False)
    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("*****************************\n",'Transposed Monthly Air Data Done in {} seconds.'.format(delta_seconds), "\n*****************************\n")
    return 'transform_monthly_data', delta_seconds, a, b, 1

# This step creates the projection tables with "geom" geometry columns in hex.
def create_postgis_proj_tables(sqlalchemy_engine, pg_engine):
    a = datetime.datetime.now()
    print('*** Creating POST_GIS Projected as of: {} ***'.format(a))

    df_gta_traffic_arcgis = pd.read_sql_table(table_name='fact_gta_traffic_arcgis', con=configs_obj.database['sqlalchemy_engine'],
                                              schema='public')
    gdf = geopandas.GeoDataFrame(df_gta_traffic_arcgis,
                                 geometry=geopandas.points_from_xy(df_gta_traffic_arcgis.longitude,
                                                                   df_gta_traffic_arcgis.latitude), crs="EPSG:26917")
    gdf.rename(columns={'geometry': 'geom'}, inplace=True)
    gdf.set_geometry(col='geom', drop=False, inplace=True)
    gdf.to_postgis('fact_gta_traffic_proj', con=configs_obj.database['sqlalchemy_engine'], schema='public', if_exists='replace', index=False)

    cur = pg_engine.cursor()
    query = """ALTER TABLE PUBLIC.FACT_GTA_TRAFFIC_PROJ
      ALTER COLUMN geom 
      TYPE Geometry(Point, 26917)
      USING ST_Transform(geom, 26917);"""

    try:
        print('Executing Query: {}'.format(query))
        cur.execute(query)
        pg_engine.commit()
    except Exception as exception:
        print('!!failed to execute query!!')
        print(exception)
    df_air_data = pd.read_sql_table(table_name='fact_combined_air_data', con=configs_obj.database['sqlalchemy_engine'], schema='public', parse_dates=True)
    df_air_data['weekday'] = df_air_data['the_date'].dt.strftime('%A')
    df_air_data = df_air_data[df_air_data['cgndb_id'].str.upper().isin(['FCKTB', 'FCWYG', 'FDQBU', 'FDQBX', 'FEUZB'])]
    gdf_air_data = geopandas.GeoDataFrame(df_air_data,
                                          geometry=geopandas.points_from_xy(df_air_data.longitude, df_air_data.latitude),
                                          crs="EPSG:26917")
    gdf_air_data.rename(columns={'geometry': 'geom'}, inplace=True)
    gdf_air_data.set_geometry(col='geom', drop=False, inplace=True)
    gdf_air_data.to_postgis('fact_air_data_proj', con=configs_obj.database['sqlalchemy_engine'], schema='public', if_exists='replace',
                            index=False)
    query_create_fact_air_data_proj = """ALTER TABLE PUBLIC.fact_air_data_proj 
      ALTER COLUMN geom 
      TYPE Geometry(Point, 26917);"""
    try:
        print('Executing Query: {}'.format(query_create_fact_air_data_proj))
        cur.execute(query_create_fact_air_data_proj)
        pg_engine.commit()
    except Exception as exception:
        print('!!failed to execute query!!')
        print(exception)
    b = datetime.datetime.now()
    delta_seconds = (b-a).total_seconds()
    print("*****************************\n",'Done Creating POSTGIS Projection Tables in {} seconds.'.format(delta_seconds), "\n*****************************\n")
    return 'create_postgis_projected_tables', delta_seconds, a, b, 1
