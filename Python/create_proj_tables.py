import pandas as pd
from shapely.geometry import Point
from data_extractor import sqlalchemy_engine, pg_engine
import geopandas


df_gta_traffic_arcgis = pd.read_sql_table(table_name='fact_gta_traffic_arcgis', con=sqlalchemy_engine, schema='public')
gdf = geopandas.GeoDataFrame(df_gta_traffic_arcgis, geometry=geopandas.points_from_xy(df_gta_traffic_arcgis.longitude, df_gta_traffic_arcgis.latitude), crs="EPSG:26917")
gdf.rename(columns={'geometry':'geom'}, inplace=True)
gdf.set_geometry(col='geom', drop=False, inplace=True)
print(gdf.head())
print(gdf.dtypes)
gdf.plot(color="blue")
gdf.to_postgis('fact_gta_traffic_proj', con=sqlalchemy_engine, schema='public', if_exists='replace', index=False)

cur = pg_engine.cursor()
query = """ALTER TABLE PUBLIC.FACT_GTA_TRAFFIC_PROJ
  ALTER COLUMN geom 
  TYPE Geometry(Point, 26917)
  USING ST_Transform(geom, 26917);"""

try:
    print('Executing Query: {}'.format(query))
    cur.execute(query)
    pg_engine.commit()
except BaseException as exception:
    print('!!failed to execute query!!')
    print(exception)


df_air_data = pd.read_sql_table(table_name='fact_combined_air_data', con=sqlalchemy_engine, schema='public', parse_dates=True)
df_air_data['weekday'] = df_air_data['the_date'].dt.strftime('%A')
df_air_data = df_air_data[df_air_data['cgndb_id'].str.upper().isin(['FCKTB', 'FCWYG', 'FDQBU', 'FDQBX', 'FEUZB'])]
gdf_air_data = geopandas.GeoDataFrame(df_air_data, geometry=geopandas.points_from_xy(df_air_data.geo_long, df_air_data.geo_lat), crs="EPSG:26917")
gdf_air_data.rename(columns={'geometry':'geom'}, inplace=True)
gdf_air_data.set_geometry(col='geom', drop=False, inplace=True)
print(gdf_air_data.head())
print(gdf_air_data.dtypes)
gdf_air_data.plot(color="red")
gdf_air_data.to_postgis('fact_air_data_proj', con=sqlalchemy_engine, schema='public', if_exists='replace', index=False)



query_create_fact_air_data_proj = """ALTER TABLE PUBLIC.fact_air_data_proj 
  ALTER COLUMN geom 
  TYPE Geometry(Point, 26917);"""

try:
    print('Executing Query: {}'.format(query_create_fact_air_data_proj))
    cur.execute(query_create_fact_air_data_proj)
    pg_engine.commit()
except BaseException as exception:
    print('!!failed to execute query!!')
    print(exception)


check_fact_air_data_proj_query = "SELECT DISTINCT ST_SRID(geom)::INT airdata_check FROM PUBLIC.FACT_AIR_DATA_PROJ;"
df_check_fact_air_data_proj = pd.read_sql_query(check_fact_air_data_proj_query, con=sqlalchemy_engine)
if df_check_fact_air_data_proj['airdata_check'].unique() == 26917:
    print("Air Data is Projected Correctly to {}".format(df_check_fact_air_data_proj['airdata_check'].unique()))

else:
    print("!! Error!!! Air Data NOT Projected Correctly to {}".format(df_check_fact_air_data_proj['airdata_check'].unique()))

check_gta_traffic_query = "SELECT DISTINCT ST_SRID(geom)::INT gtatraffic_check FROM PUBLIC.fact_gta_traffic_proj;"
df_check_gta_traffic_query = pd.read_sql_query(check_gta_traffic_query, con=sqlalchemy_engine)
if df_check_gta_traffic_query['gtatraffic_check'].unique() == 26917:
    print("GTA Traffic is Projected Correctly to {}".format(df_check_gta_traffic_query['gtatraffic_check'].unique()))

else:
    print("!! Error!!! GTA Traffic NOT Projected Correctly to {}".format(df_check_gta_traffic_query['gtatraffic_check'].unique()))


create_geo_stations_avgs_query = """--DETERMINING THE ENTIRE AVERAGE AT EACH STATION
DROP TABLE IF EXISTS PUBLIC.fact_geo_stations_avg;
CREATE TABLE PUBLIC.fact_geo_stations_avg AS(
SELECT cgndb_id, geo_lat, geo_long, geom,
CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) as station_avg
  FROM PUBLIC.fact_air_data_proj
  where air_quality_value is not null
  GROUP BY 1, 2, 3, 4);"""

try:
    print('Executing Query: {}'.format(create_geo_stations_avgs_query))
    cur.execute(create_geo_stations_avgs_query)
    pg_engine.commit()
except BaseException as exception:
    print('!!failed to execute query!!')
    print(exception)


hourly_query = """DROP TABLE IF EXISTS PUBLIC.FACT_HOURLY_AVG;
CREATE TABLE PUBLIC.FACT_HOURLY_AVG AS(
WITH MIDNIGHT_TO_5AM AS(
  SELECT 
  cgndb_id, geo_lat, geo_long, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS dawn_avg --FROM MIDNIGHT TO 5AM
  FROM public.fact_air_data_proj
  WHERE hours_utc BETWEEN '0' AND '5'
  GROUP BY 1, 2, 3, 4
), FROM_6AM_TO_11AM AS(
  SELECT cgndb_id, geo_lat, geo_long, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS morning_avg --FROM 6AM TO 11AM
  FROM public.fact_air_data_proj
  WHERE hours_utc BETWEEN '6' AND '11'
  GROUP BY 1, 2, 3, 4
), FROM_NOON_TO_5PM AS(
 SELECT   cgndb_id, geo_lat, geo_long, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS noon_avg  --FROM NOON TO 5PM
  FROM FACT_AIR_DATA_PROJ
  WHERE hours_utc BETWEEN '12' AND '17'
  GROUP BY 1, 2, 3, 4
), FROM_6PM_TO_11PM AS(
SELECT cgndb_id, geo_lat, geo_long, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS evening_avg --FROM 6PM TO 11PM
  FROM FACT_AIR_DATA_PROJ
  WHERE hours_utc BETWEEN '18' AND '23'
  GROUP BY 1, 2, 3, 4
), EXPORT AS(
  SELECT 
  A.cgndb_id, A.geo_lat, A.geo_long, A.geom,
  A.dawn_avg, B.morning_avg, C.noon_avg, D.evening_avg 
  FROM  MIDNIGHT_TO_5AM A
 INNER JOIN FROM_6AM_TO_11AM B ON B.cgndb_id = A.cgndb_id
 INNER JOIN FROM_NOON_TO_5PM C ON C.cgndb_id = A.cgndb_id
 INNER JOIN FROM_6PM_TO_11PM D ON D.cgndb_id = A.cgndb_id
) SELECT * FROM EXPORT);"""

try:
    print('Executing hourly_query')
    cur.execute(hourly_query)
    pg_engine.commit()
except BaseException as exception:
    print('!!failed to execute hourly_query!!')
    print(exception)


weekdays_query = """DROP TABLE IF EXISTS PUBLIC.FACT_WEEKDAYS_AVG;
CREATE TABLE PUBLIC.FACT_WEEKDAY_AVERAGES AS(
WITH MONDAY_AVG AS(
--SUBSETTING OUR DATA FOR THE DAY OF WEEK AND CALCULATING THE AVERAGE AIR QUALITY
SELECT cgndb_id, geo_lat, geo_long, geom
, CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS monday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Monday'
  GROUP BY 1, 2, 3, 4
), TUESDAY_AVG AS(
SELECT cgndb_id, geo_lat, geo_long, geom,
CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS tuesday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Tuesday'
  GROUP BY 1, 2, 3, 4
), WEDNESDAY_AVG AS(
SELECT cgndb_id, geo_lat, geo_long, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS wednesday_avg 
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Wednesday'
  GROUP BY 1, 2, 3, 4
), THURSDAY_AVG AS(
SELECT cgndb_id, geo_lat, geo_long, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS thursday_avg 
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Thursday'
  GROUP BY 1, 2, 3, 4
), FRIDAY_AVG AS (
SELECT cgndb_id, geo_lat, geo_long, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS friday_avg 
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Friday'
  GROUP BY 1, 2, 3, 4
), ٍِSATURDAY_AVG AS(
SELECT cgndb_id, geo_lat, geo_long, geom
, CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS saturday_av
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Saturday'
  GROUP BY 1, 2, 3, 4
), SUNDAY_AVG AS(
 SELECT cgndb_id, geo_lat, geo_long, geom
 , CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS sunday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Sunday'
  GROUP BY 1, 2, 3, 4
  
), EXPORT AS(
 SELECT A.cgndb_id, A.geo_lat, A.geo_long, A.geom
 , A.MONDAY_AVG, B.tuesday_avg, C.wednesday_avg
 , D.thursday_avg, E.friday_avg, F.saturday_av
 , G.sunday_avg
 FROM MONDAY_AVG A 
 INNER JOIN TUESDAY_AVG B ON B.cgndb_id = A.cgndb_id
 INNER JOIN WEDNESDAY_AVG C ON C.cgndb_id = A.cgndb_id
 INNER JOIN THURSDAY_AVG D ON D.cgndb_id = A.cgndb_id
 INNER JOIN FRIDAY_AVG E ON E.cgndb_id = A.cgndb_id
 INNER JOIN ٍِSATURDAY_AVG F ON f.cgndb_id = A.cgndb_id
 INNER JOIN SUNDAY_AVG G ON G.cgndb_id = A.cgndb_id
) SELECT * FROM EXPORT);"""

try:
    print('Executing weekdays_query Query')
    cur.execute(weekdays_query)
    pg_engine.commit()
except BaseException as exception:
    print('!!failed to execute weekdays_query query!!')
    print(exception)




