DROP TABLE IF EXISTS PUBLIC.FACT_HOURLY_AVG;
CREATE TABLE PUBLIC.FACT_HOURLY_AVG AS(
WITH MIDNIGHT_TO_5AM AS(
  SELECT
  cgndb_id, latitude, longitude, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS dawn_avg --FROM MIDNIGHT TO 5AM
  FROM public.fact_air_data_proj
  WHERE hours_utc BETWEEN '0' AND '5'
  GROUP BY 1, 2, 3, 4
), FROM_6AM_TO_11AM AS(
  SELECT cgndb_id, latitude, longitude, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS morning_avg --FROM 6AM TO 11AM
  FROM public.fact_air_data_proj
  WHERE hours_utc BETWEEN '6' AND '11'
  GROUP BY 1, 2, 3, 4
), FROM_NOON_TO_5PM AS(
 SELECT   cgndb_id, latitude,  longitude, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS noon_avg  --FROM NOON TO 5PM
  FROM FACT_AIR_DATA_PROJ
  WHERE hours_utc BETWEEN '12' AND '17'
  GROUP BY 1, 2, 3, 4
), FROM_6PM_TO_11PM AS(
SELECT cgndb_id, latitude,  longitude, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS evening_avg --FROM 6PM TO 11PM
  FROM FACT_AIR_DATA_PROJ
  WHERE hours_utc BETWEEN '18' AND '23'
  GROUP BY 1, 2, 3, 4
), EXPORT AS(
  SELECT
  A.cgndb_id, A.latitude, A. longitude, A.geom,
  A.dawn_avg, B.morning_avg, C.noon_avg, D.evening_avg
  FROM  MIDNIGHT_TO_5AM A
 INNER JOIN FROM_6AM_TO_11AM B ON B.cgndb_id = A.cgndb_id
 INNER JOIN FROM_NOON_TO_5PM C ON C.cgndb_id = A.cgndb_id
 INNER JOIN FROM_6PM_TO_11PM D ON D.cgndb_id = A.cgndb_id
) SELECT * FROM EXPORT);

DROP TABLE IF EXISTS PUBLIC.FACT_WEEKDAYS_AVG;
CREATE TABLE IF NOT EXISTS PUBLIC.FACT_WEEKDAYS_AVG AS(
WITH MONDAY_AVG AS(
--SUBSETTING OUR DATA FOR THE DAY OF WEEK AND CALCULATING THE AVERAGE AIR QUALITY
SELECT cgndb_id, latitude,  longitude, geom
, CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS monday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Monday'
  GROUP BY 1, 2, 3, 4
), TUESDAY_AVG AS(
SELECT cgndb_id, latitude,  longitude, geom,
CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS tuesday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Tuesday'
  GROUP BY 1, 2, 3, 4
), WEDNESDAY_AVG AS(
SELECT cgndb_id, latitude,  longitude, geom,
  CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS wednesday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Wednesday'
  GROUP BY 1, 2, 3, 4
), THURSDAY_AVG AS(
SELECT cgndb_id, latitude,  longitude, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS thursday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Thursday'
  GROUP BY 1, 2, 3, 4
), FRIDAY_AVG AS (
SELECT cgndb_id, latitude,  longitude, geom,
 CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS friday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Friday'
  GROUP BY 1, 2, 3, 4
), ٍِSATURDAY_AVG AS(
SELECT cgndb_id, latitude,  longitude, geom
, CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS saturday_av
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Saturday'
  GROUP BY 1, 2, 3, 4
), SUNDAY_AVG AS(
 SELECT cgndb_id, latitude,  longitude, geom
 , CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) AS sunday_avg
  FROM FACT_AIR_DATA_PROJ
  WHERE weekday = 'Sunday'
  GROUP BY 1, 2, 3, 4
), EXPORT AS(
 SELECT A.cgndb_id, A.latitude, A. longitude, A.geom
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
) SELECT * FROM EXPORT);

DROP TABLE IF EXISTS PUBLIC.fact_geo_stations_avg;
CREATE TABLE PUBLIC.fact_geo_stations_avg AS(
SELECT cgndb_id, latitude,  longitude, geom,
CAST(AVG(air_quality_value::float) AS DECIMAL(10, 2)) as station_avg
  FROM PUBLIC.fact_air_data_proj
  where air_quality_value is not null
  GROUP BY 1, 2, 3, 4);
