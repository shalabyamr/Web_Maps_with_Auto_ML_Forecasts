DROP TABLE IF EXISTS PUBLIC.FACT_MONTHLY_FORECASTS;
CREATE TABLE PUBLIC.FACT_MONTHLY_FORECASTS AS(
  SELECT
    cgndb_code,
    community_name,
    validity_date,
    validity_time_utc,
    period,
    value,
    amended,
    donwnload_link,
    src_filename,
    last_updated,
    NOW() AT TIME ZONE 'EST' AS last_inserted
  FROM(
      SELECT *
   , ROW_NUMBER() OVER(PARTITION BY "cgndb_code", "validity_date" ORDER BY validity_time_utc DESC) AS RN
      FROM STAGE.stg_monthly_forecasts
  ) A WHERE RN=1);