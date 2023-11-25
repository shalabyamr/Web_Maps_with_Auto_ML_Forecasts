DROP TABLE IF EXISTS PUBLIC.FACT_MONTHLY_AIR_DATA_TRANSPOSE;
CREATE TABLE PUBLIC.FACT_MONTHLY_AIR_DATA_TRANSPOSE AS(
  SELECT
    "the_date",
    hours_utc,
    cgndb_id,
    air_quality_value,
    download_link,
    src_filename,
    last_updated,
    NOW() AT TIME ZONE 'EST' AS last_inserted
  FROM(
      SELECT *
   , ROW_NUMBER() OVER(PARTITION BY "the_date","hours_utc" ORDER BY hours_utc DESC) AS RN
      FROM STAGE.stg_monthly_air_data_transpose
  ) A WHERE RN=1);