DROP TABLE IF EXISTS PUBLIC.FACT_MONTHLY_FORECASTS;
CREATE TABLE PUBLIC.FACT_MONTHLY_FORECASTS AS(
WITH DIM_GEO_NAMES AS(
  SELECT * FROM(
    SELECT *, ROW_NUMBER() OVER(PARTITION BY cgndb_id ORDER BY decision_date DESC) AS RN
    FROM STAGE.stg_geo_names) A
    WHERE A.RN = 1
), MONTHLY_FORECASTS AS(
  SELECT
    cgndb_code,
    community_name,
    validity_date,
    validity_time_utc,
    period,
    value,
    amended,
    download_link,
    src_filename,
    last_updated,
    NOW() AT TIME ZONE 'EST' AS last_inserted
  FROM(
      SELECT *
   , ROW_NUMBER() OVER(PARTITION BY "cgndb_code", "validity_date" ORDER BY validity_time_utc DESC) AS RN
      FROM STAGE.stg_monthly_forecasts
  ) A WHERE RN=1
), EXPORT AS(
  SELECT
    M.cgndb_code,
    D.latitude,
    D.longitude,
    D.generic_category,
    D.geographical_name,
    D.decision_date,
    D.location,
    D.province_territory,
    M.community_name,
    M.validity_date,
    M.validity_time_utc,
    M.period,
    M.value,
    M.amended,
    M.download_link,
    M.src_filename,
    M.last_updated,
    NOW() AT TIME ZONE 'EST' AS last_inserted
   FROM
   MONTHLY_FORECASTS AS M
   INNER JOIN DIM_GEO_NAMES AS D ON UPPER(D.cgndb_id) = UPPER(M.cgndb_code)
) SELECT * FROM EXPORT);