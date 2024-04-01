DROP TABLE IF EXISTS PUBLIC.FACT_COMBINED_AIR_DATA;
CREATE TABLE PUBLIC.FACT_COMBINED_AIR_DATA AS(
WITH GEO_NAMES AS(
        SELECT
      A.cgndb_id
    , A.geographical_name
    , A.language
    , A.syllabic_form
    , A.generic_term
    , A.generic_category
    , A.concise_code
    , A.toponymic_feature_id
    , A.latitude
    , A.longitude
    , A.location
    , A.province_territory
    , A.relevance_at_scale
    , A.decision_date
    , A.source
    , A.download_link
    , A.src_filename
    , A.last_updated
    , (NOW() AT TIME ZONE 'EST') AS last_inserted
    FROM(
        SELECT
        *
      , ROW_NUMBER() OVER(PARTITION BY cgndb_id ORDER BY decision_date DESC) AS rn
    FROM stage.stg_geo_names) A
    WHERE rn = 1
), MONTHLY_AIR_DATA_FILETERED AS(
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
  ) A WHERE RN=1
), monthly_air_data_transpose AS(
    SELECT * FROM MONTHLY_AIR_DATA_FILETERED
), EXPORT AS(
  SELECT
    UPPER(G.cgndb_id)   cgndb_id
  , G.latitude
  , G.longitude
  , G.province_territory
  , G.location
  , G.decision_date
  , G.concise_code
  , G.generic_category
  , G.generic_term
  , G.geographical_name
  , EXTRACT('SECOND' FROM G.last_inserted - G.last_updated)     GEO_NAMES_SECOND_FROM_EXTRACTION
  , DATE(M.the_date) AS "the_date"
  , EXTRACT('Year' FROM M.the_date) AS "year"
  , EXTRACT('Month' FROM M.the_date) AS "month"
  , TO_CHAR(M.the_date, 'Month') AS "the_month"
  , TO_CHAR(M.the_date, 'Day')  AS "weekday"
  , DATE_PART('week', M.the_date) AS week_number
  , M.hours_utc
  , CASE WHEN hours_utc BETWEEN 0 AND 5 THEN 'DAWN_0_TO_5'
         WHEN hours_utc BETWEEN 6 AND 11 THEN 'MORNING_6_TO_11'
         WHEN hours_utc BETWEEN 12 AND 17 THEN 'NOON_12_TO_17'
         WHEN hours_utc BETWEEN 18 AND 23 THEN 'EVENING_18_TO_23'
         ELSE 'Unknown' END          AS PHASE_HOUR_UTC
  , M.air_quality_value
  , EXTRACT('SECOND' FROM M.last_inserted - M.last_updated)   AS AIR_DATA_SECONDS_FROM_EXTRACTION
  , M.last_updated
  , M.last_inserted
  , M.src_filename
   FROM
     monthly_air_data_transpose M
 INNER JOIN GEO_NAMES G ON UPPER(G.cgndb_id) = UPPER(M.cgndb_id)
) SELECT * FROM EXPORT);