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
  , ROUND(EXTRACT('SECOND' FROM G.last_inserted - G.last_updated),1)     GEO_NAMES_SECOND_FROM_EXTRACTION
  , M.the_date
  , M.hours_utc
  , M.air_quality_value
  , ROUND(EXTRACT('SECOND' FROM M.last_inserted - M.last_updated),1)   AS AIR_DATA_SECONDS_FROM_EXTRACTION
   FROM
     monthly_air_data_transpose M
 INNER JOIN GEO_NAMES G ON UPPER(G.cgndb_id) = UPPER(M.cgndb_id)
) SELECT * FROM EXPORT);