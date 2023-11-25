DROP TABLE IF EXISTS PUBLIC.FACT_COMBINED_AIR_DATA;
CREATE TABLE PUBLIC.FACT_COMBINED_AIR_DATA AS(
WITH GEO_NAMES AS(
    SELECT * FROM dim_geo_names
), monthly_air_data_transpose AS(
    SELECT * FROM fact_monthly_air_data_transpose
), EXPORT AS(
  SELECT
    UPPER(G.cgndb_id)   cgndb_id
  , G.latitude  GEO_LAT
  , G.longitude GEO_LONG
  , G.province_territory
  , G.location   GEO_LOCATION
  , G.decision_date GEO_DECISION_DATE
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