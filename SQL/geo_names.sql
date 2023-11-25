DROP TABLE IF EXISTS PUBLIC.DIM_GEO_NAMES;
CREATE TABLE PUBLIC.DIM_GEO_NAMES AS(
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
    WHERE rn = 1);