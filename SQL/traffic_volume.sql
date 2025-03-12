DROP TABLE IF EXISTS PUBLIC.FACT_TRAFFIC_VOLUME;
CREATE TABLE PUBLIC.FACT_TRAFFIC_VOLUME AS (
    SELECT
      "X_id" as "_id"
    , location_id
    , location
    , lng
    , lat
    , centreline_type
    , centreline_id
    , px
    , count_date as latest_count_date
    , download_link
    , src_filename
    , last_updated
    , (NOW() AT TIME ZONE 'EST') AS last_inserted
    FROM(
      SELECT *, ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY count_date DESC) RN
      FROM STAGE.stg_traffic_volume
   ) A WHERE RN=1);

