DROP TABLE IF EXISTS PUBLIC.FACT_GTA_TRAFFIC_ARCGIS;
CREATE TABLE PUBLIC.FACT_GTA_TRAFFIC_ARCGIS AS(
  SELECT
    A.objectid               ,
    A.tcs__                  ,
    A.main                   ,
    A.midblock_route         ,
    A.side_1_route           ,
    A.side_2_route           ,
    A.activation_date        ,
    A.latitude               ,
    A.longitude              ,
    A.count_date             ,
    A.f8hr_vehicle_volume    ,
    A.f8hr_pedestrian_volume ,
    A.download_link         ,
    A.src_filename           ,
    A.last_updated           ,
    NOW() AT TIME ZONE 'EST' AS last_inserted
  FROM(
      SELECT *
           , ROW_NUMBER() over (PARTITION BY objectid ORDER BY COUNT_DATE DESC) AS RN
      FROM STAGE.stg_gta_traffic_arcgis) A WHERE RN = 1);

DROP TABLE IF EXISTS PUBLIC.fact_gta_traffic_arcgis_avg;
CREATE TABLE PUBLIC.fact_gta_traffic_arcgis_avg AS(
    SELECT
     "main"
   , latitude
   , longitude
   , INITCAP(TO_CHAR(count_date,'DAY'))
   , CAST(AVG(f8hr_vehicle_volume) AS INT) AS VEHCILE_VOLUME_AVG
FROM PUBLIC.FACT_GTA_TRAFFIC_ARCGIS
GROUP BY 1,2,3,4);

DROP TABLE IF EXISTS PUBLIC.fact_gta_pedestrians_arcgis_avg;
CREATE TABLE PUBLIC.fact_gta_pedestrians_arcgis_avg AS(
    SELECT
     "main"
   , latitude
   , longitude
   , INITCAP(TO_CHAR(count_date,'DAY'))
   , CAST(AVG(f8hr_pedestrian_volume) AS INT) AS VEHCILE_VOLUME_AVG
FROM PUBLIC.FACT_GTA_TRAFFIC_ARCGIS
GROUP BY 1,2,3,4);
