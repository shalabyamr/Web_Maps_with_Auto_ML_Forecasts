# GGR473 : GGR473 Group Project

## Members:
|     Name     | Student Number |             Email             |
|:------------:|:--------------:|:-----------------------------:|
| Matthew Ruku |   1000881272   | matthew.ruku@mail.utoronto.ca |
|   Ryan Siu   |   1007268519   |   ryan.siu@mail.utoronto.ca   |
| Amr Shalaby  |   1005280397   | amr.shalaby@mail.utoronto.ca  |


## Pipeline Design

## 1. Staging Layer:
### 1. Monthly Data Web Scraping:
To download the Ontario monthly air quality data from https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/, function _extract_monthly_data(boolean save_loally)_in the the Data Loader is /Python/Data_Loader.py File.
which ingests the public data into the **first** staging table "**stg_monthly_air_data**."

| Column         | Data Type        |
|----------------|------------------|
| the_date       | date             |
| hour_utc       | bigint           |
| FAFFD          | double precision |
| FALIF          | double precision |
| FALJI          | double precision |
| FAMXK          | double precision |
| FAYJG          | double precision |
| FAZKI          | double precision |
| FBKKK          | double precision |
| FBLJL          | double precision |
| FBLKS          | double precision |
| FCAEN          | double precision |
| FCCOT          | double precision |
| FCFUU          | double precision |
| FCGKZ          | double precision |
| FCIBD          | double precision |
| FCKTB          | double precision |
| FCNJT          | double precision |
| FCTOV          | double precision |
| FCWFX          | double precision |
| FCWOV          | double precision |
| FCWYG          | double precision |
| FDATE          | double precision |
| FDCHU          | double precision |
| FDEGT          | double precision |
| FDGED          | double precision |
| FDGEJ          | double precision |
| FDGEM          | double precision |
| FDJFN          | double precision |
| FDMOP          | double precision |
| FDQBU          | double precision |
| FDQBX          | double precision |
| FDSUS          | double precision |
| FDZCP          | double precision |
| FEAKO          | double precision |
| FEARV          | double precision |
| FEBWC          | double precision |
| FEUTC          | double precision |
| FEUZB          | double precision |
| FEVJR          | double precision |
| FEVJS          | double precision |
| FEVJV          | double precision |
| FEVNS          | double precision |
| FEVNT          | double precision |
| last_updated   | timestamp        |
| download_link | text             |
| src_filename   | text             |

### 2. Monthly Data Transposd:
The download Ontario monthly air quality data(https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/), was unusable as each column name was the 
geo_station_id in other tables making it possible for a row join.  The staging table _stg_monthly_air_data_ from the previous step had to transposed to 
a new staging table _stg_monthly_air_data_transpos_ via the revised function 
_extract_monthly_data(boolean save_loally)_in the the Data Loader is /Python/Data_Loader.py File.
which ingests the public data into the **second** staging table "**stg_monthly_air_data_**transposed."


| Column            | Data Type        |
|-------------------|------------------|
| the_date          | date             |
| hour_utc          | bigint           |
| cgndb_id          | text             |
| air_quality_value | double precision |
| donwnload_link    | text             |
| src_filename      | text             |
| last_updated      | timestamp        |


### 3. Monthly Forecasts Data Web Scraping:
To download the Ontario monthly air quality data from https://dd.weather.gc.ca/air_quality/aqhi/ont/forecast/model/csv/, the function _extract_monthly_forecasts(boolean save_locally)_ in the Data Loader located at /Python/Data_Loader.py File.
which ingests the public data into the **third** staging table "**stg_monthly_forecasts**" with the option to locally save the CSV files with a prefix **'FORECAST_'** to differentiate them from the actual monthly data.


|      Column       | Data Type |
|:-----------------:|:---------:|
|    cgndb_code     |   text    |
|  community_name   |   text    |
|   validity_date   |   date    |
| validity_time_utc |   text    |
|      period       |  bigint   |
|       value       |  bigint   |
|      amended      |  bigint   |
|   last_updated    | timestamp |
|  donwnload_link   |   text    |
|   src_filename    |   text    |


### 4. Traffic Volume:
Using the REST API provided for Toronto Traffic Volume https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/, the function _extract_traffic_volumes(boolean save_locally)_ in the Data Loader located at /Python/Data_Loader.py File.
which ingests the traffic volume data into the **fourth** staging table "**stg_traffic_volume**" with the option to locally save the CSV file to _'./Data/traffic_volume.csv'_.

|      Column       |    Data Type     |
|:-----------------:|:----------------:|
|        _id        |      bigint      |
|    location_id    |      bigint      |
|     location      |       text       |
|        lng        | double precision |
|        lat        | double precision |
|  centreline_type  | double precision |
|   centreline_id   | double precision |
|        px         | double precision |
| latest_count_date |       text       |
|   download_link   |       text       |
|     filename      |       text       |
|   last_updated    |    timestamp     |

### 5. Geographical Names Data:
Downloads and extracts the zip file of the Geographical Names Data from https://natural-resources.canada.ca/earth-sciences/geography/download-geographical-names-data/9245, the function _extract_geo_names(boolean save_locally)_ in the Data Loader located at /Python/Data_Loader.py File.
which ingests the geographical names data containing the coordinates of the air quality stations into the **fifth** staging table "**stg_geo_names**" with the option to retain the extracted CSV file to _'./Data/cgn_canada_csv_eng.csv'_.

|        Column        |    Data Type     |
|:--------------------:|:----------------:|
|       cgndb_id       |       text       |
|  geographical_name   |       text       |
|       language       |       text       |
|    syllabic_form     |       text       |
|     generic_term     |       text       |
|   generic_category   |       text       |
|     concise_code     |       text       |
| toponymic_feature_id |       text       |
|       latitude       | double precision |
|      longitude       | double precision |
|       location       |       text       |
|  province_territory  |       text       |
|  relevance_at_scale  |      bigint      |
|    decision_date     |       date       |
|        source        |       text       |
|     last_updated     |    timestamp     |
|    download_link    |       text       |
|     src_filename     |       text       |

### 6. ArcGIS Toronto and Peel Traffic Count:
Downloads Toronto and Peel Traffic Count from https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da, the function _extract_geo_names(boolean save_locally)_ in the Data Loader located at /Python/Data_Loader.py File.
which ingests the Toronto and Peel Traffic Counts with latitude and longitude provided by ArcGIS  into the **sixth** staging table "**load_gta_traffic_arcgis**" with the option to create CSV file to _'./Data/ArcGIS_Toronto_and_Peel_Traffic.csv'_.

|         Column         |    Data Type     |
|:----------------------:|:----------------:|
|        objectid        |      bigint      |
|         tcs__          | double precision |
|          main          |       text       |
|     midblock_route     |       text       |
|      side_1_route      |       text       |
|      side_2_route      |       text       |
|    activation_date     |       date       |
|        latitude        | double precision |
|       longitude        | double precision |
|        latitude        | double precision |
|       longitude        | double precision |
|       count_date       |       date       |
|  f8hr_vehicle_volume   | double precision |
| f8hr_pedestrian_volume | double precision |
|      last_updated      |    timestamp     |
|     download_link     |       text       |
|      src_filename      |       text       |


## 2. Production Layer:
### 1. Monthly Air Data:
The staging table _stg_monthly_air_data_ creted from the Ontario monthly air quality (https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/) ingested and converted into the **first** production table "**FACT_MONTHLY_AIR_DATA** in _**"PUBLIC"**_ Schema with the following two conditions:

* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() OVER(PARTITION BY "Date","hours_utc" ORDER BY hours_utc DESC) = 1_to eliminate duplicate records within any given date.

|    Column     |    Data Type     |
|:-------------:|:----------------:|
|     date      |       date       |
|   hour_utc    |      bigint      |
|     FAFFD     | double precision |
|     FALIF     | double precision |
|     FALJI     | double precision |
|     FAMXK     | double precision |
|     FAYJG     | double precision |
|     FAZKI     | double precision |
|     FBKKK     | double precision |
|     FBLJL     | double precision |
|     FBLKS     | double precision |
|     FCAEN     | double precision |
|     FCCOT     | double precision |
|     FCFUU     | double precision |
|     FCGKZ     | double precision |
|     FCIBD     | double precision |
|     FCKTB     | double precision |
|     FCNJT     | double precision |
|     FCTOV     | double precision |
|     FCWFX     | double precision |
|     FCWOV     | double precision |
|     FCWYG     | double precision |
|     FDATE     | double precision |
|     FDCHU     | double precision |
|     FDEGT     | double precision |
|     FDGED     | double precision |
|     FDGEJ     | double precision |
|     FDGEM     | double precision |
|     FDJFN     | double precision |
|     FDMOP     | double precision |
|     FDQBU     | double precision |
|     FDQBX     | double precision |
|     FDSUS     | double precision |
|     FDZCP     | double precision |
|     FEAKO     | double precision |
|     FEARV     | double precision |
|     FEBWC     | double precision |
|     FEUTC     | double precision |
|     FEUZB     | double precision |
|     FEVJR     | double precision |
|     FEVJS     | double precision |
|     FEVJV     | double precision |
|     FEVNS     | double precision |
|     FEVNT     | double precision |
| download_link |       text       |
| src_filename  |       text       |
| last_updated  |    timestamp     |
| last_inserted |    timestamp     |

### 2. Monthly Air Data Transpose:
The staging table _stg_monthly_air_data_transpose_ created from the Ontario monthly air quality (https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/) ingested and converted 
into the **second** production table "**FACT_MONTHLY_AIR_DATA_TRANSPOSE** in _**"PUBLIC"**_ Schema with the following two conditions:

* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() OVER(PARTITION BY "the_date","hours_utc" ORDER BY hours_utc DESC) = 1_to eliminate duplicate records within any given date.


| Column            |    Data Type     |
|-------------------|:----------------:|
| the_date          |       date       |
| hour_utc          |      bigint      |
| cgndb_id          |       text       |
| air_quality_value | double precision |
| donwnload_link    |       text       |
| src_filename      |       text       |
| last_updated      |    timestamp     |
| last_inserted     |    timestamp     |

### 3. Monthly Forecasts:
The staging table _stg_monthly_forecasts_ acquired from the Ontario monthly air quality data(https://dd.weather.gc.ca/air_quality/aqhi/ont/forecast/model/csv/) is ingested into the production table _FACT_MONTHLY_FORECASTS_in _PUBLIC_ schema with the following two conditions:

* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() OVER(PARTITION BY "cgndb_code", "validity_date" ORDER BY validity_time_utc DESC)=1_ to eliminate the duplicated records within the provided dates.


|      Column       | Data Type |
|:-----------------:|:---------:|
|    cgndb_code     |   text    |
|  community_name   |   text    |
|   validity_date   |   date    |
| validity_time_utc |   text    |
|      period       |  bigint   |
|       value       |  bigint   |
|      amended      |  bigint   |
|  donwnload_link   |   text    |
|   src_filename    |   text    |
|   last_updated    | timestamp |
|   last_inserted   | timestamp |

### 4. Traffic Volume:
The staging table _stg_traffic_volume_ constructed from the REST API provided for Toronto Traffic Volume https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/, is ingested into production _Public_ Schema as _FACT_TRAFFIC_VOLUME_ with the following two conditions:

* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() OVER(PARTITION BY location_id ORDER BY latest_count_date DESC)=1_ to eliminate the duplicated records within the provided dates.


|      Column       |    Data Type     |
|:-----------------:|:----------------:|
|        _id        |      bigint      |
|    location_id    |      bigint      |
|     location      |       text       |
|        lng        | double precision |
|        lat        | double precision |
|  centreline_type  | double precision |
|   centreline_id   | double precision |
|        px         | double precision |
| latest_count_date |       text       |
|   download_link   |       text       |
|     filename      |       text       |
|   last_updated    |    timestamp     |
|   last_inserted   |    timestamp     |

### 5. Geographical Names Data:
The staging table _stg_geo_names_from the downloaded and extracted zip file of the Geographical Names Data(https://natural-resources.canada.ca/earth-sciences/geography/download-geographical-names-data/9245) was ingested into the production table _DIM_GEO_NAMES_ with the following two conditions:
* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() OVER(PARTITION BY cgndb_id ORDER BY decision_date DESC) =1_ to eliminate the duplicated records within the provided dates.


|        Column        |    Data Type     |
|:--------------------:|:----------------:|
|       cgndb_id       |       text       |
|  geographical_name   |       text       |
|       language       |       text       |
|    syllabic_form     |       text       |
|     generic_term     |       text       |
|   generic_category   |       text       |
|     concise_code     |       text       |
| toponymic_feature_id |       text       |
|       latitude       | double precision |
|      longitude       | double precision |
|       location       |       text       |
|  province_territory  |       text       |
|  relevance_at_scale  |      bigint      |
|    decision_date     |       date       |
|        source        |       text       |
|    donwnload_link    |       text       |
|     src_filename     |       text       |
|     last_updated     |    timestamp     |
|    last_inserted     |    timestamp     |


### 6. ArcGIS Toronto and Peel Traffic Count:
The staging table _stg_gta_traffic_arcgis_ obtained from ArcGIS Toronto and Peel Traffic Data(https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da) was ingested into the production table _FACT_GTA_TRAFFIC_ARCGIS_ in PUBLIC schema with the following two conditions:

* Added column "_last_inserted_" converted from UTC to EST to capture the time of insertion into production schema
* The condition _ROW_NUMBER() over (PARTITION BY objectid ORDER BY COUNT_DATE DESC) =1_ to eliminate the duplicated records within the provided dates.


|         Column         |    Data Type     |
|:----------------------:|:----------------:|
|        objectid        |      bigint      |
|         tcs__          | double precision |
|          main          |       text       |
|     midblock_route     |       text       |
|      side_1_route      |       text       |
|      side_2_route      |       text       |
|    activation_date     |       date       |
|        latitude        | double precision |
|       longitude        | double precision |
|        latitude        | double precision |
|       longitude        | double precision |
|       count_date       |       date       |
|  f8hr_vehicle_volume   | double precision |
| f8hr_pedestrian_volume | double precision |
|     donwnload_link     |       text       |
|      src_filename      |       text       |
|      last_updated      |    timestamp     |
|     last_inserted      |    timestamp     |

