<!-- TOC -->
* [Objective](#objective)
* [§0.Setup:](#0setup-)
  * [§0.1 Config.ini](#01-configini)
  * [§0.2: R Base Installation](#02-r-base-installation)
  * [§0.3: Python Requirements](#03-python-requirements)
  * [§0.4 Safari Remote Automation](#04-safari-remote-automation)
  * [§0.5 PostGresSQL Server](#05-postgressql-server)
  * [§0.6 Execution](#06-execution)
* [Pipeline](#pipeline)
  * [§0.1 Data Curation](#01-data-curation)
  * [§0.2 Execution Time](#02-execution-time)
  * [§0.3 Auto Machine Learning Layer](#03-auto-machine-learning-layer)
  * [§0.4 Execution Steps](#04-execution-steps)
  * [§0.5 Performance Testing](#05-performance-testing)
* [Data Curation Phases](#data-curation-phases)
  * [1. Staging (Extraction) Layer](#1-staging-extraction-layer)
    * [§0.1 Data Frames Object](#01-data-frames-object)
    * [§0.2 Configurations Object](#02-configurations-object)
    * [§1. Monthly Data Web Scraping](#1-monthly-data-web-scraping)
    * [§2. Monthly Forecasts Data Web Scraping](#2-monthly-forecasts-data-web-scraping)
    * [§3. Traffic Volume](#3-traffic-volume)
    * [§4. Geographical Names Data](#4-geographical-names-data)
    * [§5. ArcGIS Toronto and Peel Traffic Count](#5-arcgis-toronto-and-peel-traffic-count)
  * [2. Transformation Layer](#2-transformation-layer)
    * [§1. Monthly Air Data Transpose](#1-monthly-air-data-transpose)
  * [3. Production (Loading) Layer:](#3-production-loading-layer)
    * [§1. Monthly Air Data](#1-monthly-air-data)
    * [§2. Monthly Forecasts](#2-monthly-forecasts)
    * [§3. Traffic Volume](#3-traffic-volume-1)
    * [§4. Geographical Names Data](#4-geographical-names-data-1)
    * [§5. ArcGIS Toronto and Peel Traffic Count](#5-arcgis-toronto-and-peel-traffic-count-1)
    * [§6. Data Model Performance](#6-data-model-performance)
    * [§7. fact_air_data_proj](#7-fact_air_data_proj)
    * [§8. fact_gta_traffic_proj](#8-fact_gta_traffic_proj)
    * [§9. fact_hourly_avg](#9-fact_hourly_avg)
    * [§10. fact_weekdays_avg](#10-fact_weekdays_avg)
<!-- TOC -->


# Objective
To create interactive WebMaps embedded with Auto Machine Learning Layer with little to no user input from nearly-live data for a unified, geospatial analysis powered by one single, automated pipeline.

# §0.Setup: 
## §0.1 Config.ini
The **ONLY** file that requires user input is [**Config.ini**](Pipeline/config.ini) in [**Pipeline**](Pipeline/) folder.  
* **[postgres_db](Pipeline/config.ini#L1-L6)**:
   - Enter the database credentials without quotation marks 

 
 - **[auto_ml](Pipeline/config.ini#L8-L11)**:
   * **run_time_seconds**: accepts an integer greater than or equal to 0. The default duration is 0 for unlimited execution time till models are constructed.
   * **forecast_horizon**: accepts an integer greater than 0 which is the 'unit' time needed to forecast ahead of the last reported date per weather, traffic, and pedestrian stations.
   * **forecast_frequency**: sets the forecast frequency to generate the number of 'units' of the forecast horizon.
     - Acceptable values are: 
       - _Hour_, _Hourly_, _Day_, _Daily_, _Month_, _Monthly_, _Year_, _Yearly_, _Annual_, _Annually_, _Quarter_, _Quarterly_.</br></br>
 - **[api_tokens](Pipeline/config.ini#L13-L14)**:
   * follows the convention _platform_name_ = _token_ without quotation marks. The starting example mapbox = __token__ </br></br>

 - **[run_conditions](Pipeline/config.ini#L16-L21)**:
   * **[save_locally](Pipeline/config.ini#L17)**: Boolean Value _True_ or _False_ to store local copies of the database tables as csv files in [/Data/](https://github.com/amr-y-shalaby/ggr_472_project/blob/1de42fae911463b23a1b6c9294f05cf5e2ab7fa3/Data) Folder. Default is _True_.
     * If **save_locally** is set to True, copies of both the staging and public tables will be stored to [/Data/](Data Folder).
     * Data Model Performance and Maps Creation Performance Tables are saved locally.
   * **[create_tables](Pipeline/config.ini#L18)**: Boolean Value _True_ or _False_. Default is _True_.
     * This creates the database tables from web scraping to ingestion. 
     * If the database tables are already created, they get dropped and web scraping is initiated.
     * If **create_tables** is set to _False_, no web scraping and data refresh.  All the tables in Public Schema will be used to generate the maps.</br></br>
   * **[show_maps](Pipeline/config.ini#L19)**: Boolean Value _True_ or _False_. Default is _True_. 
     * runs [maps_tester.py](Pipeline/maps_tester.py) to determine optimal browser (Firefox, Google Chrome, or Safari) with the lowest loading time per generated HTML file in [Maps Folder](Maps).
     * Only MacOS is supported by runs [maps_tester.py](Pipeline/maps_tester.py).
     * If show_maps is set to _False_, HTML files will be generated but **not** displayed in the optimal browser and **not** tested.</br></br>
   * **[run_auto_ml](Pipeline/config.ini#L20)**: Boolean Value _True_ or _False_.
     * Determines if the Predictions are to be embedded into the maps or skipped. 
     * If run_auto_ml is set to **False**, the maps will contain only the web scraped data without the prediction layers.</br></br>
   * **[map_types](Pipeline/config.ini#L21)**: comma-seperated values of Turf, Mapbox, Folium.
     * The inputs are not case-sensitive, without quotation marks, and specifies the desired map types.
     * Acceptable values are:
       * _Turf_, _Mapbox_, and _Folium_.</br></br>


[**Config.ini**](Pipeline/config.ini) Contents:

|    [postgres_db]     | [auto_ml]                  | [api_tokens] | [run_conditions]                 |
|:--------------------:|----------------------------|--------------|----------------------------------|
|   host = localhost   | run_time_seconds = 300     | mapbox = ... | save_locally = False             |
| db_name   = postgres | forecast_horizon = 30      |              | create_tables = True             |
|   user = postgres    | forecast_frequency = Daily |              | show_maps = True                 |
| password = postgres  |                            |              | run_auto_ml = True               |
|     port = 5432      |                            |              | map_types = folium, mapbox, turf |

## §0.2: R Base Installation
R Base needs to be installed.  
* [R-Base for Mac](https://cran.r-project.org/bin/macosx/big-sur-arm64/base/R-4.3.3-arm64.pkg)
* [R-Base for Windws](https://cran.r-project.org/bin/windows/base/R-4.3.3-win.exe)
* [R-Base for Linux](https://cran.r-project.org/bin/linux/ubuntu/fullREADME.html)</br></br>


## §0.3: Python Requirements
You need to sync the environment python requirements to the following packages:
* pandas~=2.1.3
* requests~=2.31.0
* SQLAlchemy~=2.0.23
* rpy2~=3.5.14
* wget~=3.2
* beautifulsoup4~=4.12.2
* geopandas~=0.14.1
* locust~=2.24.0
* h2o~=3.44.0.3
* ipyleaflet~=0.18.2
* altair~=5.2.0
* h2o~=3.44.0.3
* ipyleaflet~=0.18.2
* altair~=5.2.0
* selenium~=4.18.1
* psycopg2-binary~=2.9.9
* plotly~=5.20.0
* GeoAlchemy2~=0.14.6
* googledrivedownloader~=0.4
* setuptools~=69.2.0


## §0.4 Safari Remote Automation
To measure maps loading times, Safari needs to have the option 'Allow Remote Automation' enabled in 'Developer' menu.
![Safari Remote Automation](Images/safari_allow_remote_automation.jpg)

## §0.5 PostGresSQL Server
<img src="https://miro.medium.com/v2/resize:fit:1358/format:webp/1*Z_zqyvst2R2YehYM2piUfA.png" alt="PostGres SQL on Dockers" width="350" height="200"/></br>
* To setup PostGresSQL Server using dockers please [refer to this guide](https://trevorstanley.medium.com/setup-postgresql-with-postgis-on-docker-8801637a766c).
* The needed schemas, _Public_ and _Stage_, as well as the PostGIS Extension are handled internally in [database initialization function](Pipeline/data_extractor.py#L172-L179).

## §0.6 Execution
After modifying [Config.ini](Pipeline/config.ini), run the python script [Main.py](Pipeline/main.py).

# Pipeline
## §0.1 Data Curation
**Data Curation:**</br></br>
![Report](Images/pipeline_steps.png)</br></br>
**Data Augmentation for Web Maps**:</br></br>
![Report](Images/auto_ml_layer.jpg)


## §0.2 Execution Time
![Execution Time Per Phase](Images/pipeline_execution_time.jpg)

## §0.3 Auto Machine Learning Layer
![](Images/h2o_automl_flowchart.png)

## §0.4 Execution Steps
![Execution Steps](Images/execution_flow_chart.jpg)

## §0.5 Performance Testing
* The optimal browser is **Safari** across _all_ map types. 
* Should a specific HTML file loads faster in a specific browser other than Safari, that HTML file will be loaded in its **respective** fastest browser.
* Only **MacOS** is supported.

![Optimal Browsers](Images/load_testing.jpg)


# Data Curation Phases

## 1. Staging (Extraction) Layer
### §0.1 Data Frames Object
The Data Frames are stored in a single object and accessible via a dictionary that follows the following naming convention:
* dfs_obj.**pandas_dfs**: contains the Pandas data frames of all the tables in Public Schema. It follows the naming convention of dfs_obj.**pandas_dfs**['**public_table_name**'].
  * For example, if the public table name is _**fact_combined_air_data**_ then it can be accessed by dfs_obj.pandas_dfs['**fact_combined_air_data**']</br></br>
* dfs_obj.**geopandas_dfs**: contains the GeoPandas data frames of only the **projected** tables in Public Schema with a geometry column. It follows the naming convention of dfs_obj.geo_pandas_dfs['public_table_'name'].
  * For example, if the projected public table name is _**fact_gta_traffic_proj**_ then it can be accessed by dfs_obj.geopandas_dfs['**fact_gta_traffic_proj**']</br></br>
* dfs_obj.**h2o_dfs**: contains the H2O data frames of all the tables in Public Schema. It follows the naming convention of dfs_obj.h2o_dfs['**public_table_name**'].
  * For example, if the public table name is _**fact_traffic_volume**_ then it can be accessed by dfs_obj.h2o_dfs['**fact_traffic_volume**']</br></br>

### §0.2 Configurations Object
The parsed configurations from [Config.ini](Pipeline/config.ini) is stored in a single object **configs_obj** that has the following the attributes:
* configs_obj._**run_conditions_dictionary_** contains the dictionary of the _**run_conditions**_ segment in [Config.ini](Pipeline/config.ini#L16-L22)
  * configs_obj.**run_conditions**['**save_locally**'] is the processed [save_locally flag](Pipeline/config.ini#L17-L17)
  * configs_obj.**run_conditions**['**parent_dir**'] is the processed [parent_directory](Pipeline/data_extractor.py#L47-L47)
  * configs_obj.**run_conditions**['**create_tables**'] is the processed  [create_tables flag](Pipeline/config.ini#L18-L18)
  * configs_obj.**run_conditions**['**show_maps**'] is the processed  [show_maps flag](Pipeline/config.ini#L19-L19)
  * configs_obj.**run_conditions**['**run_auto_ml**'] is the processed  [run_auto_ml flag](Pipeline/config.ini#L20-L20)
  * configs_obj.**run_conditions**['**map_types**'] is the processed  [map_types](Pipeline/config.ini#L21-L21)</br></br>
  
* configs_obj._**auto_ml**_ dictionary contains the dictionary of the _**auto_ml**_ segment in [Config.ini](Pipeline/config.ini#L8-L11)
  * configs_obj.**auto_ml**['**run_time_seconds**'] is the processed [run_time_seconds integer](Pipeline/config.ini#L9-L9)
  * configs_obj.**auto_ml**['**forecast_horizon**'] is the processed [forecast_horizon integer](Pipeline/config.ini#L10-L10)
  * configs_obj.**auto_ml**['**forecast_frequency**'] is the processed  [forecast_frequency](Pipeline/config.ini#L11-L11)

* configs_obj._**database**_ is the dictionary of the _**database**_ segment in [Config.ini](Pipeline/config.ini#L1-L6)
  * configs_obj.**database**['**host**'] is the processed [host server](Pipeline/config.ini#L2-L2)
  * configs_obj.**database**['**db_name**'] is the processed [db_name database name](Pipeline/config.ini#L3-L3)
  * configs_obj.**database**['**user**'] is the processed  [user name](Pipeline/config.ini#L4-L4)
  * configs_obj.**database**['**password**'] is the processed  [the user's password](Pipeline/config.ini#L5-L5)
  * configs_obj.**database**['**port**'] is the processed  [port integer](Pipeline/config.ini#L6-L6)
  * configs_obj.**database** dictionary also holds the **database** **connectors** which are initialized only **once** throughout execution phase to avoid slow database authentications at every step.
    * configs_obj.**database**['**pg_engine**] is the [PostgreSQL database adapter](https://pypi.org/project/psycopg2/)
    * configs_obj.**database**['**sqlalchemy_engine**'] is the [SQLAlchemy is the Python SQL toolkit and Object Relational Mapper](https://www.sqlalchemy.org/)</br></br>


### §1. Monthly Data Web Scraping
To download the Ontario monthly air quality data from https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/, function _extract_monthly_data()_in the Data Extractor [data_extractor.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_extractor.py) File.
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


### §2. Monthly Forecasts Data Web Scraping
To download the Ontario monthly air quality data from https://dd.weather.gc.ca/air_quality/aqhi/ont/forecast/model/csv/, the function _extract_monthly_forecasts()_ in the Data Extractor [data_extractor.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_extractor.py) File.
which ingests the public data into the **third** staging table "**stg_monthly_forecasts**" with the option to locally appending all the individual CSV files into one file with a prefix **'FORECAST_'** to differentiate it from the actual monthly data.


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


### §3. Traffic Volume
Using the REST API provided for Toronto Traffic Volume https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/, the function _extract_traffic_volume()_ in the [data_extractor.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_extractor.py) File.
which ingests the traffic volume data into the **fourth** staging table "**stg_traffic_volume**" with the option to locally save the CSV file to _['./Data/traffic_volume.csv'](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Data/traffic_volume.csv).

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

### §4. Geographical Names Data
Downloads and extracts the zip file of the Geographical Names Data from https://natural-resources.canada.ca/earth-sciences/geography/download-geographical-names-data/9245, the function _extract_geo_names()_ in the [data_extractor.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_extractor.py).
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

### §5. ArcGIS Toronto and Peel Traffic Count
Downloads Toronto and Peel Traffic Count from https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da, the function _extract_geo_names()_ in the [data_extractor.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_extractor.py).
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
|     download_link      |       text       |
|      src_filename      |       text       |


## 2. Transformation Layer
### §1. Monthly Air Data Transpose
The staging table _stg_monthly_air_data_transpose_ created from transposing the Ontario monthly air quality (https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/) to be ingested and converted 
into the **second** production table "**FACT_MONTHLY_AIR_DATA_TRANSPOSE** in _**"PUBLIC"**_ Schema via the execution of [data_transformer.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_transformer.py) with the following two conditions:

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


## 3. Production (Loading) Layer:
### §1. Monthly Air Data
The staging table _stg_monthly_air_data_ created from the Ontario monthly air quality (https://dd.weather.gc.ca/air_quality/aqhi/ont/observation/monthly/csv/) ingested and converted into the **first** production table "**FACT_MONTHLY_AIR_DATA** in _**"PUBLIC"**_ Schema via [data_loader.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_loader.py) with the following two conditions:

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


### §2. Monthly Forecasts
The staging table _stg_monthly_forecasts_ acquired from the Ontario monthly air quality data(https://dd.weather.gc.ca/air_quality/aqhi/ont/forecast/model/csv/) is ingested into the production table _fact_monthly_forecasts_ in _PUBLIC_ schema by [data_loader.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_loader.py) with the following two conditions:

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

### §3. Traffic Volume
The staging table _stg_traffic_volume_ constructed from the REST API provided for Toronto Traffic Volume https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/, is ingested into production _Public_ Schema as _FACT_TRAFFIC_VOLUME_ by [data_loader.py](https://github.com/amr-y-shalaby/GGR473_Project/blob/main/Pipeline/data_loader.py) with the following two conditions:

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
| latest_count_date |       date       |
|   download_link   |       text       |
|     filename      |       text       |
|   last_updated    |    timestamp     |
|   last_inserted   |    timestamp     |

### §4. Geographical Names Data
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
|    download_link     |       text       |
|     src_filename     |       text       |
|     last_updated     |    timestamp     |
|    last_inserted     |    timestamp     |


### §5. ArcGIS Toronto and Peel Traffic Count
The staging table _stg_gta_traffic_arcgis_ obtained from ArcGIS Toronto and Peel Traffic Data(https://www.arcgis.com/home/item.html?id=4964801ff5de475a80c51c5d54a9c8da) was ingested into the production table _FACT_GTA_TRAFFIC_ARCGIS_ in PUBLIC schema by [data_loader.py](Pipeline/data_loader.py) with the following two conditions:

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
|     download_link      |       text       |
|      src_filename      |       text       |
|      last_updated      |    timestamp     |
|     last_inserted      |    timestamp     |


### §6. Data Model Performance
After all the staging tables, sql scripts, and hard extractions made, _data_model_performance_tbl_ in _PUBLIC_ Schema provides insight on duration, scope, and complexity of the execution steps within each of the Extraction, Transformation, and Loading Phases which are tracked by [main.py](Pipeline/main.py).

| Column           |    Data Type     |
|------------------|:----------------:|
| Phase            |       text       |
| step_name        |       text       |
| duration_seconds | double precision |
| start_time       |    timestamp     |
| end_time         |    timestamp     |
| files_processed  | double precision |

| **phase**  |          **step_name**          | **duration_seconds** | **start_time** | **end_time** | **files_processed** |
|:----------:|:-------------------------------:|:--------------------:|:--------------:|:------------:|:-------------------:|
|   stage    |      extract_monthly_data       |        23.47         |    17:19.6     |   17:43.0    |         26          |
|   stage    |    extract_monthly_forecasts    |        283.59        |    17:43.0     |   22:26.6    |         534         |
|   stage    |     extract_traffic_volumes     |         7.27         |    22:26.6     |   22:33.9    |          1          |
|   stage    |     extract_geo_names_data      |        27.11         |    22:33.9     |   23:01.0    |          2          |
|   stage    |   extract_gta_traffic_arcgis    |         0.19         |    23:01.0     |   23:01.2    |          1          |
|   stage    |     transform_monthly_data      |        26.99         |    23:01.2     |   23:28.2    |          1          |
| production |       traffic_volume.sql        |        0.033         |    23:28.3     |   23:28.3    |          1          |
| production |     gta_traffic_arcgis.sql      |        0.009         |    23:28.3     |   23:28.3    |          1          |
| production |      monthly_air_data.sql       |        0.108         |    23:28.3     |   23:28.4    |          1          |
| production |   fact_monthly_forecasts.sql    |        0.193         |    23:28.4     |   23:28.6    |          1          |
| production | monthly_air_data_transposed.sql |        0.732         |    23:28.6     |   23:29.3    |          1          |
| production |          geo_names.sql          |        3.587         |    23:29.3     |   23:32.9    |          1          |
| production |       traffic_volume.sql        |        0.045         |    23:32.9     |   23:33.0    |          1          |
| production |      monthly_air_data.sql       |        0.079         |    23:33.0     |   23:33.1    |          1          |
| production |   fact_monthly_forecasts.sql    |        0.178         |    23:33.1     |   23:33.2    |          1          |
| production | monthly_air_data_transposed.sql |        0.681         |    23:33.2     |   23:33.9    |          1          |
| production |          geo_names.sql          |        3.638         |    23:33.9     |   23:37.6    |          1          |
| production |       traffic_volume.sql        |        0.055         |    23:37.6     |   23:37.6    |          1          |
| production |   fact_monthly_forecasts.sql    |        0.162         |    23:37.6     |   23:37.8    |          1          |
| production | monthly_air_data_transposed.sql |        0.676         |    23:37.8     |   23:38.4    |          1          |
| production |          geo_names.sql          |        3.696         |    23:38.4     |   23:42.1    |          1          |
| production |       traffic_volume.sql        |        0.055         |    23:42.1     |   23:42.2    |          1          |
| production | monthly_air_data_transposed.sql |        0.697         |    23:42.2     |   23:42.9    |          1          |
| production |          geo_names.sql          |        2.760         |    23:42.9     |   23:45.7    |          1          |
| production |       traffic_volume.sql        |        0.036         |    23:45.7     |   23:45.7    |          1          |
| production |          geo_names.sql          |        2.973         |    23:45.7     |   23:48.7    |          1          |
| production |       traffic_volume.sql        |        0.067         |    23:48.7     |   23:48.7    |          1          |
| production |       traffic_volume.sql        |        0.015         |    23:48.7     |   23:48.7    |          1          |
| production |      combine_air_data.sql       |        0.521         |    23:48.8     |   23:49.3    |          1          |

### §7. fact_air_data_proj
The table _fact_air_data_proj_ in _PUBLIC_ schema serves as the POSTGIS Version of fact_air_data with additional geometry column 'geom' created in Python via [data_transformer.py](Pipeline/data_transformer.py) with the inherited properties of the geolocation name identifiers from the curated table _dim_geo_names_.

| Column                           |         Data Type          |
|----------------------------------|:--------------------------:|
| **geom**                         | **geometry(Point, 26917)** |
| the_date                         |            date            |
| hour_utc                         |           bigint           |
| cgndb_id                         |            text            |
| air_quality_value                |      double precision      |
| donwnload_link                   |            text            |
| src_filename                     |            text            |
| last_updated                     |         timestamp          |
| last_inserted                    |         timestamp          |
| geo_lat                          |      double precision      |
| geo_long                         |      double precision      |
| province_territory               |            text            |
| geo_location                     |            text            |
| geo_decision_date                |         timestamp          |
| concise_code                     |            text            |
| generic_category                 |            text            |
| generic_term                     |            text            |
| geographical_name                |            text            |
| geo_names_second_from_extraction |      double precision      |
| weekday                          |            text            |


### §8. fact_gta_traffic_proj
The table _fact_gta_traffic_proj_ in _PUBLIC_ schema serves as the POSTGIS Version of fact_gta_traffic_arcgis via [data_transformer.py](Pipeline/data_transformer.py) with additional geometry column 'geom' created in Python via [data_transformer.py](Pipeline/data_transformer.py).

|         Column         |         Data Type          |
|:----------------------:|:--------------------------:|
|        **geom**        | **geometry(Point, 26917)** |
|        objectid        |           bigint           |
|         tcs__          |      double precision      |
|          main          |            text            |
|     midblock_route     |            text            |
|      side_1_route      |            text            |
|      side_2_route      |            text            |
|    activation_date     |            date            |
|        latitude        |      double precision      |
|       longitude        |      double precision      |
|        latitude        |      double precision      |
|       longitude        |      double precision      |
|       count_date       |            date            |
|  f8hr_vehicle_volume   |      double precision      |
| f8hr_pedestrian_volume |      double precision      |
|     download_link      |            text            |
|      src_filename      |            text            |
|      last_updated      |         timestamp          |
|     last_inserted      |         timestamp          |

### §9. fact_hourly_avg
The table _fact_hourly_avg_ contains the calculated means for the hourly segments per station and converted POSTGIS table via [data_transformer.py](Pipeline/data_transformer.py).

|   Column    |         Data Type          |
|:-----------:|:--------------------------:|
|  **geom**   | **geometry(Point, 26917)** |
|  cgndb_id   |            text            |
|   geo_lat   |      double precision      |
|  geo_long   |      double precision      |
|  dawn_avg   |      double precision      |
| morning_avg |      double precision      |
|  noon_avg   |      double precision      |
| evening_avg |      double precision      |

### §10. fact_weekdays_avg
The table _fact_weekdays_avg_ contains the calculated means for the weekdays per station and converted POSTGIS table via [data_loader.py](Pipeline/data_loader.py) that executes the query [create_post_proj_tbl.sql](Pipeline/data_transformer.sql).

|    Column    |       Data Type        |
|:------------:|:----------------------:|
|     geom     | geometry(Point, 26917) |
|   cgndb_id   |          text          |
|   latitude   |     numeric(10, 2)     |
|  longitude   |     numeric(10, 2)     |
|  monday_avg  |     numeric(10, 2)     |
| tuesday_avg  |     numeric(10, 2)     |
| thursday_avg |     numeric(10, 2)     |
|  friday_avg  |     numeric(10, 2)     |
| saturday_avg |     numeric(10, 2)     |
|  sunday_avg  |     numeric(10, 2)     |

