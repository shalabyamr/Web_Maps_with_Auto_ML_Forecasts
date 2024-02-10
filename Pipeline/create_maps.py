from execute_pipeline import execute_pipeline, sqlalchemy_engine, pg_engine, parent_dir
import folium
import pandas as pd
import datetime
from folium.plugins import MarkerCluster, HeatMap, HeatMapWithTime
import plotly.express as px
import geopandas as gpd

# Important ::::
# Comment out this line after the initial setup of the tables.
#execute_pipeline()
def create_maps(map_type:str):
    # Setting Up the Data for both map types
    start = datetime.datetime.now()
    query_get_tables = """SELECT table_name FROM information_schema.tables
        WHERE (table_schema = 'public') and (table_name not in(
        'spatial_ref_sys','geography_columns','geometry_columns','data_model_performance_tbl'))"""
    cur = pg_engine.cursor()
    cur.execute(query_get_tables)
    del query_get_tables
    public_tables = [item[0] for item in cur.fetchall()]
    print('Public Tables in Production Schema : ', public_tables,'\n*****************************')
    i = 1
    print('Creating Dataframes From Public Schema Tables as of: {}\n'.format(datetime.datetime.now()))
    for public_table in public_tables:
        print("{} of {}: Processing Public Table '{}':".format(i, len(public_tables), public_table, public_table))

        if 'proj' not in public_table:
            print("\tCreating Dataframe 'df_{}' from Table '{}' :".format(public_table, public_table))
            exec_statement = "df_{} = pd.read_sql_table(table_name='{}', con=sqlalchemy_engine, schema='public')".format(public_table, public_table)
            exec(exec_statement, globals())
        if 'proj' in public_table:
            print("\tCreating Projected Dataframe 'gpdf_{}' from Table '{}' :".format(public_table, public_table))
            gpd_statement = "gpdf_{} = gpd.read_postgis('SELECT * FROM public.{}', con=sqlalchemy_engine, geom_col='geom', crs='EPSG:26917')".format(public_table, public_table)
            exec(gpd_statement, globals())
        i = i+1

    end_dfs = datetime.datetime.now()
    dfs_total_seconds = (end_dfs - start).total_seconds()
    print('\nDone Creating DataFrames from Public Tables in {} seconds!\n*****************************'.format(dfs_total_seconds))

    temp_df = df_fact_traffic_volume.dropna()
    temp_df['latest_count_date'] = pd.to_datetime(temp_df['latest_count_date'])
    temp_df.sort_values(by=['latest_count_date'], inplace=True)
    temp_df.set_index('latest_count_date', inplace=True)
    # Heatmap With Time Series
    data = []
    for _, d in temp_df.groupby('latest_count_date'):
        data.append([[row['lat'], row['lng'], row['px']] for _, row in d.iterrows()])

    # Folium-Specific Code
    if map_type.upper() in('FOLIUM','BOTH'):
        # Load map centred
        toronto_map = folium.Map(location=[gpdf_fact_air_data_proj['latitude'].mean(), gpdf_fact_air_data_proj['longitude'].mean()], zoom_start=10, control_scale=True)
        marker_cluster = MarkerCluster()

        fg = folium.FeatureGroup(name='Air Quality Measures')
        for index, row in gpdf_fact_air_data_proj.iterrows():
            marker_cluster.add_child(folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup='Air Quality Measure: <b>{}</b><br>Geographical Name:<b>{}</b>.<br>CGN_ID: <b>{}</b>'.format(row['air_quality_value'], row['geographical_name'],row['cgndb_id']),
                icon=folium.Icon(color="black", icon="info-sign")))
        fg.add_child(marker_cluster)
        toronto_map.add_child(fg)

        for index, row in df_fact_traffic_volume.iterrows():
            folium.Marker(
                location=[row['lat'], row['lng']],
                popup='Traffic Volume: <b>{}</b><br>Location:<b>{}</b>.<br>Location ID: <b>{}</b>'.format(row['px'], row['location'],row['location_id']),
                icon=folium.Icon(color="red", icon="car")).add_to(marker_cluster)

        for index, row in df_fact_gta_traffic_arcgis.iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup='Vehicle Volume: <b>{}</b><br>Pedestrian Volume:<b>{}</b>.<br>Main Stn: <b>{}</b>'.format(row['f8hr_vehicle_volume'], row['f8hr_pedestrian_volume'],row['main']),
                icon=folium.Icon(color="green", icon="flag")).add_to(marker_cluster)

        HeatMap(df_fact_combined_air_data[['latitude','longitude','air_quality_value']],
                min_opacity=0.4,
                blur = 18).add_to(folium.FeatureGroup(name='Air Quality Heatmap').add_to(toronto_map))

        HeatMap(df_fact_combined_air_data[['latitude','longitude','air_quality_value']],
                min_opacity=0.4,
                blur = 18).add_to(folium.FeatureGroup(name='Air Quality Heatmap').add_to(toronto_map))

        HeatMap(data=zip(df_fact_gta_traffic_arcgis['latitude'], df_fact_gta_traffic_arcgis['longitude'],
                              df_fact_gta_traffic_arcgis['f8hr_pedestrian_volume']),
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Pedestrian Volume').add_to(toronto_map))


        HeatMap(data=zip(temp_df['lat'], temp_df.lng, temp_df['px']),
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Vehicle Volume').add_to(toronto_map))

        HeatMapWithTime(data, index=data, auto_play=True).add_to(folium.FeatureGroup(name='Traffic Time Heatmap')).add_to(toronto_map)
        folium.LayerControl().add_to(toronto_map)
        end_folium = datetime.datetime.now()
        folium_total_seconds = (end_folium-start).total_seconds()
        print('Done Generating the Folium Map in {} seconds as of {}'.format(folium_total_seconds, end_folium))
        toronto_map.save(parent_dir + '/Maps/Folium_Toronto.html')

    # Mapbox Specific Code
    if map_type.upper() in('MAPBOX', 'BOTH'):
        px.set_mapbox_access_token(open(parent_dir+"/Data/.mapbox_token").read())
        fig_air_quality_values = px.scatter_mapbox(gpdf_fact_air_data_proj,
                             lat=gpdf_fact_air_data_proj.geom.y,
                             lon=gpdf_fact_air_data_proj.geom.x,
                             hover_name="air_quality_value",
                             height=500, zoom=10)
        fig_air_quality_values.update_layout(mapbox_style="open-street-map")
        fig_air_quality_values.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig_air_quality_values.show()
        fig_air_quality_values.write_html(parent_dir+'/Maps/Mapbox_Air_Quality.html')

        fig_vehicle_heatmap = px.density_mapbox(gpdf_fact_gta_traffic_proj,
                            lat=gpdf_fact_gta_traffic_proj.geom.y,
                            lon=gpdf_fact_gta_traffic_proj.geom.x,
                            z='f8hr_vehicle_volume',
                            mapbox_style="open-street-map")
        fig_vehicle_heatmap.show()
        fig_vehicle_heatmap.write_html(parent_dir+'/Maps/Mapbox_Vehicle_HeatMap.html')

        fig_pedestrian_heatmap = px.density_mapbox(gpdf_fact_gta_traffic_proj,
                                                lat=gpdf_fact_gta_traffic_proj.geom.y,
                                                lon=gpdf_fact_gta_traffic_proj.geom.x,
                                                z='f8hr_pedestrian_volume',
                                                mapbox_style="open-street-map")
        fig_pedestrian_heatmap.show()
        fig_pedestrian_heatmap.write_html(parent_dir + '/Maps/Mapbox_Pedestrian_HeatMap.html')


        fig_traffic_volume = px.scatter_mapbox(gpdf_fact_gta_traffic_proj.dropna(),
                                lat=gpdf_fact_gta_traffic_proj.dropna().geom.y,
                                lon=gpdf_fact_gta_traffic_proj.dropna().geom.x,
                                hover_name='f8hr_vehicle_volume',
                                height=500, zoom=10)
        fig_traffic_volume.show()
        fig_traffic_volume.write_html(parent_dir+'/Maps/Mapbox_Traffic_Volume.html')

        end_mapbox = datetime.datetime.now()
        mapbox_total_seconds = (end_mapbox-start).total_seconds()
        print('Done Generating the Mapbox Map in {} seconds as of {}'.format(mapbox_total_seconds, end_mapbox))

# Create Maps Function takes 'folium', 'mapbox', or 'both' as parameters.
create_maps(map_type='both')