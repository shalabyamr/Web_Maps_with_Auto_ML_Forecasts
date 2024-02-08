from execute_pipeline import execute_pipeline, sqlalchemy_engine, pg_engine, parent_dir
import folium
import pandas as pd
import datetime
from folium.plugins import MarkerCluster, HeatMap, HeatMapWithTime
import plotly.express as px

# Important ::::
# Comment out this line after the initial setup of the tables.
#execute_pipeline()
def create_maps(map_type:str):
    # Setting Up the Data for both map types
    start = datetime.datetime.now()
    query_get_tables = """SELECT table_name FROM information_schema.tables
        WHERE (table_schema = 'public') and (table_name not in('spatial_ref_sys','geography_columns',
        'geometry_columns','data_model_performance_tbl'))"""
    cur = pg_engine.cursor()
    cur.execute(query_get_tables)
    del query_get_tables
    public_tables = [item[0] for item in cur.fetchall()]
    print('Public Tables in Production Schema : ', public_tables,'\n*****************************')
    i = 1
    print('Creating Dataframes From Public Schema Tables as of: {}'.format(datetime.datetime.now()))
    for public_table in public_tables:
        print("{} of {}: Public Table '{}' in Production Schema:".format(i, len(public_tables), public_table, public_table))
        print("\tCreating Dataframe 'df_{}' from Table '{}' :".format(public_table, public_table))
        exec_statement = "df_{} = pd.read_sql_table(table_name='{}', con=sqlalchemy_engine, schema='public')".format(public_table, public_table)
        exec(exec_statement, globals())
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
        toronto_map = folium.Map(location=[df_fact_air_data_proj['latitude'].mean(), df_fact_air_data_proj['longitude'].mean()], zoom_start=10, control_scale=True)
        marker_cluster = MarkerCluster()

        fg = folium.FeatureGroup(name='Air Quality Measures')
        for index, row in df_fact_air_data_proj.iterrows():
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
        us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")
        fig = px.scatter_mapbox(us_cities, lat="lat", lon="lon", hover_name="City", hover_data=["State", "Population"],
                                color_discrete_sequence=["fuchsia"], zoom=3, height=300)
        fig.update_layout(mapbox_style="open-street-map")
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig.to_html(parent_dir + '/Maps/Mapbox_Toronto.html')
        end_mapbox = datetime.datetime.now()
        mapbox_total_seconds = (end_mapbox-start).total_seconds()
        print('Done Generating the Mapbox Map in {} seconds as of {}'.format(mapbox_total_seconds, end_mapbox))

# Create Maps Function takes 'folium', 'mapbox', or 'both' as parameters.
create_maps(map_type='both')