from execute_pipeline import execute_pipeline, sqlalchemy_engine, pg_engine, parent_dir
import folium
import pandas as pd
from folium.plugins import MarkerCluster, HeatMap, HeatMapWithTime

# comment out this line after initial setup of the tables.
execute_pipeline()

query_get_tables = """SELECT table_name FROM information_schema.tables
    WHERE (table_schema = 'public') and (table_name not in('spatial_ref_sys','geography_columns',
    'geometry_columns','data_model_performance_tbl'))"""
cur = pg_engine.cursor()
cur.execute(query_get_tables)
del query_get_tables
public_tables = [item[0] for item in cur.fetchall()]
print('Public tables:', public_tables)

for public_table in public_tables:
    print('Creating Dataframe df_{} from table_name {}'.format(public_table, public_table))
    exec(f"df_%s = pd.read_sql_table(table_name=public_table, con=sqlalchemy_engine, schema='public')" % (public_table), globals())


# Load map centred
toronto_map = folium.Map(location=[df_fact_air_data_proj['latitude'].mean(), df_fact_air_data_proj['longitude'].mean()], zoom_start=10, control_scale=True)
marker_cluster = MarkerCluster().add_to(toronto_map)

for index, row in df_fact_air_data_proj.iterrows():
    folium.Marker(
        location=[row['latitude'], row['longitude']],
        popup='Air Quality Measure: <b>{}</b><br>Geographical Name:<b>{}</b>.<br>CGN_ID: <b>{}</b>'.format(row['air_quality_value'], row['geographical_name'],row['cgndb_id']),
        icon=folium.Icon(color="black", icon="info-sign")).add_to(marker_cluster)


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

temp_df = df_fact_traffic_volume.dropna()
temp_df['latest_count_date'] = pd.to_datetime(temp_df['latest_count_date'])
temp_df.sort_values(by=['latest_count_date'], inplace=True)
temp_df.set_index('latest_count_date', inplace=True)

HeatMap(data=zip(temp_df['lat'], temp_df.lng, temp_df['px']),
        min_opacity=0.4,
        blur=18).add_to(folium.FeatureGroup(name='Vehicle Volume').add_to(toronto_map))


folium.LayerControl().add_to(toronto_map)

# Heatmap with time
data = []
for _, d in temp_df.groupby('latest_count_date'):
    data.append([[row['lat'], row['lng'], row['px']] for _, row in d.iterrows()])

hmt = folium.Map(location=[temp_df['lat'].mean(), temp_df['lng'].mean() ],
               tiles='cartodbpositron',#'cartodbpositron', stamentoner
               zoom_start=15,
               control_scale=True)

HeatMapWithTime(data,
                index=data,
                auto_play=True,
                use_local_extrema=True,
                display_index=True
               ).add_to(hmt)

toronto_map.save(parent_dir+'/Maps/toronto_map.html')
hmt.save(parent_dir+'/Maps/toronto_time_heatmap.html')