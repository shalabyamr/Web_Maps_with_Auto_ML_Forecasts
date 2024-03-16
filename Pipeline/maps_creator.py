import folium
from folium.plugins import MarkerCluster, HeatMap, HeatMapWithTime
import plotly.express as px
import datetime
import turfpy as tpy
import gc


def create_maps(dfs_obj, configs_obj, map_type: str, show: bool):
    # Setting Up the Data for both map types
    start = datetime.datetime.now()

    # Folium-Specific Code
    if map_type.upper() in ('FOLIUM', 'ALL'):
        # Load map centred
        toronto_map = folium.Map(location=[dfs_obj.geopandas_dict['df_fact_air_data_proj']['latitude'].mean()
            , dfs_obj.geopandas_dict['df_fact_air_data_proj']['longitude'].mean()], zoom_start=10, control_scale=True)
        marker_cluster = MarkerCluster()
        fg = folium.FeatureGroup(name='Air Quality Measures')
        for index, row in dfs_obj.geopandas_dict['df_fact_air_data_proj'].iterrows():
            marker_cluster.add_child(folium.Marker(
                location=[row['latitude'], row['longitude']]
                ,
                popup=f"Air Quality Measure: <b>{row['air_quality_value']}</b><br>Geographical Name:<b>{row['geographical_name']}</b>.<br>CGN_ID: <b>{row['cgndb_id']}</b>"
                , icon=folium.Icon(color="black", icon="info-sign")))
        fg.add_child(marker_cluster)
        toronto_map.add_child(fg)

        for index, row in dfs_obj.pandas_dict['df_fact_traffic_volume'].iterrows():
            folium.Marker(
                location=[row['lat'], row['lng']],
                popup='Traffic Volume: <b>{}</b><br>Location:<b>{}</b>.<br>Location ID: <b>{}</b>'.format(row['px'],
                                                                                                          row[
                                                                                                              'location'],
                                                                                                          row[
                                                                                                              'location_id']),
                icon=folium.Icon(color="red", icon="car")).add_to(marker_cluster)

        for index, row in dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].iterrows():
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup='Vehicle Volume: <b>{}</b><br>Pedestrian Volume:<b>{}</b>.<br>Main Stn: <b>{}</b>'.format(
                    row['f8hr_vehicle_volume'], row['f8hr_pedestrian_volume'], row['main']),
                icon=folium.Icon(color="green", icon="flag")).add_to(marker_cluster)

        HeatMap(dfs_obj.pandas_dict['df_fact_combined_air_data'][['latitude', 'longitude', 'air_quality_value']],
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Air Quality Heatmap').add_to(toronto_map))

        HeatMap(dfs_obj.pandas_dict['df_fact_combined_air_data'][['latitude', 'longitude', 'air_quality_value']],
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Air Quality Heatmap').add_to(toronto_map))

        HeatMap(data=zip(dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['latitude']
                         , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['longitude'],
                         dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['f8hr_pedestrian_volume']),
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Pedestrian Volume').add_to(toronto_map))

        HeatMap(data=zip(dfs_obj.pandas_dict['temp_df']['lat'], dfs_obj.pandas_dict['temp_df']['lng']
                         , dfs_obj.pandas_dict['temp_df']['px']),
                min_opacity=0.4,
                blur=18).add_to(folium.FeatureGroup(name='Vehicle Volume').add_to(toronto_map))

        HeatMapWithTime(dfs_obj.lists, index=dfs_obj.lists, auto_play=True).add_to(folium.FeatureGroup(
            name='Traffic Time Heatmap')).add_to(toronto_map)
        folium.LayerControl().add_to(toronto_map)
        end_folium = datetime.datetime.now()
        folium_total_seconds = (end_folium - start).total_seconds()
        print('Done Generating the Folium Map in {} seconds as of {}'.format(folium_total_seconds, end_folium))
        del end_folium, folium_total_seconds
        toronto_map.save(configs_obj.parent_dir + '/Maps/Folium_Toronto.html')
        gc.collect()

    # Mapbox Specific Code
    if map_type.upper() in ('MAPBOX', 'ALL'):
        px.set_mapbox_access_token(configs_obj.access_tokens['mapbox'])
        fig_air_quality_values = px.scatter_mapbox(dfs_obj.geopandas_dict['df_fact_air_data_proj']
                                                   , lat=dfs_obj.geopandas_dict['df_fact_air_data_proj'].geom.y
                                                   , lon=dfs_obj.geopandas_dict['df_fact_air_data_proj'].geom.x
                                                   , hover_name="air_quality_value"
                                                   , height=500, zoom=10)
        fig_air_quality_values.update_layout(mapbox_style="open-street-map")
        fig_air_quality_values.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

        if show:
            fig_air_quality_values.show()

        fig_air_quality_values.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Air_Quality.html')

        fig_vehicle_heatmap = px.density_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'],
                                                lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.y,
                                                lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.x,
                                                z='f8hr_vehicle_volume',
                                                mapbox_style="open-street-map")
        if show:
            fig_vehicle_heatmap.show()

        fig_vehicle_heatmap.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Vehicle_HeatMap.html')

        fig_pedestrian_heatmap = px.density_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'],
                                                   lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.y,
                                                   lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.x,
                                                   z='f8hr_pedestrian_volume',
                                                   mapbox_style="open-street-map")
        if show:
            fig_pedestrian_heatmap.show()

        fig_pedestrian_heatmap.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Pedestrian_HeatMap.html')

        fig_traffic_volume = px.scatter_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna(),
                                               lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna().geom.y,
                                               lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna().geom.x,
                                               hover_name='f8hr_vehicle_volume',
                                               height=500, zoom=10)
        if show:
            fig_traffic_volume.show()

        fig_traffic_volume.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Traffic_Volume.html')

        end_mapbox = datetime.datetime.now()
        mapbox_total_seconds = (end_mapbox - start).total_seconds()
        print('Done Generating the Mapbox Map in {} seconds as of {}'.format(mapbox_total_seconds, end_mapbox))
        del end_mapbox, mapbox_total_seconds
        gc.collect()

    # Turf Specific Code
    if map_type.upper() in ('TURF', 'ALL'):
        end_turf_time = datetime.datetime.now()
        turf_total_seconds = (end_turf_time - start).total_seconds()
        print('Done Generating Turf Map in {} seconds as of {}'.format(turf_total_seconds, end_turf_time))
        gc.collect()
