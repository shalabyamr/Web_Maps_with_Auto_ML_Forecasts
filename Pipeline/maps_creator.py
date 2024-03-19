import folium
from folium.plugins import MarkerCluster as f_MarkerCluster, HeatMap as f_HeatMap, HeatMapWithTime as f_HeatMapWithTime
import plotly.express as px
import datetime
from turfpy.measurement import envelope
from ipyleaflet import Map as i_Map, GeoJSON as i_GeoJSON, LayersControl as i_LayersControl, Marker as i_Marker, WidgetControl as i_WidgetControl, MarkerCluster as i_MarkerCluster
from shapely import Polygon as s_Polygon, MultiPolygon as s_MultiPolygon
import gc
import pandas as pd

# Creates the three map types (Mapbox, Turf, and Folium) using
# the previously-created dataframes object.
# Also needs the configuration and dataframes objects.
def create_maps(dfs_obj, configs_obj, show: bool, add_auto_ml: bool, map_types: []):
    # For tracking function performance and later stored in public.data_model_performance_tbl
    maps_start = datetime.datetime.now()
    for map_type in map_types:
        # Folium-Specific Code
        if map_type.upper() == 'FOLIUM':
            # Load map centred
            folium_start = datetime.datetime.now()
            toronto_map = folium.Map(location=[dfs_obj.geopandas_dict['df_fact_air_data_proj']['latitude'].mean()
                , dfs_obj.geopandas_dict['df_fact_air_data_proj']['longitude'].mean()], zoom_start=10, control_scale=True)

            # Setting up the Feature Groups for Layers Control.
            air_quality_group = folium.FeatureGroup(name='Air Quality Measures').add_to(toronto_map)
            air_quality_heatmap_group = folium.FeatureGroup(name='Air Quality HeatMap').add_to(toronto_map)
            traffic_volume_group = folium.FeatureGroup(name='Traffic Volume').add_to(toronto_map)
            traffic_heatmap_group = folium.FeatureGroup(name='Traffic HeatMap').add_to(toronto_map)
            animated_traffic_group = folium.FeatureGroup(name='Animated Traffic').add_to(toronto_map)
            pedestrians_group = folium.FeatureGroup(name='Pedestrians').add_to(toronto_map)
            pedestrians_heatmap_group = folium.FeatureGroup(name='Pedestrians HeatMap').add_to(toronto_map)
            predicted_traffic_group = folium.FeatureGroup(name='Predicted Traffic').add_to(toronto_map)
            predicted_traffic_hm_group = folium.FeatureGroup(name='Predicted Traffic HeatMap').add_to(toronto_map)
            # End of Feature Groups ##
            folium.plugins.LocateControl(auto_start=False).add_to(toronto_map)
            folium.LayerControl().add_to(toronto_map)
            # Start of Populating the Map ##

            mc = f_MarkerCluster()
            for index, row in dfs_obj.geopandas_dict['df_fact_air_data_proj'].dropna().iterrows():
                    folium.Marker(
                      location=[row['latitude'], row['longitude']]
                    , popup=f"Air Quality Measure: <b>{row['air_quality_value']}</b><br>Geographical Name:<b>{row['geographical_name']}</b>.<br>CGN_ID: <b>{row['cgndb_id']}</b>"
                    , icon=folium.Icon(color="black", icon="info-sign")).add_to(mc)
            mc.add_to(air_quality_group)

            mc = f_MarkerCluster()
            for index, row in dfs_obj.pandas_dict['df_fact_traffic_volume'].iterrows():
                folium.Marker(
                      location=[row['lat'], row['lng']]
                    , popup=f'Traffic Volume: <b>{row['px']}</b><br>Location:<b>{row['location']}</b>.<br>Location ID: <b>{row['location_id']}</b>'
                    , icon=folium.Icon(color="red", icon="car")).add_to(mc)
            mc.add_to(traffic_volume_group)

            mc = f_MarkerCluster()
            for index, row in dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].iterrows():
                folium.Marker(
                    location=[row['latitude'], row['longitude']],
                    popup='Vehicle Volume: <b>{}</b><br>Pedestrian Volume:<b>{}</b>.<br>Main Stn: <b>{}</b>'.format(
                        row['f8hr_vehicle_volume'], row['f8hr_pedestrian_volume'], row['main']),
                    icon=folium.Icon(color="green", icon="flag")).add_to(mc)
            mc.add_to(pedestrians_group)

            f_HeatMap(dfs_obj.pandas_dict['df_fact_combined_air_data'][['latitude', 'longitude', 'air_quality_value']],
                    min_opacity=0.4, overlay=True, blur=18).add_to(air_quality_heatmap_group)

            f_HeatMap(data=zip(dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].dropna()['latitude']
                               , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].dropna()['longitude']
                             , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].dropna()['f8hr_pedestrian_volume'])
                      , min_opacity=0.4, overlay=True, blur=18).add_to(pedestrians_heatmap_group)

            f_HeatMap(data=zip(dfs_obj.pandas_dict['df_fact_traffic_volume'].dropna()['lat'], dfs_obj.pandas_dict['df_fact_traffic_volume'].dropna()['lng']
                             , dfs_obj.pandas_dict['df_fact_traffic_volume'].dropna()['px'])
                      , min_opacity=0.4, overlay=True, blur=18).add_to(traffic_heatmap_group)

            # Insert another Feature Group from H2O AutoML Predictions.
            if add_auto_ml:
                dfs_obj.forecasts_dict['df_traffic_forecasts'] = pd.read_sql_table(table_name='fact_h2o_traffic_forecasts', schema='public', con=configs_obj.sqlalchemy_engine, parse_dates=True)
                f_HeatMap(data=zip(dfs_obj.forecasts_dict['df_traffic_forecasts']['lat'], dfs_obj.forecasts_dict['df_traffic_forecasts']['lng']
                                 , dfs_obj.forecasts_dict['df_traffic_forecasts']['predicted_traffic']),
                        min_opacity=0.4, overlay=True, blur=18).add_to(predicted_traffic_hm_group)
                mc = f_MarkerCluster()
                for index, row in dfs_obj.forecasts_dict['df_traffic_forecasts'].iterrows():
                    folium.Marker(location=[row['lat'], row['lng']],
                          popup=f'Predicted Traffic: <b>{row['predicted_traffic']}</b><br>Future Date: <b>{row['future_date']}</b><br>Location ID:<br><b>{row['location_id']}</b>'
                        , icon=folium.Icon(color="green", icon="flag")).add_to(mc)
                mc.add_to(predicted_traffic_group)
                # end of AutoML Insertion
            f_HeatMapWithTime(dfs_obj.lists['traffic'], index=dfs_obj.lists['traffic'],
                              min_speed=5, position="topleft", auto_play=True, overlay=True
                              , display_index=True, show=True, control=True, name='Animated Traffic').add_to(animated_traffic_group).add_to(toronto_map)
            # End of Populating the Map ##
            toronto_map.save(configs_obj.parent_dir+'/Maps/Folium_Toronto.html')
            folium_end = datetime.datetime.now()
            folium_duration = (folium_end - folium_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
                SET duration_seconds = {folium_duration} , files_processed = {9}
                , start_time = '{folium_start}', end_time = '{folium_end}'
                WHERE step_name = 'folium';"""
            cur = configs_obj.pg_engine.cursor()
            cur.execute(performance_query)
            configs_obj.pg_engine.commit()
            print(f'Done Generating the Folium Map in {folium_duration} Seconds')
            del folium_end, folium_duration, folium_start, performance_query
            toronto_map.save(configs_obj.parent_dir + '/Maps/Folium_Toronto.html')
            gc.collect()

        # Mapbox Specific Code
        if map_type.upper() == 'MAPBOX':
            mapbox_start = datetime.datetime.now()
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

            fig_vehicle_heatmap = px.density_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj']
                                        , lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.y
                                        , lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.x
                                        , z='f8hr_vehicle_volume'
                                        , mapbox_style="open-street-map")
            if show:
                fig_vehicle_heatmap.show()

            fig_vehicle_heatmap.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Vehicle_HeatMap.html')

            fig_pedestrian_heatmap = px.density_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj']
                                        , lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.y
                                        , lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].geom.x
                                        , z='f8hr_pedestrian_volume'
                                        , mapbox_style="open-street-map")
            if show:
                fig_pedestrian_heatmap.show()

            fig_pedestrian_heatmap.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Pedestrian_HeatMap.html')

            fig_traffic_volume = px.scatter_mapbox(dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna()
                                            , lat=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna().geom.y
                                            , lon=dfs_obj.geopandas_dict['df_fact_gta_traffic_proj'].dropna().geom.x
                                            , hover_name='f8hr_vehicle_volume'
                                            , height=500, zoom=10)
            if show:
                fig_traffic_volume.show()

            fig_traffic_volume.write_html(configs_obj.parent_dir + '/Maps/Mapbox_Traffic_Volume.html')

            mapbox_end = datetime.datetime.now()
            mapbox_duration = (mapbox_end - mapbox_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
                SET duration_seconds = {mapbox_duration} , files_processed = {9}
                , start_time = '{mapbox_start}', end_time = '{mapbox_end}'
                WHERE step_name = 'mapbox';"""
            cur = configs_obj.pg_engine.cursor()
            cur.execute(performance_query)
            configs_obj.pg_engine.commit()
            print(f'Done Generating the Mapbox Map in {mapbox_duration} Seconds')
            del mapbox_end, mapbox_duration, mapbox_start, performance_query
            gc.collect()

        # Turf Specific Code
        if map_type.upper() == 'TURF':
            turf_start = datetime.datetime.now()
            points = []
            for index, row in dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis'].iterrows():
                point = [row['longitude'], row['latitude']]
                points.append(point)

            bbox = [dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['longitude'].min()
                    , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['latitude'].min()
                    , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['longitude'].max()
                    , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['latitude'].max()]
            #points_and_bbox = voronoi(points=points, bbox=bbox)
            m = i_Map(scroll_wheel_zoom=True
                    , center=[dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['latitude'].mean()
                    , dfs_obj.pandas_dict['df_fact_gta_traffic_arcgis']['longitude'].mean()]
                    , zoom=14
                    , touch_zoom=True)
            for point in points:
                marker = i_Marker(location=[point[1], point[0]])
                m.add_layer(marker)
            m.save(outfile=configs_obj.parent_dir+'/Maps/Turf_gta_traffic.html')
            turf_end = datetime.datetime.now()
            turf_total_seconds = (turf_end - turf_start).total_seconds()
            print(f'Done Generating Turf Map in {turf_total_seconds} Seconds')
            turf_end = datetime.datetime.now()
            turf_duration = (turf_end - turf_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
            SET duration_seconds = {turf_duration} , files_processed = {9}
            , start_time = '{turf_start}', end_time = '{turf_end}'
            WHERE step_name = 'turf';"""
            cur = configs_obj.pg_engine.cursor()
            cur.execute(performance_query)
            configs_obj.pg_engine.commit()
            del turf_start, turf_end, turf_duration, performance_query
            gc.collect()

    maps_end = datetime.datetime.now()
    maps_duration = (maps_end - maps_start).total_seconds()
    print(f'****************************\nDone Generating All Maps in {maps_duration} Seconds\n****************************')
    del maps_start, maps_end
    gc.collect()
