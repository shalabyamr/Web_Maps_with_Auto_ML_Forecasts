import datetime
import gc
import altair as alt
import folium
import pandas as pd
import plotly.express as px
from folium.plugins import MarkerCluster as f_MarkerCluster, HeatMap as f_HeatMap, HeatMapWithTime as f_HeatMapWithTime
from ipyleaflet import Map as i_Map, Marker as i_Marker, LayersControl as i_LayerControl, MeasureControl as i_MeasureControl
from ipyleaflet import SearchControl as i_SearchControl, MarkerCluster as i_MarkerCluster, basemaps as i_basemaps
import warnings
warnings.filterwarnings("ignore")


# Creates the three map types (Mapbox, Turf, and Folium) using
# the previously-created dataframes object.
# Also needs the configuration and dataframes objects.
def create_maps(dfs_obj, configs_obj):
    # For tracking function performance and later stored in public.data_model_performance_tbl
    maps_start = datetime.datetime.now()
    for map_type in configs_obj.run_conditions['map_types']:
        # Folium-Specific Code
        if map_type.upper() == 'FOLIUM':
            # Load map centred
            folium_start = datetime.datetime.now()
            toronto_map = folium.Map(location=[dfs_obj.geopandas_dfs['fact_air_data_proj']['latitude'].mean()
                , dfs_obj.geopandas_dfs['fact_air_data_proj']['longitude'].mean()], zoom_start=10, control_scale=True)

            # Setting up the Feature Groups for Layers Control.
            air_quality_group = folium.FeatureGroup(name='Air Quality Measures').add_to(toronto_map)
            air_quality_heatmap_group = folium.FeatureGroup(name='Air Quality HeatMap').add_to(toronto_map)
            air_quality_chart_group = folium.FeatureGroup(name='Air Quality Charts').add_to(toronto_map)
            traffic_volume_group = folium.FeatureGroup(name='Traffic Volume').add_to(toronto_map)
            traffic_heatmap_group = folium.FeatureGroup(name='Traffic HeatMap').add_to(toronto_map)
            animated_traffic_group = folium.FeatureGroup(name='Traffic Animation').add_to(toronto_map)
            if configs_obj.run_conditions['run_auto_ml']:
                predicted_traffic_group = folium.FeatureGroup(name='Predicted Traffic').add_to(toronto_map)
                predicted_traffic_hm_group = folium.FeatureGroup(name='Predicted Traffic HeatMap').add_to(toronto_map)
            pedestrians_group = folium.FeatureGroup(name='Pedestrians').add_to(toronto_map)
            pedestrians_heatmap_group = folium.FeatureGroup(name='Pedestrians HeatMap').add_to(toronto_map)
            if configs_obj.run_conditions['run_auto_ml']:
                predicted_pedestrians_group = folium.FeatureGroup(name='Predicted Pedestrians').add_to(toronto_map)
                predicted_pedestrians_hm_group = folium.FeatureGroup(name='Predicted Pedestrians HeatMap').add_to(
                    toronto_map)
            # End of Feature Groups ##
            folium.plugins.LocateControl(auto_start=False).add_to(toronto_map)
            folium.LayerControl().add_to(toronto_map)
            # Start of Populating the Map ##

            mc = f_MarkerCluster()
            for index, row in dfs_obj.geopandas_dfs['fact_air_data_proj'].iterrows():
                color = 'black'
                if row['air_quality_value'] > int(
                        dfs_obj.geopandas_dfs['fact_air_data_proj']['air_quality_value'].mean()):
                    color = 'red'
                if row['air_quality_value'] < int(
                        dfs_obj.geopandas_dfs['fact_air_data_proj']['air_quality_value'].mean()):
                    color = 'green'
                folium.Marker(
                    location=[row['latitude'], row['longitude']]
                    , popup=folium.Popup(
                        f"<font color={color}>Air Quality Measure: <b>{row['air_quality_value']}</b><br>Name: <b>{row['geographical_name']}</b><br>Date :<b>{str(row['the_date']).split(' ')[0]}</b><br>Phase :<b>{row['phase_hour_utc']}</font>",
                        min_width=200, max_width=200)
                    , icon=folium.Icon(color=color, icon="info-sign")).add_to(mc)
            mc.add_to(air_quality_group)

            # Adding Graph for Air Quality Measures.
            for index, row in dfs_obj.pandas_dfs['fact_weekdays_avg'].iterrows():
                df = dfs_obj.pandas_dfs['fact_weekdays_avg'][
                    dfs_obj.pandas_dfs['fact_weekdays_avg']['cgndb_id'] == row['cgndb_id']]
                weekday_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                weekday_avgs = []
                for index, row in df.iterrows():
                    weekday_avgs.append(row['monday_avg'])
                    weekday_avgs.append(row['tuesday_avg'])
                    weekday_avgs.append(row['wednesday_avg'])
                    weekday_avgs.append(row['thursday_avg'])
                    weekday_avgs.append(row['friday_avg'])
                    weekday_avgs.append(row['saturday_av'])
                    weekday_avgs.append(row['sunday_avg'])

                df1 = pd.DataFrame({'weekday': weekday_names, 'weekday_avg': weekday_avgs}, index=weekday_names)
                df1['weekday'] = pd.Categorical(df1['weekday'], categories=weekday_names, ordered=True)
                del weekday_names, weekday_avgs
                chart = alt.Chart(df1).mark_bar().encode(x='weekday', y='weekday_avg')
                vis1 = chart.to_json()
                # create a marker, with altair graphic as popup
                circ_mkr = folium.CircleMarker(
                    location=[row['latitude'], row['longitude']],
                    radius=15,
                    color='grey',
                    fill=True,
                    fill_color='grey',
                    fillOpacity=1.0,
                    opacity=1.0,
                    tooltip=f"Air Station ID:<b>{df['cgndb_id'].unique()[0]}.</b>",
                    popup=folium.Popup(max_width=200).add_child(folium.VegaLite(vis1, width=400, height=300)),
                )
                # add to map
                circ_mkr.add_to(air_quality_chart_group)
                # end here

            mc = f_MarkerCluster()
            for index, row in dfs_obj.pandas_dfs['fact_gta_traffic_arcgis'].iterrows():
                color = 'black'
                if row['f8hr_vehicle_volume'] > int(
                        dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                    color = 'red'
                if row['f8hr_vehicle_volume'] < int(
                        dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                    color = 'green'
                folium.Marker(
                    location=[row['latitude'], row['longitude']]
                    , popup=folium.Popup(
                        f"<font color={color}>Traffic Volume:<b>{int(round(row['f8hr_vehicle_volume'], 0))}</b><br>Date:<b>{str(row['count_date']).split(' ')[0]}</b><br>Main Stn:<b>{row['main']}</b></font>"
                        , min_width=200, max_width=200)
                    , icon=folium.Icon(color=color, icon="car")).add_to(mc)
            mc.add_to(traffic_volume_group)

            mc = f_MarkerCluster()
            for index, row in dfs_obj.pandas_dfs['fact_gta_traffic_arcgis'].iterrows():
                color = 'black'
                if row['f8hr_vehicle_volume'] >= int(
                        dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                    color = 'red'
                if row['f8hr_vehicle_volume'] < int(
                        dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                    color = 'green'
                folium.Marker(
                    location=[row['latitude'], row['longitude']]
                    , popup=folium.Popup(
                        f"<font color={color}>Pedestrian Volume:<b><br>{int(round(row['f8hr_pedestrian_volume'], 0))}</b><br>Date:<br><b>{str(row['count_date']).split(' ')[0]}</b><br>Main Stn: <b>{row['main']}</b></font>"
                        , min_width=200, max_width=200)
                    , icon=folium.Icon(color=color, icon="flag")).add_to(mc)
            mc.add_to(pedestrians_group)

            f_HeatMap(dfs_obj.pandas_dfs['fact_combined_air_data'][['latitude', 'longitude', 'air_quality_value']],
                      min_opacity=0.4, overlay=True, blur=18).add_to(air_quality_heatmap_group)

            f_HeatMap(data=zip(dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['latitude']
                               , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['longitude']
                               , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_pedestrian_volume'])
                      , min_opacity=0.4, overlay=True, blur=18).add_to(pedestrians_heatmap_group)

            f_HeatMap(data=zip(dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['latitude'],
                               dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['longitude']
                               , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'])
                      , min_opacity=0.4, overlay=True, blur=18).add_to(traffic_heatmap_group)

            # Insert another Feature Group from H2O AutoML Predictions.
            if configs_obj.run_conditions['run_auto_ml']:
                # Part 1: Add Predicted Traffic
                f_HeatMap(data=zip(dfs_obj.forecasts_dict['traffic_forecast']['latitude'],
                                   dfs_obj.forecasts_dict['traffic_forecast']['longitude']
                                   , dfs_obj.forecasts_dict['traffic_forecast']['predicted_traffic']),
                          min_opacity=0.4, overlay=True, blur=18).add_to(predicted_traffic_hm_group)
                mc = f_MarkerCluster()
                for index, row in dfs_obj.forecasts_dict['traffic_forecast'].iterrows():
                    color = 'black'
                    if row['predicted_traffic'] > int(
                            dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                        color = 'red'
                    if row['predicted_traffic'] < int(
                            dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_vehicle_volume'].mean()):
                        color = 'green'
                    folium.Marker(location=[row['latitude'], row['longitude']],
                                  popup=folium.Popup(
                                      f"<font color={color}>Predicted Traffic: <b>{row['predicted_traffic']}</b><br>Future Date: <b><br>{row['future_date']}</b><br>Name:<br><b>{row['main']}</b></font>"
                                      , min_width=200, max_width=200)
                                  , icon=folium.Icon(color=color, icon="flag")).add_to(mc)
                mc.add_to(predicted_traffic_group)

                # Part 2: Add Predicted Pedestrians.
                f_HeatMap(data=zip(dfs_obj.forecasts_dict['pedestrians_forecast']['latitude'],
                                   dfs_obj.forecasts_dict['pedestrians_forecast']['longitude']
                                   , dfs_obj.forecasts_dict['pedestrians_forecast']['predicted_pedestrians']),
                          min_opacity=0.4, overlay=True, blur=18).add_to(predicted_pedestrians_hm_group)
                mc = f_MarkerCluster()
                for index, row in dfs_obj.forecasts_dict['pedestrians_forecast'].iterrows():
                    color = 'black'
                    if row['predicted_pedestrians'] > int(
                            dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_pedestrian_volume'].mean()):
                        color = 'red'
                    if row['predicted_pedestrians'] < int(
                            dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['f8hr_pedestrian_volume'].mean()):
                        color = 'green'
                    folium.Marker(location=[row['latitude'], row['longitude']],
                                  popup=folium.Popup(
                                      f"<font color={color}>Predicted Pedestrians: <b>{row['predicted_pedestrians']}</b><br>Future Date: <b>{row['future_date']}</b><br>Location Name:<br><b>{row['main']}</b></font>"
                                      , min_width=200, max_width=200)
                                  , icon=folium.Icon(color=color, icon="flag")).add_to(mc)
                mc.add_to(predicted_pedestrians_group)
                # End of Part 2.
                # end of AutoML Insertion
            f_HeatMapWithTime(dfs_obj.lists['traffic'], index=dfs_obj.lists['traffic'],
                              min_speed=1, position="topleft", auto_play=False, overlay=True
                              , display_index=False, show=True, control=True, name='Traffic Animation').add_to(
                animated_traffic_group).add_to(toronto_map)
            # End of Populating the Map ##
            toronto_map.save(configs_obj.run_conditions['parent_dir'] + '/Maps/Folium_Toronto.html')
            folium_end = datetime.datetime.now()
            folium_duration = (folium_end - folium_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
                SET duration_seconds = {folium_duration} , files_processed = {9}
                , start_time = '{folium_start}', end_time = '{folium_end}'
                WHERE step_name = 'folium';"""
            cur = configs_obj.database['pg_engine'].cursor()
            cur.execute(performance_query)
            configs_obj.database['pg_engine'].commit()
            toronto_map.save(configs_obj.run_conditions['parent_dir'] + '/Maps/Folium_Toronto.html')
            if configs_obj.run_conditions['save_locally']:
                data_model_performance_df = pd.read_sql_table('data_model_performance_tbl',
                                                              con=configs_obj.database['sqlalchemy_engine'],
                                                              schema='public')
                data_model_performance_df.to_csv(
                    configs_obj.run_conditions['parent_dir'] + '/Data/data_model_performance_tbl.csv', index=False,
                    index_label=False, mode='w')
                del data_model_performance_df
            print(f"Done Generating the Folium Map in {folium_duration} Seconds")
            del folium_end, folium_duration, folium_start, performance_query
            gc.collect()

        # Mapbox Specific Code
        if map_type.upper() == 'MAPBOX':
            mapbox_start = datetime.datetime.now()
            px.set_mapbox_access_token(configs_obj.access_tokens['mapbox'])
            fig_air_quality_values = px.scatter_mapbox(dfs_obj.geopandas_dfs['fact_air_data_proj']
                                                       , lat=dfs_obj.geopandas_dfs['fact_air_data_proj'].geom.y
                                                       , lon=dfs_obj.geopandas_dfs['fact_air_data_proj'].geom.x
                                                       , hover_name="air_quality_value"
                                                       , height=500, zoom=10)
            fig_air_quality_values.update_layout(mapbox_style="open-street-map")
            fig_air_quality_values.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
            fig_air_quality_values.write_html(
                configs_obj.run_conditions['parent_dir'] + '/Maps/Mapbox_Air_Quality.html')
            fig_vehicle_heatmap = px.density_mapbox(dfs_obj.geopandas_dfs['fact_gta_traffic_proj']
                                                    , lat=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].geom.y
                                                    , lon=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].geom.x
                                                    , z='f8hr_vehicle_volume'
                                                    , mapbox_style="open-street-map")
            fig_vehicle_heatmap.write_html(
                configs_obj.run_conditions['parent_dir'] + '/Maps/Mapbox_Vehicle_HeatMap.html')
            fig_pedestrian_heatmap = px.density_mapbox(dfs_obj.geopandas_dfs['fact_gta_traffic_proj']
                                                       , lat=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].geom.y
                                                       , lon=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].geom.x
                                                       , z='f8hr_pedestrian_volume'
                                                       , mapbox_style="open-street-map")
            fig_pedestrian_heatmap.write_html(
                configs_obj.run_conditions['parent_dir'] + '/Maps/Mapbox_Pedestrian_HeatMap.html')

            fig_traffic_volume = px.scatter_mapbox(dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].dropna()
                                                   , lat=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].dropna().geom.y
                                                   , lon=dfs_obj.geopandas_dfs['fact_gta_traffic_proj'].dropna().geom.x
                                                   , hover_name='f8hr_vehicle_volume'
                                                   , height=500, zoom=10)
            fig_traffic_volume.write_html(configs_obj.run_conditions['parent_dir'] + '/Maps/Mapbox_Traffic_Volume.html')
            mapbox_end = datetime.datetime.now()
            mapbox_duration = (mapbox_end - mapbox_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
                SET duration_seconds = {mapbox_duration} , files_processed = {9}
                , start_time = '{mapbox_start}', end_time = '{mapbox_end}'
                WHERE step_name = 'mapbox';"""
            cur = configs_obj.database['pg_engine'].cursor()
            cur.execute(performance_query)
            configs_obj.database['pg_engine'].commit()
            if configs_obj.run_conditions['save_locally']:
                data_model_performance_df = pd.read_sql_table('data_model_performance_tbl',
                                                              con=configs_obj.database['sqlalchemy_engine'],
                                                              schema='public')
                data_model_performance_df.to_csv(
                    configs_obj.run_conditions['parent_dir'] + '/Data/data_model_performance_tbl.csv', index=False,
                    index_label=False, mode='w')
                del data_model_performance_df
            print(f"Done Generating the Mapbox Map in {mapbox_duration} Seconds")
            del mapbox_end, mapbox_duration, mapbox_start, performance_query
            gc.collect()

        # Turf Specific Code
        if map_type.upper() == 'TURF':
            turf_start = datetime.datetime.now()
            points = []
            for index, row in dfs_obj.pandas_dfs['fact_gta_traffic_arcgis'].iterrows():
                point = [row['longitude'], row['latitude']]
                points.append(point)

            bbox = [dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['longitude'].min()
                , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['latitude'].min()
                , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['longitude'].max()
                , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['latitude'].max()]
            m = i_Map(scroll_wheel_zoom=True
                      , center=[dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['latitude'].mean()
                      , dfs_obj.pandas_dfs['fact_gta_traffic_arcgis']['longitude'].mean()]
                      , zoom=14
                      , touch_zoom=True
                      )
            for point in points:
                marker = i_Marker(location=[point[1], point[0]])
                m.add(marker)
            m.save(outfile=configs_obj.run_conditions['parent_dir'] + '/Maps/Turf_gta_traffic.html')
            turf_end = datetime.datetime.now()
            turf_total_seconds = (turf_end - turf_start).total_seconds()
            print(f"Done Generating Turf Map in {turf_total_seconds} Seconds")
            turf_end = datetime.datetime.now()
            turf_duration = (turf_end - turf_start).total_seconds()
            performance_query = f"""UPDATE public.data_model_performance_tbl
            SET duration_seconds = {turf_duration} , files_processed = {9}
            , start_time = '{turf_start}', end_time = '{turf_end}'
            WHERE step_name = 'turf';"""
            cur = configs_obj.database['pg_engine'].cursor()
            cur.execute(performance_query)
            configs_obj.database['pg_engine'].commit()
            if configs_obj.run_conditions['save_locally']:
                data_model_performance_df = pd.read_sql_table('data_model_performance_tbl',
                                                              con=configs_obj.database['sqlalchemy_engine'],
                                                              schema='public')
                data_model_performance_df.to_csv(
                    configs_obj.run_conditions['parent_dir'] + '/Data/data_model_performance_tbl.csv', index=False,
                    index_label=False, mode='w')
                del data_model_performance_df
            del turf_start, turf_end, turf_duration, performance_query
            gc.collect()

    maps_end = datetime.datetime.now()
    maps_duration = (maps_end - maps_start).total_seconds()
    print(
        f"****************************\nDone Generating All Maps in {maps_duration} Seconds\n****************************")
    del maps_start, maps_end
    gc.collect()
