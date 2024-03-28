import datetime
from selenium import webdriver
import glob
import pandas as pd
import gc
import os
import sys
import warnings
warnings.filterwarnings("ignore")

def launch_browser(driver, url):
    driver.get(url)
    navigation_start = driver.execute_script(
        "return window.performance.timing.navigationStart")
    dom_complete = driver.execute_script(
        "return window.performance.timing.domComplete")
    total_time = dom_complete - navigation_start
    return total_time


def test_maps(configs_obj):
    test_maps_start = datetime.datetime.now()
    print('Started Testing Maps.....')
    maps_paths = glob.glob(f"{configs_obj.run_conditions['parent_dir']}/Maps/*.html")
    maps_performance = pd.DataFrame(columns=['map', 'map_type', 'test_start'
        , 'test_end', 'chrome_load_time'
        , 'firefox_load_time', 'safari_load_time'])
    data = []
    for map in maps_paths:
        chrome_driver = webdriver.Chrome()
        firefox_driver = webdriver.Firefox()
        safari_driver = webdriver.Safari()
        if 'FOLIUM' in map.upper():
            test_start = datetime.datetime.now()
            try:
                data.append({'map': map.split('/')[-1], 'map_type': 'folium', 'test_start': test_start
                                , 'chrome_load_time': launch_browser(driver=chrome_driver, url=f"file:///{map}")
                                , 'firefox_load_time': launch_browser(driver=firefox_driver, url=f"file:///{map}")
                                , 'safari_load_time': launch_browser(driver=safari_driver, url=f"file:///{map}")
                                , 'test_end': datetime.datetime.now()})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()
        elif 'TURF' in map.upper():
            test_start = datetime.datetime.now()
            try:
                data.append({'map': map.split('/')[-1], 'map_type': 'turf', 'test_start': test_start
                                , 'chrome_load_time': launch_browser(driver=chrome_driver, url=f"file:///{map}")
                                , 'firefox_load_time': launch_browser(driver=firefox_driver, url=f"file:///{map}")
                                , 'safari_load_time': launch_browser(driver=safari_driver, url=f"file:///{map}")
                                , 'test_end': datetime.datetime.now()})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()
        elif 'MAPBOX' in map.upper():
            test_start = datetime.datetime.now()
            try:
                data.append({'map': map.split('/')[-1], 'map_type': 'mapbox', 'test_start': test_start
                                , 'chrome_load_time': launch_browser(driver=chrome_driver, url=f"file:///{map}")
                                , 'firefox_load_time': launch_browser(driver=firefox_driver, url=f"file:///{map}")
                                , 'safari_load_time': launch_browser(driver=safari_driver, url=f"file:///{map}")
                                , 'test_end': datetime.datetime.now()})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()
        else:
            test_start = datetime.datetime.now()
            try:
                data.append({'map': map.split('/')[-1], 'map_type': 'unknown', 'test_start': test_start
                                , 'chrome_load_time': launch_browser(driver=chrome_driver, url=f"file:///{map}")
                                , 'firefox_load_time': launch_browser(driver=firefox_driver, url=f"file:///{map}")
                                , 'safari_load_time': launch_browser(driver=safari_driver, url=f"file:///{map}")
                                , 'test_end': datetime.datetime.now()})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()

    maps_performance = pd.concat([maps_performance, pd.DataFrame(data)])
    maps_performance.to_sql('data_maps_performance_tbl', con=configs_obj.database['sqlalchemy_engine'],
                            if_exists='replace', index=False, schema='public')
    if configs_obj.run_conditions['save_locally']:
        data_model_performance_df = pd.read_sql_table('data_maps_performance_tbl', con=configs_obj.database['sqlalchemy_engine'], schema='public')
        data_model_performance_df.to_csv(configs_obj.run_conditions['parent_dir']+'/Data/data_maps_performance_tbl.csv', index=False, index_label=False, mode='w')
        del data_model_performance_df
    maps_tester_end = datetime.datetime.now()
    performance_query = f"""UPDATE public.data_model_performance_tbl
        SET duration_seconds = {(maps_tester_end - test_maps_start).total_seconds()} , files_processed = {len(maps_paths)}
        , start_time = '{test_maps_start}', end_time = '{maps_tester_end}'
        WHERE step_name = 'test_maps';"""
    cur = configs_obj.database['pg_engine'].cursor()
    cur.execute(performance_query)
    configs_obj.database['pg_engine'].commit()
    if configs_obj.run_conditions['save_locally']:
        data_model_performance_df = pd.read_sql_table('data_model_performance_tbl', con=configs_obj.database['sqlalchemy_engine'], schema='public')
        data_model_performance_df.to_csv(configs_obj.run_conditions['parent_dir']+'/Data/data_model_performance_tbl.csv', index=False, index_label=False, mode='w')
        del data_model_performance_df
    if configs_obj.run_conditions['show_maps']:
        print('Launching Maps in their Respective Optimal Browser...')
        for index, row in maps_performance.iterrows():
            if row['chrome_load_time'] == \
                    min([row['chrome_load_time'], row['firefox_load_time'], row['safari_load_time']]):
                try:
                    print(f"Loading Chrome with Map: {configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                    os.system(
                    f"open '/Applications/Google Chrome.app' file:///{configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                except Exception as e:
                    print('Error Launching Google Chrome.app:', e)
                    print('You need to install Google Chrome in Applications Folders.')
                    sys.exit(1)
            elif row['firefox_load_time'] == \
                    min([row['chrome_load_time'], row['firefox_load_time'], row['safari_load_time']]):
                try:
                    print(f"Loading Firefox with Map: {configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                    os.system(
                    f"open '/Applications/Firefox.app' file:///{configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                except Exception as e:
                    print('Error Loading Firefox.app:', e)
                    print('You need to install Firefox Browser in Applications Folders.')
                    sys.exit(1)
            elif row['safari_load_time'] == \
                    min([row['chrome_load_time'], row['firefox_load_time'], row['safari_load_time']]):
                try:
                    print(f"Loading Safari with Map: {configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                    os.system(
                    f"open '/Applications/Safari.app' file:///{configs_obj.run_conditions['parent_dir']}/Maps/{row['map']}")
                except Exception as e:
                    print('Error Loading Safari.app:', e)
                    print('You need to enable Safari Automation in Developer Tools. For Instructions Please Visit: \nhttps://developer.apple.com/documentation/webkit/testing_with_webdriver_in_safari')
                    sys.exit(1)
    print(
        f"****************************\nDone Testing Maps.  Maps Tester took {(maps_tester_end - test_maps_start).total_seconds()} Seconds.\n****************************")
    gc.collect()
    return maps_performance
