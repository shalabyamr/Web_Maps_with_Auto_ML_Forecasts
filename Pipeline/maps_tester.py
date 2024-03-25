import datetime
from selenium import webdriver
from data_extractor import read_configs, initialize_database
import glob
import pandas as pd
import gc

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
    maps = glob.glob(f"{configs_obj.run_conditions['parent_dir']}/Maps/*.html")
    maps_performance = pd.DataFrame(columns=['map', 'map_type', 'test_start'
        , 'test_end', 'chrome_load_time'
        , 'firefox_load_time', 'safari_load_time'])
    data = []
    for map in maps:
        chrome_driver = webdriver.Chrome()
        firefox_driver = webdriver.Firefox()
        safari_driver = webdriver.Safari()
        if 'FOLIUM' in map.upper():
            map_type = 'folium'
            test_start = datetime.datetime.now()
            try:
                chrome_load_time = launch_browser(driver=chrome_driver, url=f"file:///{map}")
                firefox_load_time = launch_browser(driver=firefox_driver, url=f"file:///{map}")
                safari_load_time = launch_browser(driver=safari_driver, url=f"file:///{map}")
                test_end = datetime.datetime.now()
                data.append({'map': map.split('/')[-1], 'map_type': map_type, 'test_start': test_start, 'test_end': test_end
                              , 'chrome_load_time': chrome_load_time , 'firefox_load_time': firefox_load_time, 'safari_load_time':safari_load_time})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()

        elif 'TURF' in map.upper():
            map_type = 'turf'
            test_start = datetime.datetime.now()
            try:
                chrome_load_time = launch_browser(driver=chrome_driver, url=f"file:///{map}")
                firefox_load_time = launch_browser(driver=firefox_driver, url=f"file:///{map}")
                safari_load_time = launch_browser(driver=safari_driver, url=f"file:///{map}")
                test_end = datetime.datetime.now()
                data.append({'map': map.split('/')[-1], 'map_type': map_type, 'test_start': test_start, 'test_end': test_end
                              , 'chrome_load_time': chrome_load_time , 'firefox_load_time': firefox_load_time, 'safari_load_time':safari_load_time})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()
        elif 'MAPBOX' in map.upper():
            map_type = 'mapbox'
            test_start = datetime.datetime.now()
            try:
                chrome_load_time = launch_browser(driver=chrome_driver, url=f"file:///{map}")
                firefox_load_time = launch_browser(driver=firefox_driver, url=f"file:///{map}")
                safari_load_time = launch_browser(driver=safari_driver, url=f"file:///{map}")
                test_end = datetime.datetime.now()
                data.append({'map': map.split('/')[-1], 'map_type': map_type, 'test_start': test_start, 'test_end': test_end
                              , 'chrome_load_time': chrome_load_time , 'firefox_load_time': firefox_load_time, 'safari_load_time':safari_load_time})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()
        else:
            map_type = 'unknown'
            test_start = datetime.datetime.now()
            try:
                chrome_load_time = launch_browser(driver=chrome_driver, url=f"file:///{map}")
                firefox_load_time = launch_browser(driver=firefox_driver, url=f"file:///{map}")
                safari_load_time = launch_browser(driver=safari_driver, url=f"file:///{map}")
                test_end = datetime.datetime.now()
                data.append({'map': map.split('/')[-1], 'map_type': map_type, 'test_start': test_start, 'test_end': test_end
                              , 'chrome_load_time': chrome_load_time , 'firefox_load_time': firefox_load_time, 'safari_load_time':safari_load_time})
            finally:
                chrome_driver.close()
                firefox_driver.close()
                safari_driver.close()

    maps_performance = pd.concat([maps_performance, pd.DataFrame(data)])
    maps_performance.to_sql('data_maps_performance_tbl', con=configs_obj.database['sqlalchemy_engine'],
                            if_exists='replace', index=False, schema='public')
    maps_tester_end = datetime.datetime.now()
    performance_query = f"""UPDATE public.data_model_performance_tbl
        SET duration_seconds = {(maps_tester_end - test_maps_start).total_seconds()} , files_processed = {len(maps)}
        , start_time = '{test_maps_start}', end_time = '{maps_tester_end}'
        WHERE step_name = 'test_maps';"""
    cur = configs_obj.database['pg_engine'].cursor()
    cur.execute(performance_query)
    configs_obj.database['pg_engine'].commit()
    print(f'****************************\nDone Testing Maps.  Maps Tester took {(maps_tester_end - test_maps_start).total_seconds()} Seconds.\n****************************')
    gc.collect()
    return maps_performance
