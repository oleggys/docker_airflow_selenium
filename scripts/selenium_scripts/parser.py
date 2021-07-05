import json
import time
from scripts.selenium_scripts.commons.tools import get_soup_body, get_max_page_number, get_items, parse_flat_data, \
    can_find_results
from airflow.providers.mongo.hooks.mongo import MongoHook



def parse_flats_from_all_pages(selhook, url, collection_name):
    driver = selhook.driver
    running = True
    page = 1
    hook = MongoHook('parser')
    while running:
        correct_url = url.format(page)
        print(f'Start parsing page {page}. \n{correct_url}')
        soup = get_soup_body(driver, correct_url)
        if can_find_results(soup):
            max_page_number = get_max_page_number(soup)
            flat_elements = get_items(soup)
            print('    flat_elements__len = {0} max_page_number = {1}'.format(len(flat_elements), max_page_number))
            for flat_element in flat_elements:
                flat_object = parse_flat_data(flat_element)
                flat_object_json = json.loads(flat_object)
                print(flat_object_json)
                print('    saving flat with id = {0}'.format(flat_object_json['id']))
                if not hook.find(collection_name, {'id': flat_object_json['id']}, find_one=True, mongo_db='parser'):
                    hook.insert_one(collection_name, flat_object_json, mongo_db='parser')
            if max_page_number == page or page >= 10000:
                running = False
            page += 1
            time.sleep(10)
            if page % 8 == 0 :
                selhook.update_driver()
        else:
            break
