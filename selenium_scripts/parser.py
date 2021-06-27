import bs4
import os
import time
import json
import time
import requests
from random import choice
import pymongo
from selenium_scripts.commons.tools import get_soup_body, get_max_page_number, get_items, parse_flat_data
from airflow.providers.mongo.hooks.mongo import MongoHook


def parse_flats_from_all_pages(driver, url):
    running = True
    page = 1
    hook = MongoHook('parser')
    # if not hook.find('cian_flats', {'id': '555555555555'}, find_one=True, mongo_db='parser'):
    #     hook.insert_one('cian_flats', {'id': '555555555555', 'test': True}, mongo_db='parser')
    # client = pymongo.MongoClient("mongodb://airflow:philips830P@193.124.206.198:27017/parser")
    while running:
        correct_url = url.format(page)
        print(f'Start parsing page {page}. \n{correct_url}')
        soup = get_soup_body(driver, correct_url)
        max_page_number = get_max_page_number(soup)

        flat_elements = get_items(soup)
        print('    flat_elements__len = {0} max_page_number = {1}'.format(len(flat_elements), max_page_number))
        for flat_element in flat_elements:
            flat_object = parse_flat_data(flat_element)
            flat_object_json = json.loads(flat_object)
            print(flat_object_json)
            print('    saving flat with id = {0}'.format(flat_object_json['id']))
            # if client.parser.cian_flats.find_one({'id': flat_object_json['id']}):
            #     client.parser.cian_flats.update_one({'id': flat_object_json['id']}, flat_object_json)
            if not hook.find('cian_flats', {'id': flat_object_json['id']}, find_one=True, mongo_db='parser'):
                hook.insert_one('cian_flats', flat_object_json, mongo_db='parser')
        if max_page_number == page or page >= 10000:
            running = False
        page += 1
        time.sleep(10)
        if page % 8 == 0 :
            driver.delete_all_cookies()
