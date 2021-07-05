import json

from airflow import DAG
# from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy import DummyOperator

from plugins.selenium_plugin.operators.selenium_operator import SeleniumOperator
from scripts.selenium_scripts.commons.classes import Format
from scripts.selenium_scripts.parser import parse_flats_from_all_pages
from datetime import datetime, timedelta
from airflow.models import Variable

objs = []
titles = ['r1', 'r2', 'r3', 'r4', 'r5', 'r6', 'st', 'sv']
categories = ['room1', 'room2', 'room3', 'room4', 'room5', 'room6', 'room7', 'room9']

RUSSIAN_CITIES = json.loads(Variable.get('RUSSIAN_CITIES'))[0]

for city, val in RUSSIAN_CITIES.items():
    region_idx = val['region_idx']
    if val['parse']:
        for title, cat in zip(titles, categories):
            objs.append({
                'title': title + '_' + city,
                'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2' +
                       '{min_ceiling_height}' +
                       '{minsu_s}' +
                       '{minsu_r}' +
                       '{minsu}' +
                       '{min_balconies}' +
                       '{loggia}' +
                       '{repair%5B0%5D}' +
                       '&offer_type=flat{page}&region=' + str(region_idx) + '&' + cat + '=1' +
                       '{room_type}'
            })





default_args = {
    'owner': 'oleg',
    # 'wait_for_downstream': True,
    'start_date': datetime(2021, 6, 22),
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}


search_filters = {
    'room_type': [1, 2],
    'min_ceiling_height': [2.5, 2.7, 3, 3.5, 4],
    'minsu_s': [1],
    'minsu_r': [1],
    'minsu': [2],
    'min_balconies': [1],
    'loggia': [1],
    'repair%5B0%5D':[1, 2, 3, 4]
}


dag = DAG('parse_flats_data_from_cian',
          schedule_interval=None,
          default_args=default_args, concurrency=2)

start = DummyOperator(
    task_id='start',
    dag=dag)

parse_tasks = []
collection_name = Variable.get('mongo_db_flats_collection_name')

fmt = Format()

for obj in objs:
    k = 0
    for filter, values in search_filters.items():
        k += 1
        for value in values:
            d = {'page': '&p={}'}
            for key in search_filters.keys():
                d.update({key: ''})
            d.update({filter: '&{}={}'.format(filter, value)})
            formatted_url = fmt.format(obj['url'], **d)
            get_flats = SeleniumOperator(
                script=parse_flats_from_all_pages,
                script_args=[formatted_url, collection_name],
                task_id='get_flat_with_category_' + obj['title'] + '_' + str(k) + '_' + str(value),
                dag=dag)
            parse_tasks.append(get_flats)


start >> parse_tasks
