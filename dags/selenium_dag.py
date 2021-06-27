import os
from airflow import DAG
# from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy import DummyOperator

from plugins.selenium_plugin.operators.selenium_operator import SeleniumOperator
from selenium_scripts.parser import parse_flats_from_all_pages
from datetime import datetime, timedelta

objs = [
    # {
    #     'title': 'r1',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&'\
    #            'region=4777&room1=1&p='
    # },
    # {
    #     'title': 'r2',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room2=1'
    # },
    # {
    #     'title': 'r3',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room3=1'
    # },
    # {
    #     'title': 'r4',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room4=1'
    # },
    # {
    #     'title': 'r5',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room5=1'
    # },
    # {
    #     'title': 'r6',
    #     'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room6=1'
    # },
    {
        'title': 'st',
        'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room7'
    },
    {
        'title': 'sv',
        'url': 'https://kazan.cian.ru/cat.php?deal_type=sale&engine_version=2&offer_type=flat&p={}&region=4777&room9=1'
    },
]

default_args = {
    'owner': 'oleg',
    # 'wait_for_downstream': True,
    'start_date': datetime(2021, 6, 22),
    'retries': 0,
    'retries_delay': timedelta(minutes=5)
}


dag = DAG('selenium_example_dag',
          schedule_interval=None,
          default_args=default_args, concurrency=1)

start = DummyOperator(
    task_id='start',
    dag=dag)

parse_tasks = []

for obj in objs:

    get_flats = SeleniumOperator(
        script=parse_flats_from_all_pages,
        script_args=[obj['url']],
        task_id='get_flat_with_category_' + obj['title'],
        dag=dag)
    parse_tasks.append(get_flats)


end = DummyOperator(
    task_id='end',
    dag=dag)

start >> parse_tasks >> end
