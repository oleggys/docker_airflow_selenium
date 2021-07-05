import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
from scripts.location_scripts.tools import get_flat_geocoding, get_flat_distance_to_center, parse_address_info

default_args = {
    'owner': 'oleg',
    'start_date': datetime(2021, 6, 22),
    'email': ['gysevov@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

dag = DAG('geo_dag',
          schedule_interval=None,
          default_args=default_args)


hook = MongoHook('parser')

flats_collection_name = Variable.get('mongo_db_flats_collection_name')
flats_database_name = Variable.get('mongo_db_flats_database_name')

# flats = list(hook.find(collection_name, {"location":  {"$or": [{"$exists": False}, {"$eq": None}]}}, mongo_db='parser'))

API_KEY = Variable.get('MAP_API_KEY')

RUSSIAN_CITIES = json.loads(Variable.get('RUSSIAN_CITIES'))[0]


API_QUERY_URL_GEOCODING = f'https://eu1.locationiq.com/v1/search.php?key={API_KEY}&format=json&q='
API_QUERY_URL_ROUTING = 'https://eu1.locationiq.com/v1/directions/driving/{lng},{lat};' \
                        '{city_center_lng},{city_center_lat}' \
                        f'?key={API_KEY}&steps=false&alternatives=false&overview=full'

def get_flats_info(query, api_query_url, function, **sub_args):
    k = 0
    flats = list(hook.find(flats_collection_name, query, mongo_db=flats_database_name))
    flats_len = len(flats)
    for i, flat in enumerate(flats):
        res, wait = function(flat, api_query_url, **sub_args)
        k += 1
        if hook.find(flats_collection_name, {'id': flat['id']}, find_one=True, mongo_db=flats_database_name):
            hook.update_one(flats_collection_name, {'id': flat['id']}, {"$set": res}, mongo_db=flats_database_name)
        if i % 10 == 0:
            print(f'Taken info for {i}/{flats_len}')
        if k == 4700:
            break
        if wait:
            time.sleep(1)


def parse_address():
    query = {"location_parsed": {"$exists": False}}
    flats = list(hook.find(flats_collection_name, query, mongo_db=flats_database_name))
    flats_len = len(flats)
    cities = [city.lower() for city in RUSSIAN_CITIES.keys()]
    for i, flat in enumerate(flats):
        parsed_info = parse_address_info(flat, cities=cities)
        if hook.find(flats_collection_name, {'id': flat['id']}, find_one=True, mongo_db=flats_database_name):
            hook.update_one(flats_collection_name, {'id': flat['id']}, {"$set": parsed_info}, mongo_db=flats_database_name)
        if i % 100 == 0:
            print(f'Parsed {i}/{flats_len}')


parse_address_info_operator = PythonOperator(
    task_id='parse_address_info',
    python_callable=parse_address,
    dag=dag)


get_flats_location_operator = PythonOperator(
    task_id='flats_location',
    python_callable=get_flats_info,
    op_kwargs={'query': {"$or": [{"location": {"$exists": False}}, {"location": {"$eq": None}}]},
               'api_query_url': API_QUERY_URL_GEOCODING,
               'function': get_flat_geocoding},
    dag=dag)


query = {
    "$and" : [
        {"$and": [
            { "location": {"$exists": True}},
            { "location": {"$ne": None}},
        ]},
        {"$or": [
            { "distance_to_center": {"$exists": False}},
            { "distance_to_center": {"$eq": None}},
        ]}
    ]
}

get_flats_distance_to_center_operator = PythonOperator(
    task_id='flats_distance_to_center',
    python_callable=get_flats_info,
    op_kwargs={'query': query,
               'api_query_url': API_QUERY_URL_ROUTING,
               'function': get_flat_distance_to_center,
               'sub_args': {'cities': RUSSIAN_CITIES}},
    dag=dag)


parse_address_info_operator >> get_flats_location_operator >> get_flats_distance_to_center_operator

# flat_tasks
#['москва', 'санкт-петербург', 'новосибирск', 'екатеринбург', 'казань', 'нижний новгород', 'челябинск', 'самара', 'омск', 'ростов-на-дону', 'уфа', 'красноярск', 'воронеж', 'пермь', 'волгоград', 'краснодар', 'саратов', 'тюмень', 'тольятти', 'ижевск', 'барнаул', 'ульяновск', 'иркутск', 'хабаровск', 'махачкала', 'ярославль', 'владивосток', 'оренбург', 'томск', 'кемерово', 'новокузнецк', 'рязань', 'набережные челны', 'астрахань', 'киров', 'пенза', 'балашиха', 'липецк', 'чебоксары', 'калининград', 'тула', 'севастополь', 'ставрополь', 'курск', 'улан-удэ', 'сочи', 'тверь', 'магнитогорск', 'иваново', 'брянск', 'белгород', 'сургут', 'владимир', 'чита', 'архангельск', 'нижний тагил', 'симферопoль', 'калуга', 'якутск', 'грозный', 'волжский', 'смоленск', 'саранск', 'череповец', 'курган', 'подольск', 'вологда', 'орёл', 'владикавказ', 'тамбов', 'мурманск', 'петрозаводск', 'нижневартовск', 'кострома', 'йошкар-ола', 'новороссийск', 'стерлитамак', 'химки', 'таганрог', 'мытищи', 'сыктывкар', 'комсомольск-на-амуре', 'нижнекамск', 'нальчик', 'шахты', 'дзержинск', 'энгельс', 'благовещенск', 'королёв', 'братск', 'великий новгород', 'орск', 'старый оскол', 'ангарск', 'псков', 'люберцы', 'южно-сахалинск', 'бийск', 'прокопьевск', 'абакан', 'армавир', 'балаково', 'норильск', 'рыбинск', 'северодвинск', 'петропавловск-камчатский', 'красногорск', 'уссурийск', 'волгодонск', 'новочеркасск', 'сызрань', 'каменск-уральский', 'златоуст', 'альметьевск', 'электросталь', 'керчь', 'миасс', 'салават', 'хасавюрт', 'пятигорск', 'копейск', 'находка', 'рубцовск', 'майкоп', 'коломна', 'березники', 'одинцово', 'домодедово', 'ковров', 'нефтекамск', 'каспийск', 'нефтеюганск', 'кисловодск', 'новочебоксарск', 'батайск', 'щёлково', 'дербент', 'серпухов', 'назрань', 'раменское', 'черкесск', 'новомосковск', 'кызыл', 'первоуральск', 'новый уренгой', 'орехово-зуево', 'долгопрудный', 'обнинск', 'невинномысск', 'ессентуки', 'октябрьский', 'димитровград', 'пушкино', 'камышин', 'ноябрьск', 'евпатория', 'реутов', 'жуковский', 'северск', 'муром', 'новошахтинск', 'артём', 'ачинск', 'бердск', 'элиста', 'арзамас', 'ханты-мансийск', 'ногинск', 'елец', 'железногорск', 'зеленодольск', 'новокуйбышевск', 'сергиев посад', 'тобольск', 'воткинск', 'саров', 'междуреченск', 'михайловск', 'серов', 'сарапул', 'анапа', 'ленинск-кузнецкий', 'ухта', 'воскресенск', 'соликамск', 'глазов', 'магадан', 'великие луки', 'мичуринск', 'лобня', 'гатчина', 'канск', 'каменск-шахтинский', 'губкин', 'бузулук', 'киселёвск', 'ейск', 'ивантеевка', 'новотроицк', 'чайковский', 'бугульма', 'железногорск', 'юрга', 'кинешма', 'азов', 'кузнецк', 'усть-илимск', 'новоуральск', 'клин', 'видное', 'мурино', 'ялта', 'озёрск', 'кропоткин', 'бор', 'всеволожск', 'геленджик', 'черногорск', 'усолье-сибирское', 'балашов', 'новоалтайск', 'дубна', 'шадринск', 'верхняя пышма', 'выборг', 'елабуга', 'минеральные воды', 'егорьевск', 'троицк', 'чехов', 'чапаевск', 'белово', 'биробиджан', 'когалым', 'кирово-чепецк', 'дмитров', 'туймазы', 'славянск-на-кубани', 'феодосия', 'минусинск', 'сосновый бор', 'наро-фоминск', 'анжеро-судженск', 'кстово', 'сунжа', 'буйнакск', 'ступино', 'георгиевск', 'заречный', 'горно-алтайск', 'белогорск', 'белорецк', 'кунгур', 'ишим', 'урус-мартан', 'ишимбай', 'павловский посад', 'клинцы', 'гуково', 'россошь', 'асбест', 'котлас', 'зеленогорск', 'донской', 'лениногорск', 'избербаш', 'туапсе', 'вольск', 'ревда', 'будённовск', 'берёзовский', 'сибай', 'полевской', 'лыткарино', 'лысьва', 'кумертау', 'белебей', 'нерюнгри', 'лесосибирск', 'фрязино', 'сертолово', 'чистополь', 'прохладный', 'борисоглебск', 'нягань', 'лабинск', 'крымск', 'тихвин', 'гудермес', 'алексин', 'александров', 'михайловка', 'ржев', 'щёкино', 'тихорецк', 'сальск', 'шали', 'павлово', 'шуя', 'мелеуз', 'краснотурьинск', 'искитим', 'североморск', 'апатиты', 'свободный', 'выкса', 'лиски', 'дзержинский', 'волжск', 'вязьма', 'воркута', 'гусь-хрустальный', 'снежинск', 'краснокамск', 'арсеньев', 'краснокаменск', 'белореченск', 'салехард', 'жигулёвск', 'котельники', 'тимашёвск', 'кириши']