import json
import re
from typing import Optional, Any, Dict, List

import requests

def is_town(text: str, cities: List) -> bool:
    if text.lower() in cities:
        return True
    return False

def get_street_and_house_number(text: str) -> [Optional[str], Optional[str]]:
    """
    Function return list which has two elements, street name and house number.
    if text has`t information about street function return [None, None]
    examples:
        >>> text = 'Республика Татарстан, Казань, р-н. Вахитовский, улица Баумана, 12а'
        >>> res = get_street_and_house_number(text)
        >>> res
        ['улица Баумана', '12а']

        >>> text = 'Республика Татарстан, Казань, р-н. Вахитовский, Солнечный ЖК'
        >>> res = get_street_and_house_number(text)
        >>> res
        [None, None]
    """
    regexes = [r'(улица[(\w)(\s)-]+),?\s*(\d+\w*)?', r'([(\w)(\s)-]+улица),?\s*(\d+\w*)?',
               r'(проспект[(\w)(\s)-]+),?\s*(\d+\w*)?', r'([(\w)(\s)-]+проспект),?\s*(\d+\w*)?',
               r'(проезд[(\w)(\s)-]+),?\s*(\d+\w*)?', r'([(\w)(\s)-]+проезд),?\s*(\d+\w*)?']
    for reg in regexes:
        value = re.search(reg, text)
        if value:
            res = [value.group(i).strip() for i in range(1, len(value.groups()) + 1) if value.group(i)]
            for i in range(2 - len(res)):
                res.append(None)
            return res
    return [None, None]


def get_district(text: str) -> Optional[str]:
    """
    Function return dictict.
    examples:
        >>> text = 'Республика Татарстан, Казань, р-н. Вахитовский, улица Баумана, 12'
        >>> res = get_district(text)
        >>> res
        'Вахитовский'

        >>> text = 'Республика Татарстан, Казань, Солнечный ЖК'
        >>> res = get_district(text)
        >>> res
        None
    """
    regexes = [r'р-н([(\w)(\s)-]+)', r'район([(\w)(\s)-]+)', r'([(\w)(\s)-]+)район']
    for reg in regexes:
        value = re.search(reg, text)
        if value:
            return value.group(1).strip()
    return None


def get_local_district(text) -> Optional[str]:
    """
    Function return local dictict.
    examples:
        >>> text = 'Республика Татарстан, Казань, р-н Советский, мкр. Малые Клыки, улица Баумана, 12'
        >>> res = get_local_district(text)
        >>> res
        'Малые Клыки'

        >>> text = 'Республика Татарстан, Казань, Солнечный ЖК'
        >>> res = get_local_district(text)
        >>> res
        None
    """
    regex = r'мкр.([(\s)(\w)-]+)'
    value = re.search(regex, text)
    if value:
        return value.group(1).strip()
    return None

def get_housing_complex(text: str) -> Optional[str]:
    """
    Function return name of housing complex.
    examples:
        >>> text = 'Республика Татарстан, Казань, р-н Советский, мкр. Малые Клыки, улица Баумана, 12'
        >>> res = get_housing_complex(text)
        >>> res
        None

        >>> text = 'Республика Татарстан, Казань, Солнечный ЖК'
        >>> res = get_housing_complex(text)
        >>> res
        'Солнечный ЖК'
    """
    regex = r'([(\w*)(\s*)-]*ЖК)'
    value = re.search(regex, text)
    if value:
        return value.group(0).strip()
    return None


def parse_address_info(flat, cities) -> Dict[str, Any]:
    """
    Function return address info.
    examples:
        >>> flat = {}
        >>> flat['address'] = 'Республика Татарстан, Казань, р-н Советский, мкр. Малые Клыки, улица Баумана, 12'
        >>> res = parse_address_info(flat, ['Казань'])
        >>> res
        {
            'address_info': {
                'region': 'Республика Татарстан',
                'town': 'Казань',
                'district': 'Советский',
                'local_district': 'Малые Клыки',
                'housing_complex': None,
                'street': 'улица Баумана',
                'house_number': '12',
            },
            'location_parsed': True
        }
        >>> flat = {}
        >>> res = parse_address_info(flat, ['Казань'])
        >>> res
        {
            'address_info': None,
            'location_parsed': True
        }
    """
    if 'address' not in flat:
        return {
            'address_info': None,
            'location_parsed': False
        }
    address = flat['address']
    items = address.split(',')
    region = items[0]
    items[1] = items[1].strip()
    if len(items) > 1:
        # check is town or not
        town = items[1] if is_town(items[1], cities) else None
    else:
        town = None
    district = get_district(address)
    local_district = get_local_district(address)
    housing_complex = get_housing_complex(address)
    street, house_number = get_street_and_house_number(address)
    return {
        'address_info': {
            'region': region,
            'town': town,
            'district': district,
            'local_district': local_district,
            'housing_complex': housing_complex,
            'street': street,
            'house_number': house_number,
        },
        'location_parsed': True
    }


def get_flat_geocoding(flat, api_query_url: str, **kwargs) -> [Dict[str, Optional[str]], bool]:
    """
    Function send request to api and get location of house if it exists.
    examples:
        >>> flat = {
            ...,
            'address_info': {
                'region': 'Республика Татарстан',
                'town': 'Казань',
                'district': 'Советский',
                'local_district': 'Малые Клыки',
                'housing_complex': None,
                'street': 'улица Баумана',
                'house_number': '12',
            },
            ...
        }
        >>> res = get_flat_geocoding(flat, 'query_to_api')
        >>> res
        [
            {
                'location': {
                    'lat': '55.8706042',
                    'lng': '49.2313376'
                }
            },
            True
        ]
        >>> flat = {
            ...,
            'address_info': None,
            ...
        }
        >>> res = get_flat_geocoding(flat)
        >>> res
        [
            {
                'location': None
            },
            False
        ]
    """
    address = ''
    res = {'location': None}
    if 'address_info' not in flat or flat['address_info']['town'] is None or flat['address_info']['street'] is None:
        return res, False
    address_info = flat['address_info']
    if address_info['region']:
        address += ', ' + address_info['region']
    if address_info['town']:
        address += ', ' + address_info['town']
    if address_info['street']:
        address += ', ' + address_info['street']
    if address_info['house_number']:
        address += ', ' + address_info['house_number']
    wait = False
    if address:
        response = requests.get(api_query_url + address)
        response_json = json.loads(response.text)
        if not 'error' in response_json:
            response_json = response_json[0]
            res = {
                'location': {
                    'lat': response_json['lat'],
                    'lng': response_json['lon']
                }
            }
        wait = True
    return res, wait


def get_flat_distance_to_center(flat, api_query_url, **kwargs) -> [Dict[str, Optional[str]], bool]:
    """
    Function send request to api and get distance to city center.
    examples:
        >>> flat = {..., location: {...}, ... }
        >>> res = get_flat_distance_to_center(flat, 'query_to_api')
        >>> res
            [
                {
                    'distance_to_center': 4569.8
                },
                True
            ]
        >>> flat = {..., location: None, ... }
        >>> res = get_flat_distance_to_center(flat, 'query_to_api')
        >>> res
            [
                {
                    'distance_to_center': None
                },
                False
            ]
    """
    sub_args = kwargs.pop('sub_args')
    cities = sub_args['cities']
    location = flat['location']
    res = {'distance_to_center': None}
    wait = False
    if location:
        if flat['address_info'] is None or flat['address_info']['town'] is None:
            return res, wait
        city_center_location = cities[flat['address_info']['town'].lower()]
        response = requests.get(api_query_url.format(
            lat=location['lat'], lng=location['lng'],
            city_center_lng=city_center_location['lng'], city_center_lat=city_center_location['lat']
        ))
        response_json = json.loads(response.text)
        if response_json['code'] == 'Ok':
            distance_to_center = response_json['routes'][0]['distance']
            res = {
                'distance_to_center': distance_to_center
            }
        if response_json['code'] == 'Rate Limited Day':
            raise Exception('API ERROR: Rate Limited Day')
        wait = True
    return res, wait