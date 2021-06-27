import json
import re
from enum import IntEnum
from typing import List, Dict, T
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup


class FlatType(IntEnum):
    BLANK = 0
    ONE_ROOM = 1
    TWO_ROOM = 2
    THREE_ROOM = 3
    FOUR_ROOM = 4
    FIVE_ROOM = 5
    SIX_ROOM = 6
    STUDIO = 7
    FREE_PLAN = 8
    GUEST = 9


@dataclass
class Flat:
    id: str
    link: str
    type: FlatType
    square: float
    floor: int = None
    max_floor_number: int = None
    address: str = None
    price: float = None
    price_per_meter: int = None

    def to_json(self):
        return json.dumps(asdict(self))


class FlatParser:

    def __init__(self, soup: BeautifulSoup):
        self.soup = soup

    def get_type(self) -> FlatType:
        """
        parse type from soup object
        :param soup: BeautifulSoup
        :return: FlatType
        """
        flat_types = {
            '1-комн. кв.': FlatType.ONE_ROOM,
            '1-к квартира': FlatType.ONE_ROOM,
            '2-комн. кв.': FlatType.TWO_ROOM,
            '2-к квартира': FlatType.TWO_ROOM,
            '3-комн. кв.': FlatType.THREE_ROOM,
            '3-к квартира': FlatType.THREE_ROOM,
            '4-комн. кв.': FlatType.FOUR_ROOM,
            '4-к квартира': FlatType.FOUR_ROOM,
            '5-комн. кв.': FlatType.FIVE_ROOM,
            '5-к квартира': FlatType.FIVE_ROOM,
            '6-комн. кв.': FlatType.SIX_ROOM,
            '6-к квартира': FlatType.SIX_ROOM,
            'Студия': FlatType.STUDIO,
            'Своб. планировка': FlatType.FREE_PLAN,
            'Гостинка': FlatType.GUEST

        }
        item = self.soup.find('div', {'data-name': 'TitleComponent'})
        if not item:
            return FlatType.BLANK

        regex = re.compile('(\d{1}-комн\. кв\.|\d{1}-к квартира|Студия|Гостинка|Своб\. планировка)')
        text = item.text.split(',')[0]
        search_object = re.search(regex, text)
        if search_object is None:
            return FlatType.BLANK
        type_str = search_object.group(0)
        return flat_types[type_str]

    def _get_square_from_text(self, text) -> float:
        square_regex = r'(\d+[,.]?\d*)\s+м'
        square_value = re.search(square_regex, text)
        if not square_regex:
            return 0.0
        return float(square_value.group(1).replace(',', '.'))

    def _get_price_from_text(self, text) -> float:
        square_regex = r'(\d+[,.]?\d*)'
        square_value = re.search(square_regex, text.replace(' ', ''))
        if not square_regex:
            return 0.0
        return float(square_value.group(1).replace(',', '.'))

    def get_square(self) -> float:
        item = self.soup.find('div', {'data-name': 'TitleComponent'})
        if item is None:
            return 0
        square_value = self._get_square_from_text(item.text)
        return square_value

    def get_floor_and_max_floor_numbers(self) -> [int, int]:
        item = self.soup.find('div', {'data-name': 'TitleComponent'})
        floor_number = 0
        max_floor_number = 0
        if item is None:
            return floor_number, max_floor_number
        square_regex = r'(\d+)/(\d+)'
        numbers = re.search(square_regex, item.text)
        if numbers:
            floor_number = int(numbers.group(1))
            max_floor_number = int(numbers.group(2))
        return floor_number, max_floor_number

    def get_price(self) -> float:
        item = self.soup.find('span', {'data-mark': 'MainPrice'})
        if item is None:
            return 0.0
        price = self._get_price_from_text(item.text)
        return price

    def get_price_per_meter(self, price, square):
        return int(price / square)

    def get_address(self):
        item = self.soup.find('div', {'class': re.compile('\w*--labels--\w*')})
        if item is None:
            return ''
        return item.text.replace('\n', '').strip()

    def get_link(self):
        item = self.soup.find('a', {'class': re.compile('\w*--link--\w*')})
        href = item['href']
        return href.strip()

    def get_id(self):
        item = self.soup.find('a', {'class': re.compile('\w*--link--\w*')})
        href = item['href']
        id = href.split('/')[-2]
        return id

