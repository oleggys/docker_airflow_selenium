import re
import time
from bs4 import BeautifulSoup

from selenium_scripts.commons.classes import FlatParser, Flat


def get_soup_body(driver, url: str):
    driver.get(url)
    time.sleep(10)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    driver.close()
    return soup


def parse_flat_data(soup) -> str:
    parser = FlatParser(soup)
    id = parser.get_id()
    link = parser.get_link()
    flat_type = parser.get_type()
    square = parser.get_square()
    address = parser.get_address()
    price = parser.get_price()
    floor_number, max_floor_number = parser.get_floor_and_max_floor_numbers()
    price_per_meter = parser.get_price_per_meter(price, square)
    flat = Flat(
        id=id,
        link=link,
        type=flat_type,
        square=square,
        floor=floor_number,
        max_floor_number=max_floor_number,
        price=price,
        price_per_meter=price_per_meter,
        address=address,
    )
    return flat.to_json()


def get_items(soup: BeautifulSoup):
    items = soup.find_all('div', {'data-name': 'LinkArea'})
    return items


def get_max_page_number(soup: BeautifulSoup):
    paginator = soup.find('div', {'data-name': 'Pagination'})
    if paginator is None:
        return None
    items = paginator.find_all('li', {'class': re.compile('\w*--list-item--\w*')})
    if items is []:
        return 0
    numbers = [int(item.text) for item in items if item.text != '..']
    return max(numbers)


def check_available(soup: BeautifulSoup):
    if soup is None:
        return False
    item = soup.find('form', {'id': 'form_captcha'})
    if item is None:
        return True
    return False
