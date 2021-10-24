import datetime
import requests
import os
import logging
import json
from database import clients, orders
from http.client import RemoteDisconnected
from urllib3.exceptions import ProtocolError

from exceptions import RetryError

logger = logging.getLogger(__name__)

GETCOURSE_ADAPTER_ADRESS = os.environ.get(
    'GCADAPTER_URL', 'http://127.0.0.1:5000/getgc')

CREDIT_ACCEPTED_STATUS = os.environ.get(
    'CREDIT_ACCEPTED_STATUS', 'Кредит предоставлен')
BASE_URL = os.environ.get('POS_BASE_URL', None)

BANK_PASSWORD = os.environ.get('BANK_PASSWORD')
BANK_LOGIN = os.environ.get('BANK_LOGIN')
BANK_COMPANY_ID = os.environ.get('BANK_COMPANY_ID', '22862')
BANK_ACCESS_TRADE = os.environ.get('BANK_ACCESS_TRADE', '156678')
BANK_SUCCESS_STATUSID = os.environ.get('BANK_SUCCESS_STATUSID', 1)


assert BASE_URL, "Please, specify POS_BASE_URL env variable"

GC_ID_MAP = ((0, 'course_listener'),
             (1, 'course_manager'), (2, 'course_platinum'))
GC_NAME_MAP = (('Курс СЛУШАТЕЛЬ', 'course_listener'), ('Курс МЕНЕДЖЕР',
               'course_manager'), ('Курс PLATINUM', 'course_platinum'))
GC_NAME_MAP_DICT = dict(((name.lower().replace(' ', ''), course)
                        for name, course in GC_NAME_MAP))
GC_ID_MAP_DICT = dict(GC_ID_MAP)


def get_poscredit_session():
    session = requests.Session()

    session.get(f'{BASE_URL}/list/')
    return session


def get_poscredit_bank_session(retries=5):
    session = requests.Session()
    counter = retries
    while not session.cookies.get('PHPSESSID'):
        if counter <= 0:
            raise RetryError('Retries exceeded getting bank session')
        counter -= 1
        session.post(
            'https://bank.b2pos.ru/',
            files={'login': (None, BANK_LOGIN),
                   'password': (None, BANK_PASSWORD),
                   'enter': (None, 'Отправить')})

        session.post(
            'https://bank.b2pos.ru/',
            files={'access_company': (None, BANK_COMPANY_ID),
                   'access_trade': (None, BANK_ACCESS_TRADE),
                   'access_point_success': (None, 'Отправить')})
    return session


def get_bank_orders_table(session, status_id=BANK_SUCCESS_STATUSID):
    response = session.post('https://bank.b2pos.ru/services/search_profiles.php', files={
        'time_end': (None, datetime.datetime.now().strftime('%d.%m.%Y')),
        'time_start': (None, (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%d.%m.%Y')),
        'statusId': (None, status_id),
    })
    return json.loads(response.text)


def get_bank_questionnaire(session, id, hash):
    response = session.get(f"https://bank.b2pos.ru/view/{id}_{hash}/")
    return response


def get_poscredit_orders(session, status=CREDIT_ACCEPTED_STATUS, **kwargs):
    files = {
        'credit_status': (None, status),
        **kwargs
    }

    response = session.post(
        'https://in.b2pos.ru/action/request_search.php', files=files)
    return response


def get_order_details(session, id):
    response = session.get(f"{BASE_URL}/view/{id}")
    return response


def send_getcourse_request(course_id, email):
    offer = None
    try:
        offer = GC_ID_MAP_DICT[int(course_id)]
    except (KeyError, ValueError):
        offer = GC_NAME_MAP_DICT[course_id.lower().replace(' ', '')]
    data = {
        'offer': offer,
        'email': email
    }
    requests.post(GETCOURSE_ADAPTER_ADRESS, data=json.dumps(data), headers={
                  'Content-type': 'application/json'})
