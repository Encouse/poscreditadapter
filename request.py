import datetime
import requests
import os
import logging
from database import clients, orders
from http.client import RemoteDisconnected
from urllib3.exceptions import ProtocolError

logger = logging.getLogger(__name__)

GETCOURSE_ADAPTER_ADRESS = os.environ.get(
    'GCADAPTER_URL', 'http://127.0.0.1:5000/getgc')

CREDIT_ACCEPTED_STATUS = os.environ.get(
    'CREDIT_ACCEPTED_STATUS', 'Кредит предоставлен')
BASE_URL = os.environ.get('POS_BASE_URL', None)

assert BASE_URL, "Please, specify POS_BASE_URL env variable"

GC_ID_MAP = ((0, 'course_listener'),
             (1, 'course_manager'), (2, 'course_platinum'))
GC_ID_MAP_DICT = dict(GC_ID_MAP)


def get_poscredit_session():
    session = requests.Session()

    session.get(f'{BASE_URL}/list/')
    return session


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
    data = {
        'offer': GC_ID_MAP_DICT[int(course_id)],
        'email': email
    }
    requests.post(GETCOURSE_ADAPTER_ADRESS, data)
