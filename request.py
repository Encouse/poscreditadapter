import datetime
import requests
import os
from database import clients, orders
from http.client import RemoteDisconnected
from urllib3.exceptions import ProtocolError

GETCOURSE_ADAPTER_ADRESS = os.environ.get(
    'GCADAPTER_URL', 'https://127.0.0.1:5000/getgc')

CREDIT_ACCEPTED_STATUS = 'Кредит предоставлен'
BASE_URL = 'https://in.b2pos.ru/eb312687dbf2df0a2d968291bfc4f22d-d41485647614dedca037f9cde6ce4df822862514/'

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


def send_gc_request_for_order(order, retries=5):
    client = clients.find_one({'phone': order['phone']})
    if client:
        email = client['email']
        counter = retries
        while True:
            if not counter:
                raise Exception(f"Retries drowned with order {order['id']}")
            counter -= 1
            try:
                send_getcourse_request(order['items'][0], email)
                break
            except (RemoteDisconnected, ProtocolError) as e:
                continue
        orders.update_one({'_id': order['_id']}, {
                          '$set': {'processed_at': datetime.datetime.now()}})
