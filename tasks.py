from celery import Celery
import celery
from bs4 import BeautifulSoup
from concurrent.futures.thread import ThreadPoolExecutor
from celery.schedules import crontab
from decorators import PROCESS_MAPPING, in_process_flag
import pymongo
from generic import get_questionnaire_data
from request import *
from parse import *
from database import get_mongo_client
from bson.json_util import dumps, loads
from datetime import datetime
from requests import ConnectionError
from settings import (SITE_URL, ACCEPTED_WARNING_EMAIL_TEXT, POS_REJECTED_BANK_STATUS,
                      POS_CANCELLED_BANK_STATUS, SIGNING_REJECT_MESSAGE,
                      POS_ACCEPTED_BANK_STATUS, POS_SIGNED_BANK_STATUS, POS_REJECT_MESSAGE)
import json
import os
import logging

app = Celery('app', broker=os.getenv(
    'CELERY_BROKER_URL', 'redis://127.0.0.1:6379'))

logger = logging.getLogger(__name__)

NOTIFICATOR_URL = os.environ.get('NOTIFICATOR_URL', 'http://notificator:3000')
BANK_ACCEPTED_STATUSID = os.environ.get('BANK_ACCEPTED_STATUSID', 3)


@app.task
def send_mail(to, subject, message):
    url = NOTIFICATOR_URL + '/send_mail'
    response = requests.post(
        url,
        json={
            'to': to,
            'subject': subject,
            'body': message
        }
    )
    return response.status_code


@app.task
@in_process_flag('gc_request')
def send_gc_request_for_order(id, hash):
    data = get_questionnaire_data(id, hash)
    send_getcourse_request(data['model'], data['email'])


@app.task
@in_process_flag('accepted_email_warning')
def send_warning_email(id, hash):
    data = get_questionnaire_data(id, hash)
    send_mail(data['email'], 'Получена заявка на оформление рассрочки на курс',
              ACCEPTED_WARNING_EMAIL_TEXT.format(data['model'],
                                                 data['price']))


@app.task
@in_process_flag('rejected_email_warning')
def rejected_email_warning(id, hash):
    data = get_questionnaire_data(id, hash)
    course_name = GC_NAME_MAP_DICT[data['model'].lower().replace(' ', '')]
    tinkoff_url = f"{SITE_URL}/?offer=tinkoff:{course_name}"
    direct_pay = f"{SITE_URL}/?offer=direct:{course_name}"
    send_mail(data['email'], 'Получен отказ на оформление рассрочки на курс',
              POS_REJECT_MESSAGE.format(data['model'], data['price'], direct_pay, tinkoff_url))


@app.task
@in_process_flag('canceled_email_warning')
def canceled_email_warning(id, hash):
    data = get_questionnaire_data(id, hash)
    site_url = f"{SITE_URL}/#besprc"
    send_mail(data['email'], 'Жаль, что вы отказались от рассрочки!',
              SIGNING_REJECT_MESSAGE.format(site_url, site_url, site_url))


@app.task
def refresh_orders_database():
    session = get_poscredit_bank_session()
    data = get_bank_orders_table(session, status_id=0)
    if data:
        data = data['response']
        for item in data:
            item.update({'processed_at': [], 'in_process': []})
        client = get_mongo_client()
        orders = client.main.orders.find().sort(
            '_id', pymongo.ASCENDING).limit(200)
        ids = {order['id']: True for order in orders}
        insert = [item for item in data if not ids.get(item['id'])]
        logger.info(f'Items for insert {insert}')
        if insert:
            client.main.orders.insert_many(insert)
        client.close()


@app.task
def process_orders(flag_name, status, callback=None):
    if not callback:
        callback = PROCESS_MAPPING[flag_name]
    client = get_mongo_client()
    orders = client.main.orders.find(
        {
            'processed_at': {'$nin': [flag_name]},
            'in_process': {'$nin': [flag_name]},
            'bank_status': status,
        }
    )
    for order in list(orders):
        celery.execute.send_task(f"tasks.{callback}", args=[
                                 order['id'], order['hash']])


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute='*'), refresh_orders_database.s())
    sender.add_periodic_task(
        crontab(minute='*'), process_orders.s('gc_request', POS_SIGNED_BANK_STATUS))
    sender.add_periodic_task(crontab(
        minute='*'), process_orders.s('accepted_email_warning', POS_ACCEPTED_BANK_STATUS))
    sender.add_periodic_task(crontab(
        minute='*'), process_orders.s('rejected_email_warning', POS_REJECTED_BANK_STATUS))
    sender.add_periodic_task(crontab(
        minute='*'), process_orders.s('canceled_email_warning', POS_CANCELLED_BANK_STATUS))
