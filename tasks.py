from celery import Celery
import celery
from bs4 import BeautifulSoup
from concurrent.futures.thread import ThreadPoolExecutor
from celery.schedules import crontab
import pymongo
from request import *
from parse import *
from database import get_mongo_client
from bson.json_util import dumps
from datetime import datetime
from requests import ConnectionError
import json
import os
import logging

app = Celery('app', broker=os.getenv(
    'CELERY_BROKER_URL', 'redis://127.0.0.1:6379'))

logger = logging.getLogger(__name__)


@app.task(bind=True, retry_kwargs={'max_retries': 5})
def send_gc_request_for_order(self, order):
    mclient = get_mongo_client()
    mclient.main.orders.update_one(
        {'id': order['id']}, {'$set': {'in_process': True}})
    try:
        session = get_poscredit_bank_session()
        questionnaire = get_bank_questionnaire(
            session, order['id'], order['hash'])
        soup = BeautifulSoup(questionnaire.text, "html.parser")
        data = parse_bank_questionnaire_data(soup)
        send_getcourse_request(data['model'], data['email'])
        mclient.main.orders.update_one(
            {'id': order['id']}, {'$set': {'processed_at': datetime.now()}})
    except:
        raise
    finally:
        mclient.main.orders.update_one(
            {'id': order['id']}, {'$set': {'in_process': False}})


@app.task
def refresh_orders_database():
    session = get_poscredit_bank_session()
    data = get_bank_orders_table(session)
    if data:
        data = data['response']
        for item in data:
            item.update({'processed_at': None, 'in_process': False})
        client = get_mongo_client()
        orders = client.main.orders
        last_item = orders.find().sort({'_id': -1}).limit(1)
        if not last_item:
            orders.insert_many(data)
        else:
            last_item_id = dumps(last_item)['id']
            logger.info(f"Insert last item id {last_item_id}")
            insert = []
            for item in data:
                if item['id'] == last_item_id:
                    break
                insert.append(item)
            orders.insert_many(insert)
        client.close()


@app.task
def process_orders():
    client = get_mongo_client()
    orders = client.main.orders.find(
        {'processed_at': None, 'in_process': False})
    for order in dumps(orders):
        send_gc_request_for_order.delay(order)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute='*'), refresh_orders_database.s())
    sender.add_periodic_task(crontab(minute='*'), process_orders.s())
