from celery import Celery
import celery
from bs4 import BeautifulSoup
from concurrent.futures.thread import ThreadPoolExecutor
from celery.schedules import crontab
import pymongo
from request import *
from parse import *
from database import get_mongo_client
from bson.json_util import dumps, loads
from datetime import datetime
from requests import ConnectionError
import json
import os
import logging

app = Celery('app', broker=os.getenv(
    'CELERY_BROKER_URL', 'redis://127.0.0.1:6379'))

logger = logging.getLogger(__name__)


@app.task
def send_gc_request_for_order(id, hash):
    logging.info(f"Processing order {id}")
    mclient = get_mongo_client()
    mclient.main.orders.update_one(
        {'id': id}, {'$set': {'in_process': True}})
    try:
        session = get_poscredit_bank_session()
        questionnaire = get_bank_questionnaire(
            session, id, hash)
        soup = BeautifulSoup(questionnaire.text, "html.parser")
        data = parse_bank_questionnaire_data(soup)
        send_getcourse_request(data['model'], data['email'])
        mclient.main.orders.update_one(
            {'id': id}, {'$set': {'processed_at': datetime.now(), 'items': [data]}})
    except:
        raise
    finally:
        mclient.main.orders.update_one(
            {'id': id}, {'$set': {'in_process': False}})


@app.task
def refresh_orders_database():
    session = get_poscredit_bank_session()
    data = get_bank_orders_table(session)
    if data:
        data = data['response']
        for item in data:
            item.update({'processed_at': None, 'in_process': False})
        client = get_mongo_client()
        orders = client.main.orders.find().sort(
            '_id', pymongo.ASCENDING).limit(200)
        ids = [order['id'] for order in orders]
        insert = [item for item in data if item['id'] not in ids]
        logger.info(f'Items for insert {insert}')
        if insert:
            client.main.orders.insert_many(insert)
        client.close()


@app.task
def process_orders():
    client = get_mongo_client()
    orders = client.main.orders.find(
        {'processed_at': None, 'in_process': False})
    json_orders = list(orders)
    for order in json_orders:
        send_gc_request_for_order.delay(
            order['id'], order['hash'])


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute='*'), refresh_orders_database.s())
    sender.add_periodic_task(crontab(minute='*'), process_orders.s())
