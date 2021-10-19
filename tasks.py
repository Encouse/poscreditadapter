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


@app.task
def refresh_orders_database():
    client = get_mongo_client()
    orders = client.main.orders
    session = get_poscredit_session()
    response = get_poscredit_orders(session)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = parse_order_table(soup)
    for item in data:
        item.update({'processed_at': None})
    last_item = orders.find_one(sort=[('_id', pymongo.ASCENDING)])
    logger.info(f"Insert last item id {dumps(last_item)}")
    insert = []
    for item in data:
        if last_item:
            if item['id'] == last_item['id']:
                break
        insert.append(item)
        celery.execute.send_task("tasks.send_gc_request_for_order", args=[item], kwargs={})
    if insert:
        orders.insert_many(insert)
    client.close()


@app.task(bind=True, retry_kwargs={'max_retries': 5})
def send_gc_request_for_order(self, order):
    session = get_poscredit_session()
    response = get_order_details(session, order['id'])
    soup = BeautifulSoup(response.text, "html.parser")
    order_detail = parse_order_details(soup)
    mclient = get_mongo_client()
    clients = mclient.main.clients
    phone = order_detail['phone'].replace(
        ' ', '').replace('-', '').replace('+', '')
    logger.info(f'Got result phone {phone}')
    client = clients.find_one({'phone': phone})
    try:
        if client:
            logger.info(f"Found client, processing order")
            email = client['email']
            if order_detail['items']:
                logger.info('Sending getcourse request')
                send_getcourse_request(order_detail['items'][0]['id'], email)
        orders.update_one({'id': order['id']}, {
            '$set': {'processed_at': datetime.datetime.now()}})
    except (RemoteDisconnected, ProtocolError, ConnectionError) as e:
        raise self.retry(countdown=10)
    finally:
        mclient.close() 


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute='*'), refresh_orders_database.s())
