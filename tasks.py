from celery import Celery
from bs4 import BeautifulSoup
from concurrent.futures.thread import ThreadPoolExecutor
from celery.schedules import crontab
from request import *
from parse import *
from database import get_mongo_client
from datetime import datetime
import os

app = Celery('app', broker=os.getenv('CELERY_BROKER_URL', 'redis://127.0.0.1:6379'))


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
    last_item = orders.find().sort('_id', 1).find_one()
    if data and data[0]['id'] != last_item['id']:
        insert = []
        for item in data:
            if item['id'] == last_item['id']:
                break
            insert.append(item)
        orders.insert_many(insert)


@app.task
def process_getcourse_orders():
    client = get_mongo_client()
    orders = client.main.orders
    orders_not_processed = orders.find({'processed_at': None})
    with ThreadPoolExecutor(max_workers=10) as executor:
        for order in orders_not_processed:
            executor.submit(send_gc_request_for_order, order)


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(crontab(minute='*'), refresh_orders_database.s())
    sender.add_periodic_task(crontab(minute='*'), process_getcourse_orders.s())
