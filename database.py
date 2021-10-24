from pymongo import MongoClient


def get_mongo_client():
    client = MongoClient('mongo', 27017)
    return client

client = get_mongo_client()
db = client.main
orders = db.orders
clients = db.clients
warnings = db.warnings

