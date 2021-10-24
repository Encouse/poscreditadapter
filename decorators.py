from functools import wraps
from datetime import datetime
from util import rgetattr

from database import get_mongo_client
import logging

logger = logging.getLogger(__name__)

PROCESS_MAPPING = {}


def in_process_flag(*args, **kwargs):
    process_flag = args[0]
    colleciton_path = kwargs.get('colleciton_path')

    def decorator(func):
        PROCESS_MAPPING[process_flag] = func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs):
            logging.info(f"Processing warning {args[0]}")
            mclient = get_mongo_client()
            if colleciton_path:
                collection = rgetattr(mclient, colleciton_path)
            else:
                collection = mclient.main.orders
            logging.info(f"Working with collection {str(collection)}")
            collection.update_one(
                {'id': args[0]}, {'$push': {'in_process': process_flag}})
            try:
                func(*args, **kwargs)
                collection.update_one(
                    {'id': args[0]}, {'$push': {'processed_at': process_flag}})
            except:
                raise
            finally:
                collection.update_one(
                    {'id': args[0]}, {'$pull': {'in_process': process_flag}})
        return wrapper
    return decorator
