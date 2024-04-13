from pymongo import MongoClient
import json


def open_db():
    stock_conn_file = open("stock-db.json", "r")
    stock_conn_dict = json.load(stock_conn_file)
    try:
        client = MongoClient(stock_conn_dict["url"])
        return client.stocks
    except:
        return None


class CommonMongo:
    def __init__(self):
        self._db = open_db()

    def getclient(self):
        return self._db

common_db = CommonMongo()


def get_client():
    return common_db.getclient()