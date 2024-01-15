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
