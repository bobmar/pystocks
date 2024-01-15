import pymongo
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


coll_name = "yahooKeyStatistic"


class YahooStatDB:
    def __init__(self):
        self._db = open_db()

    def find_by_price_date(self, ticker, price_date):
        yahoo_cur = self._db[coll_name].find({"tickerSymbol": ticker, "priceDate": {"$lte": price_date}}).limit(10).sort([("priceDate", pymongo.DESCENDING)])
        return [stat for stat in yahoo_cur]

    def find_by_ticker(self, ticker):
        yahoo_cur = self._db[coll_name].find({"tickerSymbol": ticker}).sort([("priceDate", pymongo.DESCENDING)])
        return [stat for stat in yahoo_cur]
