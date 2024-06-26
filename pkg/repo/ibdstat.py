import pymongo
from pkg.repo import dbutil


coll_name = "ibdStatistic"


class IbdStatisticDB:
    def __init__(self):
        self._db = dbutil.get_client()

    def find_stat_by_ticker(self, ticker_symbol):
        stat_cur = self._db[coll_name].find({"tickerSymbol": ticker_symbol}).sort([("priceDate", pymongo.DESCENDING)])
        result = [stat for stat in stat_cur]
        return result

    def find_stat_by_price_id(self, price_id):
        stat_cur = self._db[coll_name].find({"_id": price_id})
        result = [stat for stat in stat_cur]
        return result

    def find_stat_by_price_date(self, price_date):
        stat_cur = self._db[coll_name].find({"priceDate": price_date})
        result = [stat for stat in stat_cur]
        return result

    def update_stat(self, primary_key, field, value):
        query = {'_id': primary_key}
        update = {'$set': {field: value}}
        print(query, update)
        self._db[coll_name].update_one(query, update)

    def insert_stats(self, stat_list):
        result = self._db[coll_name].insert_many(stat_list)
        return result
