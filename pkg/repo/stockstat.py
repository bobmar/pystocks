import pymongo
from pkg.repo import dbutil


coll_stat_type = "statisticType"
coll_stat = "stockStatistic"
coll_stat_candidate = 'statCandidate'


class StatisticsDB:
    def __init__(self):
        self._db = dbutil.get_client()

    def stat_type_list(self):
        stat_type_cur = self._db[coll_stat_type].find()
        return [stat_type for stat_type in stat_type_cur]

    def find_stat_by_ticker(self, ticker_symbol):
        stat_cur = self._db[coll_stat].find({"tickerSymbol": ticker_symbol}).sort([("statisticType", pymongo.ASCENDING), ("priceDate", pymongo.DESCENDING)])
        return [stat for stat in stat_cur]

    def find_stat_by_ticker_and_type(self, ticker_symbol, stat_type):
        stat_cur = self._db[coll_stat].find({"tickerSymbol": ticker_symbol, "statisticType": stat_type}).sort([("priceDate", pymongo.DESCENDING)])
        return [stat for stat in stat_cur]

    def find_stat_by_type(self, stat_type, limit_cnt):
        stat_cur = self._db[coll_stat].find({"statisticType": stat_type}).sort([("priceDate", pymongo.DESCENDING), ("statisticValue", pymongo.DESCENDING)]).limit(limit_cnt)
        return [stat for stat in stat_cur]

    def find_stat_by_price_id(self, price_id):
        stat_cur = self._db[coll_stat].find({"priceId": price_id})
        return [stat for stat in stat_cur]

    def find_stat_by_type_and_date(self, stat_type, price_date, limit_cnt):
        stat_cur = self._db[coll_stat].find({"statisticType": stat_type, "priceDate": price_date}).sort([("statisticValue", pymongo.DESCENDING)]).limit(limit_cnt)
        return [stat for stat in stat_cur]

    def save_candidate_stat(self, candidate_stat_list):
        return self._db[coll_stat_candidate].insert_many(candidate_stat_list)

    def drop_candidate_stat(self):
        return self._db[coll_stat_candidate].drop()
