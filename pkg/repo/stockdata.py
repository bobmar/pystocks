import pymongo
import datetime
from pkg.repo import dbutil


def calc_price_id(weeks, ticker, price_date):
    delta = datetime.timedelta(weeks * 7)
    search_date = (price_date - delta)
    previous_day = 1
    if search_date.weekday() == 0:
        previous_day = 3
    search_date_1 = search_date - datetime.timedelta(previous_day)
    param_date = search_date.strftime('%Y-%m-%d')
    param_date_1 = search_date_1.strftime('%Y-%m-%d')
    price_id = ticker + ":" + param_date
    price_id_1 = ticker + ":" + param_date_1
    return price_id, price_id_1


class StocksDB:

    def __init__(self):
        self._db = dbutil.get_client()
        self._st_list = ["UPDNVOL50", "NETABVBLW50", "AVG20V200", "ZSCORE", "TRMOM", "DYPRCV50A", "DYPRCV200A", "STDDEV2WK", "STDDEV10WK"]
        self._st_list.sort()
        self._stat_dict = {
            "PCTCHG4WK": {"value": "PctChg4Weeks", "numWeeks": 4},
            "PCTCHG8WK": {"value": "PctChg8Weeks", "numWeeks": 8},
            "PCTCHG12WK": {"value": "PctChg12Weeks", "numWeeks": 12}
        }

    def stat_dict(self):
        return self._stat_dict

    def ticker_symbol_random_list(self):
        """
        Retrieve ticker_cnt ticker symbols at random.
        """
        ticker_cur = self._db["stockTicker"].find({})
        ticker_list = [ticker["_id"] for ticker in ticker_cur]
        selected_tickers = ticker_list
        return selected_tickers

    def ticker_symbol_list(self, ticker_cnt):
        """
        Retrieve the first ticker_cnt ticker symbols.
        """
        if ticker_cnt == 0:
            ticker_cur = self._db["stockTicker"].find({}, {"_id": 1})
        else:
            ticker_cur = self._db["stockTicker"].find({}, {"_id": 1}).limit(ticker_cnt)
        result = [ticker["_id"] for ticker in ticker_cur]
        ticker_cur.close()
        return result

    def ticker_list(self, ticker_cnt):
        if ticker_cnt == 0:
            ticker_cur = self._db["stockTicker"].find({})
        else:
            ticker_cur = self._db["stockTicker"].find({}).limit(ticker_cnt)
        result = [ticker for ticker in ticker_cur]
        return result

    def delete_ticker_in_list(self, tickers):
        result = self._db["stockTicker"].delete_many({"_id": {"$in": tickers}})
        return result.deleted_count

    def ibd_max_stat(self):
        ibd_stat_cur = self._db["ibdStatistic"].find().sort("priceDate", -1).limit(1)
        result = [ibd_stat for ibd_stat in ibd_stat_cur]
        return result

    def ibd_unique_ticker(self):
        ibd_ticker_cur = self._db["ibdStatistic"].find({"priceDate": self.ibd_max_stat()[0]["priceDate"]})
        ibd_ticker_list = [ticker["tickerSymbol"] for ticker in ibd_ticker_cur]
        return ibd_ticker_list

    def ibd_ticker_delta(self):
        """
        Retrieve a list of ticker symbols and a list of unique IBD statistic tickers.
        Return a list of ticker symbols representing tickers with no IBD statistics.
        """
        ticker_cur = self._db["stockTicker"].find({}, {"_id": 1})
        ticker_list = [ticker["_id"] for ticker in ticker_cur]
        ibd_ticker_list = self.ibd_unique_ticker()
        print("Ticker count", len(ticker_list), "IBD ticker count", len(ibd_ticker_list))
        delta = list(set(ticker_list) - set(ibd_ticker_list))
        return delta

    def stat_feature_dict(self, weeks, ticker_symbol, price_date):
        price_id, price_id_1 = calc_price_id(weeks, ticker_symbol, price_date)
        stat_cur = self._db["stockStatistic"].find({"statisticType": {"$in": self._st_list}, "priceId": price_id})
        if stat_cur is None:
            stat_cur = self._db["stockStatistic"].find({"statisticType": {"$in": self._st_list}, "priceId": price_id_1})
        stat_features = {}
        for stat in stat_cur:
            stat_features.update({stat["statisticType"]: float(stat["statisticValue"])})
        return stat_features

    def ibd_stat_list(self, ticker_symbol):
        ibd_stat_cur = self._db["ibdStatistic"].find({"tickerSymbol": ticker_symbol}).sort("priceDate", pymongo.DESCENDING)
        result = [ibd_stat for ibd_stat in ibd_stat_cur]
        return result

    def ibd_stat_by_price_id(self, price_id):
        ibd_stat_cur = self._db["ibdStatistic"].find({"priceId": price_id})
        result = [ibd_stat for ibd_stat in ibd_stat_cur]
        return result

    def find_pctchg_stats(self, stat_type, num_weeks, tickers):
        stats_coll = self._db["stockStatistic"]
        stats_cur = stats_coll.find({"statisticType": stat_type, "tickerSymbol": {"$in": tickers}})
        stats_dict = {}
        for stat in stats_cur:
            stat_feat_dict = self.stat_feature_dict(num_weeks, stat["tickerSymbol"], stat["priceDate"])
            if stat_feat_dict:
                if float(stat["statisticValue"]) > 0:
                    stats_dict.update(
                        {stat["priceId"]: (float(stat["statisticValue"]),) + tuple(stat_feat_dict.values())})
        return stats_dict, list(stat_feat_dict.keys())

    def stat_type_list(self):
        stat_type_cur = self._db["statisticType"].find({}, {"_id": 1}).sort("_id", pymongo.ASCENDING)
        result = [stat_type["_id"] for stat_type in stat_type_cur]
        return result

    def find_stat_by_type(self, stat_type, num_weeks, limit_cnt, asc_desc):
        start_date = datetime.datetime.now() - datetime.timedelta(weeks=num_weeks)
        stat_cur = self._db["stockStatistic"].find({"statisticType": stat_type, "priceDate": {"$gte": start_date}}).sort("statisticValue", asc_desc).limit(limit_cnt)
        result = [stat for stat in stat_cur]
        return result

    def find_stat_by_ticker(self, ticker_symbol, price_date):
        stat_cur = self._db["stockStatistic"].find({"tickerSymbol": ticker_symbol, "priceDate": price_date}) \
            .sort("statisticType", pymongo.ASCENDING)
        result = [stat for stat in stat_cur]
        return result

    def find_stat_by_price_id(self, price_id):
        with pymongo.timeout(60):
            stat_cur = self._db["stockStatistic"].find({"priceId": price_id}).sort("statisticType", pymongo.ASCENDING)
        result = [stat for stat in stat_cur]
        return result

    def find_stats_by_tickers_and_stat_type(self, ticker_list, stat_type):
        stats_cur = self._db["stockStatistic"].find({"statisticType": stat_type, "tickerSymbol": {"$in": ticker_list}}) \
            .sort("priceDate")
        result = [stat for stat in stats_cur]
        return result

    def find_selected_stat_by_price_id(self, price_id, stat_type_list):
        stat_cur = self._db.stockStatistic.find({"statisticType": {"$in": stat_type_list}, "priceId": price_id}) \
            .sort("statisticType")
        return [stat for stat in stat_cur]

    def find_price_by_ticker(self, ticker_symbol):
        price_cur = self._db["stockPrice"].find({"tickerSymbol": ticker_symbol}).sort("priceDate", pymongo.ASCENDING)
        return [price for price in price_cur]

    def find_stat_by_ticker_and_type(self, ticker_symbol, stat_type):
        stat_cur = self._db["stockStatistic"].find({"tickerSymbol": ticker_symbol, "statisticType": stat_type}).sort("priceDate", pymongo.ASCENDING)
        return [stat for stat in stat_cur]

    def find_signal_count(self, start_date):
        sig_cnt_cur = self._db["signalTypeCount"].find({"signalDate": {"$gte": start_date}}, {"signalDate": 1, "signalCode": 1, "signalCount": 1}).sort([("signalDate", pymongo.DESCENDING), ("signalCount", pymongo.DESCENDING)])
        return [sig_cnt for sig_cnt in sig_cnt_cur]

    def find_price_dates(self, limit):
        price_date_cur = self._db["stockPrice"].distinct("priceDate")
        return sorted([price_date for price_date in price_date_cur], key=None, reverse=True)[0:limit]
