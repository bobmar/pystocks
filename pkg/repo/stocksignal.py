import pymongo
from pkg.repo import dbutil


coll_signal_type = "signalType"
coll_signal = "stockSignal"


class SignalsDB:
    def __init__(self):
        self._db = dbutil.get_client()

    def signal_type_list(self):
        signal_type_cur = self._db[coll_signal_type].find()
        result = [signal_type for signal_type in signal_type_cur]
        return result

    def find_signal_by_ticker(self, ticker_symbol):
        signal_cur = self._db[coll_signal].find({"tickerSymbol": ticker_symbol}).sort([("priceDate", pymongo.DESCENDING), ("signalType", pymongo.ASCENDING)])
        result = [signal for signal in signal_cur]
        signal_cur.close()
        return result

    def find_signal_by_ticker_and_type(self, ticker_symbol, signal_type):
        signal_cur = self._db[coll_signal].find({"tickerSymbol": ticker_symbol, "signalType": signal_type})
        result = [signal for signal in signal_cur]
        signal_cur.close()
        return result

    def find_signal_by_type(self, signal_type):
        signal_cur = self._db[coll_signal].find({"signalType": signal_type})
        result = [signal for signal in signal_cur]
        signal_cur.close()
        return result

    def find_signal_by_price_id(self, price_id):
        signal_cur = self._db[coll_signal].find({"priceId": price_id})
        result = [signal for signal in signal_cur]
        signal_cur.close()
        return result
