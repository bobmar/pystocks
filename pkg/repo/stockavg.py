from pkg.repo import dbutil

coll_name = 'stockAveragePrice';


class StockAveragePriceDB:
    def __init__(self):
        self._db = dbutil.get_client()

    def find_by_price_date(self, price_date):
        avg_cur = self._db[coll_name].find({"priceDate": price_date})
        result = [avg_item for avg_item in avg_cur]
        return result

    def find_by_price_id(self, price_id):
        avg_cur = self._db[coll_name].find({"_id": price_id})
        avg_list = [avg_item for avg_item in avg_cur]
        if avg_list is None:
            result = None
        else:
            result = avg_list[0]
        return result

