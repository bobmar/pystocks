from pkg.repo import dbutil

coll_name = 'financialRatio'


class FinancialRatio:

    def __init__(self):
        self._db = dbutil.open_db()

    def find_ratios(self, ticker):
        ratio_cur = self._db[coll_name].find({'symbol': ticker}).sort('date', -1)
        result = [ratio for ratio in ratio_cur]
        return result
