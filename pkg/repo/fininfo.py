from pkg.repo import dbutil

coll_ratio = 'financialRatio'
coll_growth = 'financialGrowth'

class FinancialRatio:

    def __init__(self):
        self._db = dbutil.open_db()

    def find_ratios(self, ticker):
        ratio_cur = self._db[coll_ratio].find({'symbol': ticker}).sort('date', -1)
        result = [ratio for ratio in ratio_cur]
        return result

    def find_growth(self, ticker):
        growth_cur = self._db[coll_growth].find({'symbol': ticker}).sort('date', -1)
        result = [fingrowth for fingrowth in growth_cur]
        return result
