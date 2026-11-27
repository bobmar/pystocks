from pkg.repo import dbutil

coll_ratio = 'financialRatio'
coll_growth = 'financialGrowth'
coll_trans = 'brokerTransactions'
aggr_income = [
    {
        '$match': {
            'action': {
                '$in': [
                    'Sell to Open', 'Reinvest Dividend', 'Non-Qualified Div', 'Bank Interest', 'Qualified Dividend', 'Qual Div Reinvest', 'Non-Qualified Div', 'Cash Dividend', 'Reinvest Dividend'
                ]
            }
        }
    }, {
        '$group': {
            '_id': {
                'yrMonth': '$tranMonth',
                'action': '$action'
            },
            'amt': {
                '$sum': '$amount'
            }
        }
    }, {
        '$sort': {
            '_id.yrMonth': -1,
            '_id.action': 1
        }
    }
]

class FinancialRatio:

    def __init__(self):
        self._db = dbutil.get_client()

    def find_ratios(self, ticker):
        ratio_cur = self._db[coll_ratio].find({'symbol': ticker}).sort('date', -1)
        result = [ratio for ratio in ratio_cur]
        return result

    def find_growth(self, ticker):
        growth_cur = self._db[coll_growth].find({'symbol': ticker}).sort('date', -1)
        result = [fingrowth for fingrowth in growth_cur]
        return result

    def save_trans(self, tran_list):
        result = self._db[coll_trans].insert_many(tran_list)
        return result

    def clear_trans(self):
        result = self._db[coll_trans].delete_many({})
        return result

    def aggr_income(self):
        cur = self._db[coll_trans].aggregate(aggr_income)
        return [tran for tran in cur]