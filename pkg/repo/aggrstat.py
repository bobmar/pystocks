from pkg.repo import dbutil
from pkg.repo.stockstat import coll_stat_type, coll_stat_candidate

coll_aggr_stat = 'aggrStatistics'
coll_aggr_param = 'aggrStatParam'
coll_stat_candidate = 'statCandidate'

aggr_pipeline = [
    {
        '$match': {
            'curr_price_chg': {
                '$gte': 0.0
            }
        }
    }, {
        '$group': {
            '_id': None,
            'avgStdDev2Week': {
                '$avg': '$STDDEV2WK'
            },
            'avgStdDev10Week': {
                '$avg': '$STDDEV10WK'
            },
            'avgUpDownVol': {
                '$avg': '$UPDNVOL50'
            },
            'avgDlyPriceVs10': {
                '$avg': '$DYPRCV10A'
            },
            'avgDlyPriceVs20': {
                '$avg': '$DYPRCV20A'
            },
            'avgDlyPriceVs50': {
                '$avg': '$DYPRCV50A'
            },
            'avgDlyPriceVs200': {
                '$avg': '$DYPRCV200A'
            },
            'avgDlyPriceVs10e': {
                '$avg': '$DYPRCV10E'
            },
            'avgDlyPriceVs20e': {
                '$avg': '$DYPRCV20E'
            },
            'avgDlyPriceVs50e': {
                '$avg': '$DYPRCV50E'
            },
            'avgDlyPriceVs200e': {
                '$avg': '$DYPRCV200E'
            }
        }
    }
]

candidate_pipeline = [
    {
        '$match': {
            'UPDNVOL50': {
                '$gt': 1.0
            },
            'currentRatio': {
                '$gt': 1.0
            },
            'quickRatio': {
                '$gt': 1.0
            },
            '$expr': {
                '$ne': [
                    '$currentRatio', '$quickRatio'
                ]
            },
            'epsgrowth': {
                '$gt': 0
            },
            'grossProfitGrowth': {
                '$gt': 0
            },
            'netProfitMargin': {
                '$gt': 0
            },
            'netIncomeGrowth': {
                '$gt': 0
            },
            'cashFlowToDebtRatio': {
                '$gt': 0
            },
            'returnOnEquity': {
                '$gt': 0
            },
            'revenueGrowth': {
                '$gt': 0
            },
            'operatingProfitMargin': {
                '$gt': 0
            },
            'operatingIncomeGrowth': {
                '$gt': 0
            },
            'freeCashFlowPerShare': {
                '$gt': 0
            },
            'avgVolume': {
                '$gt': 400000
            }
        }
    }, {
        '$group': {
            '_id': '$tickerSymbol',
            'count': {
                '$sum': 1
            },
            'updn': {
                '$avg': '$UPDNVOL50'
            },
            'updnmed': {
                '$median': {
                    'input': '$UPDNVOL50',
                    'method': 'approximate'
                }
            },
            'updnmin': {
                '$min': '$UPDNVOL50'
            },
            'updnmax': {
                '$max': '$UPDNVOL50'
            },
            'avgdyvolv20a': {
                '$avg': '$DYVOLV20A'
            },
            'avgdyvolv50a': {
                '$avg': '$DYVOLV50A'
            },
            'avgdyvolv200a': {
                '$avg': '$DYVOLV200A'
            },
            'avgcurrRatio': {
                '$avg': '$currentRatio'
            },
            'avgquickRatio': {
                '$avg': '$quickRatio'
            },
            'avgroe': {
                '$avg': '$returnOnEquity'
            },
            'avgnetProfitMargin': {
                '$avg': '$netProfitMargin'
            },
            'avgnetIncomeGrowth': {
                '$avg': '$netIncomeGrowth'
            },
            'avgepsgrowth': {
                '$avg': '$epsgrowth'
            },
            'avgoperatingProfitMargin': {
                '$avg': '$operatingProfitMargin'
            },
            'avgoperatingIncomeGrowth': {
                '$avg': '$operatingIncomeGrowth'
            },
            'avgrevenueGrowth': {
                '$avg': '$revenueGrowth'
            },
            'avggrossProfitGrowth': {
                '$avg': '$grossProfitGrowth'
            },
            'avgcashFlowToDebtRatio': {
                '$avg': '$cashFlowToDebtRatio'
            },
            'avgfreeCashFlowPerShare': {
                '$avg': '$freeCashFlowPerShare'
            },
            'avgthreeYRevenueGrowthPerShare': {
                '$avg': '$threeYRevenueGrowthPerShare'
            },
            'begin': {
                '$min': '$priceDate'
            },
            'end': {
                '$max': '$priceDate'
            },
            'weeklyopt': {
                '$first': '$weeklyOptions'
            }
        }
    }, {
        '$match': {
            'count': {
                '$gt': 1
            },
            '$expr': {
                '$gt': [
                    '$end', {
                        '$dateSubtract': {
                            'startDate': '$$NOW',
                            'unit': 'year',
                            'amount': 1
                        }
                    }
                ]
            }
        }
    }, {
        '$project': {
            '_id': 1,
            'count': 1,
            'updn': {
                '$round': [
                    '$updn', 4
                ]
            },
            'updnmed': {
                '$round': [
                    '$updnmed', 4
                ]
            },
            'updnmin': {
                '$round': [
                    '$updnmin', 4
                ]
            },
            'updnmax': {
                '$round': [
                    '$updnmax', 4
                ]
            },
            'dyvolv20a': {
                '$round': [
                    '$avgdyvolv20a', 4
                ]
            },
            'dyvolv50a': {
                '$round': [
                    '$avgdyvolv50a', 4
                ]
            },
            'dyvolv200a': {
                '$round': [
                    '$avgdyvolv200a', 4
                ]
            },
            'currRatio': {
                '$round': [
                    '$avgcurrRatio', 4
                ]
            },
            'quickRatio': {
                '$round': [
                    '$avgquickRatio', 4
                ]
            },
            'roe': {
                '$round': [
                    '$avgroe', 4
                ]
            },
            'netProfitMargin': {
                '$round': [
                    '$avgnetProfitMargin', 4
                ]
            },
            'netIncomeGrowth': {
                '$round': [
                    '$avgnetIncomeGrowth', 4
                ]
            },
            'epsgrowth': {
                '$round': [
                    '$avgepsgrowth', 4
                ]
            },
            'operatingProfitMargin': {
                '$round': [
                    '$avgoperatingProfitMargin', 4
                ]
            },
            'operatingIncomeGrowth': {
                '$round': [
                    '$avgoperatingIncomeGrowth', 4
                ]
            },
            'revenueGrowth': {
                '$round': [
                    '$avgrevenueGrowth', 4
                ]
            },
            'grossProfitGrowth': {
                '$round': [
                    '$avggrossProfitGrowth', 4
                ]
            },
            'cashFlowToDebtRatio': {
                '$round': [
                    '$avgcashFlowToDebtRatio', 4
                ]
            },
            'freeCashFlowPerShare': {
                '$round': [
                    '$avgfreeCashFlowPerShare', 4
                ]
            },
            'threeYRevenueGrowthPerShare': {
                '$round': [
                    '$avgthreeYRevenueGrowthPerShare', 4
                ]
            },
            'begin': 1,
            'end': 1,
            'weeklyopt': 1
        }
    }, {
        '$sort': {
            'end': -1,
            'netProfitMargin': -1,
            'count': -1
        }
    }
]

class AggregateStatDB:

    def __init__(self):
        self._db = dbutil.get_client()

    def calc_scan_params(self):
        aggr_col = self._db[coll_aggr_stat]
        aggr_result = aggr_col.aggregate(aggr_pipeline)
        return [aggr_item for aggr_item in aggr_result]

    def save_aggr_stats(self, aggr_stat_list):
        return self._db[coll_aggr_stat].insert_many(aggr_stat_list)

    def drop_aggr_stats(self):
        return self._db[coll_aggr_stat].drop()

    def save_aggr_param(self, param):
        aggr_result = self._db[coll_aggr_param].insert_one(param)
        return aggr_result

    def find_aggr_newest(self):
        aggr_result = self._db[coll_aggr_param].find_one(sort=[('_id',-1)])
        return aggr_result

    def calc_stat_candidates(self):
        candidate_col = self._db[coll_stat_candidate]
        candidate_result = candidate_col.aggregate(candidate_pipeline)
        return [candidate_item for candidate_item in candidate_result]
