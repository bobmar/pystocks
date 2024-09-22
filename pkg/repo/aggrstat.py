from pkg.repo import dbutil


coll_aggr_stat = 'aggrStatistics'
coll_aggr_param = 'aggrStatParam'

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
            'emaDlyPriceVs10': {
                '$avg': '$DYPRCV10E'
            },
            'emaDlyPriceVs20': {
                '$avg': '$DYPRCV20E'
            },
            'emaDlyPriceVs50': {
                '$avg': '$DYPRCV50E'
            },
            'emaDlyPriceVs200': {
                '$avg': '$DYPRCV200E'
            }
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