from pkg.repo import aggrstat as aggr
import datetime as dt

aggr_db = aggr.AggregateStatDB()

result = aggr_db.calc_scan_params()
if len(result) > 0:
    aggr_param = result[0]
    create_date = dt.datetime.now(dt.UTC)
    aggr_param['createDate'] = create_date
    del aggr_param['_id']
    aggr_db.save_aggr_param(aggr_param)

print(result[0])
