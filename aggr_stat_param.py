from pkg.repo import aggrstat as aggr
import datetime as dt

aggr_db = aggr.AggregateStatDB()

result = aggr_db.calc_stat_candidates()
if len(result) > 0:
    for item in result:
        print(item)
