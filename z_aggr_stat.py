from pkg.repo import aggrstat as aggr

aggr_db = aggr.AggregateStatDB()

result = aggr_db.calc_scan_params()
print(result)
