import datetime
import time
from pkg.repo import stockdata as sd
from pkg.repo import ibdstat as ibd
from pkg.repo import aggrstat as aggr
"""
Look for stock trades which have increased by a certain percentage in the last 4 weeks.
Collect statistics from 4 weeks ago for further analysis.
"""
stat_names = ['STDDEV2WK', 'STDDEV10WK', 'UPDNVOL50', 'DYPRCV50A', 'DYPRCV10A', 'DYPRCV200A', 'ZSCORE', 'TRMOM', 'DYPRCV20A', 'DYVOLV20A', 'DYVOLV50A', 'DYVOLV200A']
ibd_stat_names = ['compositeRating', 'epsRating', 'relativeStrength', 'groupStrength', 'accumDist', 'salesMarginRoe', 'mgmtOwnPct', 'mgmtOwnPct']
ibd_db = ibd.IbdStatisticDB()
sdb = sd.StocksDB()
aggr_db = aggr.AggregateStatDB()

price_date_offset = (4*5)-1
pct_increase1 = 12
pct_increase2 = 20

hdr = ['curr_price_date', 'curr_four_wk_chg', 'four_wk_price_date']
for stat_fld in ibd_stat_names:
    hdr.append(stat_fld)
hdr.append('ibdListCount')
for stat_name in stat_names:
    hdr.append(stat_name)


def find_stats(ticker_symbol, stat_price_id):
    prices = sdb.find_price_by_ticker(ticker_symbol)
    price_idx = 0
    curr_price_idx = 0
    for price in prices:
        if price['priceId'] == stat_price_id:
            curr_price_idx = price_idx
            break
        else:
            price_idx += 1
    four_wk_price_id = prices[curr_price_idx + price_date_offset]['priceId']
    four_wk_ibd = ibd_db.find_stat_by_price_id(four_wk_price_id)
    return four_wk_ibd


def find_hist_price(p_price_list, price_id, period_cnt):
    price_idx = 0
    period_price_id = None
    period_price_date = None
    for price in p_price_list:
        if price['_id'] == price_id:
            break
        else:
            price_idx += 1
    if price_idx > period_cnt:
        period_price_id = p_price_list[price_idx - period_cnt]['_id']
        period_price_date = p_price_list[price_idx - period_cnt]['priceDate']
    else:
        print('Unable to find historical price_id for', price_id, period_cnt)
    return period_price_id, period_price_date


aggr_records = []

price_list = []
candidate_stats = []
tickers = sdb.ticker_symbol_list(800)
print('Found ', len(tickers), ' tickers')
price_chg_stats = []
four_wk_stats = []
for ticker in tickers:
    price_chg_stats = sdb.find_stat_by_ticker_and_type(ticker, 'PCTCHG4WK')
    print('Found ', len(price_chg_stats), ' price change stats for ', ticker)
    for stat in price_chg_stats:
        if pct_increase1 <= stat['statisticValue'] < pct_increase2:
            candidate_stats.append(stat)
stat_cnt = len(candidate_stats)
print('Found ', stat_cnt, ' stats')
stat_handled_cnt = 0
for stat in candidate_stats:
    aggr_dict = {}
    print('Find current prices for ', stat['tickerSymbol'])
    price_list = sdb.find_price_by_ticker(stat['tickerSymbol'])
    print('Find historical prices for ', stat['priceId'])
    price_id, price_date = find_hist_price(price_list, stat['priceId'], price_date_offset+1)
    print('Find statistics for ', price_id)
    stat_list = sdb.find_stat_by_price_id(price_id)
    print('Find IBD statistics for ', price_id)
    ibd_stats = ibd_db.find_stat_by_price_id(price_id)
    aggr_dict['_id'] = stat['priceId']
    aggr_dict['curr_four_wk_chg'] = stat['statisticValue']
    if price_date is not None:
        aggr_dict['four_wk_price_date'] = price_date.strftime('%Y-%m-%d')
    else:
        aggr_dict['four_wk_price_date'] = price_id

    if len(ibd_stats) > 0:
        for sr in ibd_stat_names:
            if sr in ibd_stats[0]:
                aggr_dict[sr] = ibd_stats[0][sr]
        aggr_dict['listCnt'] = len(ibd_stats[0]['listName'])
    print('Aggregate record', aggr_dict)
    stat_dict = {}
    for stat_item in stat_list:
        stat_dict[stat_item['statisticType']] = stat_item

    if len(stat_list) > 0:
        for stat_name in stat_names:
            if stat_name in stat_dict:
                aggr_dict[stat_name] = stat_dict[stat_name]['statisticValue']
    if len(ibd_stats) > 0:
        aggr_dict['createDate'] = datetime.datetime.now(datetime.UTC)
        aggr_records.append(aggr_dict)
    stat_handled_cnt += 1
    print(stat_handled_cnt, '/', stat_cnt)

print('Found ', len(aggr_records), ' aggregate records')
print('Drop existing aggregate statistics')
aggr_db.drop_aggr_stats()
print('Save aggregate statistics')
x = aggr_db.save_aggr_stats(aggr_records)
print('Aggregate records added: ', len(x.inserted_ids))
