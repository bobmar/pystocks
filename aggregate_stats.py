import json
import datetime
from pkg.repo import stockdata as sd
from pkg.repo import aggrstat as aggr
"""
MongoDB clients
"""
sdb = sd.StocksDB()
aggr_db = aggr.AggregateStatDB()

num_weeks = 4
periods_per_week = 5
price_date_offset = (num_weeks*periods_per_week)-1
pct_increase1 = 12
pct_increase2 = 20
ibd_stat_names = ['compositeRating', 'epsRating', 'relativeStrength', 'groupStrength', 'accumDist', 'salesMarginRoe', 'mgmtOwnPct', 'mgmtOwnPct']
stat_names = ['STDDEV2WK', 'STDDEV10WK', 'UPDNVOL50', 'DYPRCV50A', 'DYPRCV10A', 'DYPRCV200A', 'ZSCORE', 'TRMOM', 'DYPRCV20A', 'DYVOLV20A', 'DYVOLV50A', 'DYVOLV200A']


def retrieve_tickers():
    tickers = sdb.ticker_symbol_list(800)
    print('Found ', len(tickers), ' tickers')
    return tickers


def retrieve_candidate_stats(ticker_list, stat_type):
    candidate_stats = []
    for ticker in ticker_list:
        price_chg_stats = sdb.find_stat_by_ticker_and_type(ticker, stat_type)
        print('Found ', len(price_chg_stats), ' price change stats for ', ticker)
        for stat in price_chg_stats:
            if pct_increase1 <= stat['statisticValue'] < pct_increase2:
                candidate_stats.append(stat)
    stat_cnt = len(candidate_stats)
    print('Found ', stat_cnt, ' stats')
    return candidate_stats


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


def aggregate_stats(candidate_stats):
    stat_cnt = len(candidate_stats)
    print('Found ', stat_cnt, ' stats')
    aggr_records = []
    stat_handled_cnt = 0
    for stat in candidate_stats:
        aggr_dict = {}
        print('Find current prices for ', stat['tickerSymbol'])
        price_list = sdb.find_price_by_ticker(stat['tickerSymbol'])
        print('Find historical prices for ', stat['priceId'])
        price_id, price_date = find_hist_price(price_list, stat['priceId'], price_date_offset + 1)
        print('Find statistics for ', price_id)
        stat_list = sdb.find_stat_by_price_id(price_id)
        aggr_dict['_id'] = stat['priceId']
        aggr_dict['curr_four_wk_chg'] = stat['statisticValue']
        if price_date is not None:
            aggr_dict['four_wk_price_date'] = price_date.strftime('%Y-%m-%d')
        else:
            aggr_dict['four_wk_price_date'] = price_id

        stat_dict = {}
        for stat_item in stat_list:
            stat_dict[stat_item['statisticType']] = stat_item

        if len(stat_list) > 0:
            for stat_name in stat_names:
                if stat_name in stat_dict:
                    aggr_dict[stat_name] = stat_dict[stat_name]['statisticValue']
        aggr_dict['createDate'] = datetime.datetime.now(datetime.UTC)
        aggr_records.append(aggr_dict)
        stat_handled_cnt += 1
        print(stat_handled_cnt, '/', stat_cnt)
    return aggr_records


def replace_aggregate_stats_coll(aggr_records):
    print('Drop existing aggregate statistics')
    aggr_db.drop_aggr_stats()
    print('Save aggregate statistics')
    x = aggr_db.save_aggr_stats(aggr_records)
    print('Aggregate records added: ', len(x.inserted_ids))


def calc_scan_parameters():
    result = aggr_db.calc_scan_params()
    if len(result) > 0:
        aggr_param = result[0]
        create_date = datetime.datetime.now(datetime.UTC)
        aggr_param['createDate'] = create_date
        del aggr_param['_id']
        aggr_db.save_aggr_param(aggr_param)
    return result


stat_type = {
    "4": "PCTCHG4WK",
    "8": "PCTCHG8WK",
    "12": "PCTCHG12WK",
}

tickers = retrieve_tickers()
candidates = retrieve_candidate_stats(tickers, stat_type["4"])
aggr_records = aggregate_stats(candidates)
replace_aggregate_stats_coll(aggr_records)
scan_params = calc_scan_parameters()
if len(scan_params) > 0:
    aggr_param = scan_params[0]
    create_date = datetime.datetime.now(datetime.UTC)
    aggr_param['createDate'] = create_date
    del aggr_param['_id']
    aggr_db.save_aggr_param(aggr_param)

del scan_params[0]['createDate']
del scan_params[0]['_id']
print(json.dumps(scan_params[0],indent=4))