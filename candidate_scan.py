from pkg.repo import stockdata as sd
from pkg.repo import ibdstat as ibd
from pkg.repo import stockstat as ss
from pkg.repo import aggrstat as aggr
from pkg.repo import finratio as fr
"""
Candidate scan attempts to 'pattern match' successful stock performance by evaluating
ratio of current price vs. average balance at 4 data points -- 10, 20, 50 and 200 day averages.
Successful pattern is determined by retrieving ratios for stocks which increase more than 10% in
a 4 week period (determined by aggregate_4wk_stats.py and summarized by aggr_stat_param.py).
"""
aggr_db = aggr.AggregateStatDB()
avgBalLevels = aggr_db.find_aggr_newest()
fr_db = fr.FinancialRatio()


def calc_plus_minus(input_value, offset):
    plus_value = input_value + offset
    minus_value = input_value - offset
    return plus_value, minus_value


def find_fin_ratio(ticker_symbol):
    ratios = fr_db.find_ratios(ticker_symbol)
    if len(ratios) > 0:
        return ratios[0]
    else:
        return None


plus_minus_offset = .02
avg_dly10_plus, avg_dly10_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs10'], plus_minus_offset)
avg_dly20_plus, avg_dly20_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs20'], plus_minus_offset)
avg_dly50_plus, avg_dly50_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs50'], plus_minus_offset)
avg_dly200_plus, avg_dly200_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs200'], plus_minus_offset)

ibd_stat_names = ['compositeRating', 'epsRating', 'relativeStrength', 'groupStrength', 'accumDist', 'salesMarginRoe', 'mgmtOwnPct', 'mgmtOwnPct']
fin_ratio_names = ['currentRatio', 'quickRatio', 'returnOnAssets', 'returnOnEquity']
ss_db = ss.StatisticsDB()
sdb = sd.StocksDB()
ibd_db = ibd.IbdStatisticDB()

ticker_idx = 0
candidate_stats = {}
tickers = sdb.ticker_list(800)
ticker_size = len(tickers)
price_chg_stats = []
four_wk_stats = []
filter_stat_types = '|DYPRCV10A|DYPRCV20A|DYPRCV50A|DYPRCV200A|'
asis_stat_types = '|STDDEV2WK|STDDEV10WK|UPDNVOL50|DYVOLV10A|DYVOLV20A|DYVOLV50A|DYVOLV200A|'
full_stat_types = filter_stat_types + asis_stat_types

"""
Retrieve a list of candidate tickers which have average balance ratios within the desired range which was determined
by the aggreage_4wk_stats.py program.
"""
for ticker in tickers:
    ticker_idx += 1
    print('Processing ', str(ticker_idx), 'of', str(ticker_size), ':', ticker['_id'], '(weekly=', ticker['weeklyOptions'],')')
    price_chg_stats = ss_db.find_stat_by_ticker(ticker['_id'])
    for stat in price_chg_stats:
        if stat['statisticType'] in full_stat_types:
            try:
                candidate_stat = candidate_stats[stat['priceId']]
            except KeyError:
                candidate_stat = {'tickerSymbol': stat['tickerSymbol'], 'priceId': stat['priceId'], 'priceDate': stat['priceDate']}
                candidate_stats[stat['priceId']] = candidate_stat
            if stat['statisticType'] in asis_stat_types:
                candidate_stat[stat['statisticType']] = stat['statisticValue']

            if stat['statisticType'] == 'DYPRCV10A':
                if avg_dly10_plus >= stat['statisticValue'] > avg_dly10_minus:
                    candidate_stat[stat['statisticType']] = stat['statisticValue']
            if stat['statisticType'] == 'DYPRCV20A':
                if avg_dly20_plus >= stat['statisticValue'] > avg_dly20_minus:
                    candidate_stat[stat['statisticType']] = stat['statisticValue']
            if stat['statisticType'] == 'DYPRCV50A':
                if avg_dly50_plus >= stat['statisticValue'] > avg_dly50_minus:
                    candidate_stat[stat['statisticType']] = stat['statisticValue']
            if stat['statisticType'] == 'DYPRCV200A':
                if avg_dly200_plus > stat['statisticValue'] > avg_dly200_minus:
                    candidate_stat[stat['statisticType']] = stat['statisticValue']


def find_ibd_stat(candidate_stat):
    ibd_stat = ibd_db.find_stat_by_price_id(candidate_stat['priceId'])
    if len(ibd_stat) > 0:
        candidate_stat['listCnt'] = len(ibd_stat[0]['listName'])
        for ibd_stat_name in ibd_stat_names:
            candidate_stat[ibd_stat_name] = ibd_stat[0][ibd_stat_name]
        candidate_stat['foundIbd'] = True
    else:
        candidate_stat['foundIbd'] = False


def find_ticker(ticker_symbol):
    ticker_match = {}
    for ticker in tickers:
        if ticker['_id'] == ticker_symbol:
            ticker_match = ticker
            break
    return ticker_match


candidate_stat_list = []

"""
For the candidates having the desired ratios, retrieve IBD statistics and weekly option indicator.
"""
print('Finding candidates')
for stat in candidate_stats.keys():
    cs = candidate_stats[stat]
    keyCnt = 0
    for key in cs.keys():
        if key in '|DYPRCV10A|DYPRCV20A|DYPRCV50A|DYPRCV200A|':
            keyCnt += 1
    if keyCnt == 4:
        find_ibd_stat(cs)
        if cs['foundIbd']:
            ticker = find_ticker(cs['tickerSymbol'])
            cs['weeklyOptions'] = ticker['weeklyOptions']
            candidate_stat_list.append(cs)
        fr = find_fin_ratio(cs['tickerSymbol'])
        if fr is not None:
            for fr_name in fin_ratio_names:
                cs[fr_name] = fr[fr_name]

saved_cnt = 0
if len(candidate_stat_list) > 0:
    print('Uploading...')
    ss_db.drop_candidate_stat()
    saved_cnt = ss_db.save_candidate_stat(candidate_stat_list)
print('Saved ', len(saved_cnt.inserted_ids), ' candidates')
print('Average balance ratios')
print('avgDlyPriceVs10:', avgBalLevels['avgDlyPriceVs10'])
print('avgDlyPriceVs20:', avgBalLevels['avgDlyPriceVs20'])
print('avgDlyPriceVs50:', avgBalLevels['avgDlyPriceVs50'])
print('avgDlyPriceVs200:', avgBalLevels['avgDlyPriceVs200'])
print('Plus/Minus offset:', plus_minus_offset)
