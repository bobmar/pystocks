from pkg.repo import stockdata as sd
from pkg.repo import ibdstat as ibd
from pkg.repo import stockstat as ss

avgBalLevels = {
    'avgDlyPriceVs20': 1.032634834219484,
    'avgDlyPriceVs50': 1.0559530184524843,
    'avgDlyPriceVs200': 1.188171317154193
}


def calc_plus_minus(input_value, offset):
    plus_value = input_value + offset
    minus_value = input_value - offset
    return plus_value, minus_value


avg_dly20_plus, avg_dly20_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs20'], .03)
avg_dly50_plus, avg_dly50_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs50'], .03)
avg_dly200_plus, avg_dly200_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs200'], .03)

ibd_stat_names = ['compositeRating', 'epsRating', 'relativeStrength', 'groupStrength', 'accumDist', 'salesMarginRoe', 'mgmtOwnPct', 'mgmtOwnPct']
ss_db = ss.StatisticsDB()
sdb = sd.StocksDB()
ibd_db = ibd.IbdStatisticDB()

ticker_idx = 0
candidate_stats = {}
tickers = sdb.ticker_list(800)
ticker_size = len(tickers)
price_chg_stats = []
four_wk_stats = []
"""
Retrieve a list of candidate tickers which have average balance ratios within the desired range which was determined
by the aggreage_4wk_stats.py program.
"""
for ticker in tickers:
    ticker_idx += 1
    print('Processing ', str(ticker_idx), 'of', str(ticker_size), ':', ticker['_id'], '(weekly=', ticker['weeklyOptions'],')')
    price_chg_stats = ss_db.find_stat_by_ticker(ticker['_id'])
    for stat in price_chg_stats:
        if stat['statisticType'] in '|DYPRCV20A|DYPRCV50A|DYPRCV200A|STDDEV2WK|STDDEV10WK|UPDNVOL50|DYVOLV20A|DYVOLV50A|DYVOLV200A|':
            try:
                candidate_stat = candidate_stats[stat['priceId']]
            except KeyError:
                candidate_stat = {'tickerSymbol': stat['tickerSymbol'], 'priceId': stat['priceId'], 'priceDate': stat['priceDate']}
                candidate_stats[stat['priceId']] = candidate_stat
            if stat['statisticType'] in '|STDDEV2WK|STDDEV10WK|UPDNVOL50|DYVOLV20A|DYVOLV50A|DYVOLV200A|':
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
        if key in '|DYPRCV20A|DYPRCV50A|DYPRCV200A|':
            keyCnt += 1
    if keyCnt == 3:
        find_ibd_stat(cs)
        if cs['foundIbd']:
            ticker = find_ticker(cs['tickerSymbol'])
            cs['weeklyOptions'] = ticker['weeklyOptions']
            candidate_stat_list.append(cs)

saved_cnt = 0
if len(candidate_stat_list) > 0:
    print('Uploading...')
    ss_db.drop_candidate_stat()
    saved_cnt = ss_db.save_candidate_stat(candidate_stat_list)
print('Saved ', len(saved_cnt.inserted_ids), ' candidates')
