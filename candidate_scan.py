import datetime

from pkg.repo import stockdata as sd
from pkg.repo import stockstat as ss
from pkg.repo import aggrstat as aggr
from pkg.repo import fininfo as fr
from pkg.repo import stockavg as ad
from pkg.common import fin_attributes as attr
"""
Candidate scan attempts to 'pattern match' successful stock performance by evaluating
ratio of current price vs. average balance at 4 data points -- 10, 20, 50 and 200 day averages.
Successful pattern is determined by retrieving ratios for stocks which increase more than 10% in
a 4 week period (determined by aggregate_4wk_stats.py and summarized by aggr_stat_param.py).
"""
aggr_db = aggr.AggregateStatDB()
avgBalLevels = aggr_db.find_aggr_newest()
fr_db = fr.FinancialRatio()
avg_db = ad.StockAveragePriceDB()


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


def find_fin_growth(ticker_symbol):
    fin_growth = fr_db.find_growth(ticker_symbol)
    if len(fin_growth) > 0:
        return fin_growth[0]
    else:
        return None


plus_minus_offset = .03
avg_dly10_plus, avg_dly10_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs10'], plus_minus_offset)
avg_dly20_plus, avg_dly20_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs20'], plus_minus_offset)
avg_dly50_plus, avg_dly50_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs50'], plus_minus_offset)
avg_dly200_plus, avg_dly200_minus = calc_plus_minus(avgBalLevels['avgDlyPriceVs200'], plus_minus_offset)


#fin_ratio_names = ['currentRatio', 'quickRatio', 'netProfitMargin', 'debtEquityRatio', 'returnOnEquity',
#                   'returnOnAssets', 'cashFlowToDebtRatio', 'freeCashFlowPerShare', 'operatingCashFlowSalesRatio',
#                   'operatingProfitMargin', 'debtEquityRatio', 'priceSalesRatio', 'priceCashFlowRatio']
#fin_growth_names = ['revenueGrowth', 'netIncomeGrowth', 'epsgrowth', 'grossProfitGrowth', 'operatingIncomeGrowth',
#                    'threeYRevenueGrowthPerShare', 'threeYOperatingCFGrowthPerShare', 'threeYNetIncomeGrowthPerShare',
#                    'operatingCashFlowGrowth', 'operatingCashFlowGrowth', 'freeCashFlowGrowth', 'assetGrowth']

ss_db = ss.StatisticsDB()
sdb = sd.StocksDB()

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
by the aggregate_stats.py program.
"""
for ticker in tickers:
    ticker_idx += 1
    print('Processing ', str(ticker_idx), 'of', str(ticker_size), ':', ticker['_id'], '(weekly=', ticker['weeklyOptions'],')')
    price_chg_stats = ss_db.find_stat_by_ticker(ticker['_id'])
    for stat in price_chg_stats:
        if stat['statisticType'] in full_stat_types:
            if stat['priceId'] in candidate_stats:
                candidate_stat = candidate_stats[stat['priceId']]
            else:
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


def find_ticker(ticker_symbol):
    ticker_match = {}
    for ticker in tickers:
        if ticker['_id'] == ticker_symbol:
            ticker_match = ticker
            break
    return ticker_match


candidate_stat_list = []
create_date = datetime.datetime.now(datetime.UTC)

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
        ticker = find_ticker(cs['tickerSymbol'])
        cs['weeklyOptions'] = ticker['weeklyOptions']
        candidate_stat_list.append(cs)
        avg_entry = avg_db.find_by_price_id(cs['priceId'])
        for avg_item in avg_entry['avgList']:
            if avg_item['daysCnt'] == 50:
                cs['avgVolume'] = avg_item['avgVolume']
                break
        fr = attr.retrieve_fin_ratio(cs['tickerSymbol'])
        if fr is not None:
            cs.update(fr)
        fg = attr.retrieve_fin_growth(cs['tickerSymbol'])
        if fg is not None:
            cs.update(fg)
        cs['createDate'] = create_date

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
