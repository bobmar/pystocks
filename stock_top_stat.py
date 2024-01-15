from pkg.repo import stockstat as ss
from pkg.repo import stockdata as sd
import json
import sys

stat_repo = ss.StatisticsDB()
stat_types = []
for stat_type in stat_repo.stat_type_list():
    try:
        if stat_type['showInDashboard']:
            stat_types.append(stat_type)
    except KeyError:
        continue
price_date_cnt = 10 if len(sys.argv) == 1 else int(sys.argv[1])

stock_repo = sd.StocksDB()
price_dates = stock_repo.find_price_dates(price_date_cnt)

stat_tally = {}

for date in price_dates:
    for stat_type in stat_types:
        stats = stat_repo.find_stat_by_type_and_date(stat_type['_id'], date, 10)
        stat_dict = {}
        try:
            stat_dict = stat_tally[stat_type['_id']]
        except KeyError:
            stat_dict = {}
            stat_tally[stat_type['_id']] = stat_dict
        stat_list = []
        for stat in stats:
            try:
                stat_list = stat_dict[stat['tickerSymbol']]
            except KeyError:
                stat_list = []
                stat_dict[stat['tickerSymbol']] = stat_list
            del stat['priceDate']
            stat_list.append(stat)

# print(json.dumps(stat_tally, indent=4))

ticker_dict = {}
ticker_list = []
sorted_stat_dict = {}
ticker_stat_list = []
for stat_type in stat_tally.keys():
    stat_dict = stat_tally[stat_type]
    sorted_stat_dict = dict(sorted(stat_dict.items(), key=lambda item: (-1*len(item[1]))))
    for s_ticker in sorted_stat_dict.keys():
        ticker_stat_list = sorted_stat_dict[s_ticker]
        if len(ticker_stat_list) >= (price_date_cnt * .8):
            try:
                ticker_list = ticker_dict[s_ticker]
            except KeyError:
                ticker_list = []
                ticker_dict[s_ticker] = ticker_list
            ticker_list.append(stat_type)
    sorted_ticker_dict = dict(sorted(ticker_dict.items(), key=lambda item: (-1*len(item[1]))))

for ticker in sorted_ticker_dict.keys():
    ticker_size = len(sorted_ticker_dict[ticker])
    if ticker_size > 2:
        print(ticker, ticker_size, sorted_ticker_dict[ticker])
