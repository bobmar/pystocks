from datetime import datetime
from pkg.repo import yahoostat as ys
from pkg.repo import stockavg as savg
from pkg.repo import stockstat as ss
import json

price_date = datetime(2022, 6, 24, 7, 0)

ys_db = ys.YahooStatDB()
savg_db = savg.StockAveragePriceDB()
sstat_db = ss.StatisticsDB()

share_stat_list = []

yahoo_stats = ys_db.find_by_price_date(price_date)
avg_dict = {}
avg_list = savg_db.find_by_price_date(price_date)
for avg_item in avg_list:
    avg_dict[avg_item['_id']] = avg_item

pct_chg_dict = {}
pct_chg_list = sstat_db.find_stat_by_type_and_date('PCTCHG4WK', price_date, 1000)
print('Found ' + str(len(pct_chg_list)) + ' stats in stock stat')
for pct_chg_item in pct_chg_list:
    pct_chg_dict[pct_chg_item['priceId']] = pct_chg_item


def custom_cvt(o):
    if isinstance(o, datetime):
        return o.__str__


def avg_balance(avg_bal, days_cnt):
    selected_ab = {}
    for ab in avg_bal['avgList']:
        if ab['daysCnt'] == days_cnt:
            selected_ab = ab
    return selected_ab


for yahoo_stat in yahoo_stats:
    try:
        share_stat = {'tickerSymbol': yahoo_stat['tickerSymbol'],
                      'sharesOutstanding': yahoo_stat['sharesOutstanding'],
                      'floatShares': yahoo_stat['floatShares'],
                      'sharesShort': yahoo_stat['sharesShort'],
                      'sharesShortPriorMonth': yahoo_stat['sharesShortPriorMonth']}
        avg_bal_item = avg_dict[yahoo_stat['_id'].replace('|', ':')]
        share_stat['stockAvg'] = avg_bal_item
        pct_chg_item = pct_chg_dict[yahoo_stat['_id'].replace('|', ':')]
        share_stat['pctChg4Wk'] = pct_chg_item['statisticValue']
        share_stat_list.append(share_stat)
    except KeyError:
        print('KeyError in processing yahoo_stat')

for share_stat in share_stat_list:
    try:
        avg_bal = avg_balance(share_stat['stockAvg'], 50)
        share_stat['avgVolVsFloat'] = avg_bal['avgVolume']/share_stat['floatShares']
    except KeyError:
        share_stat['avgVolVsFloat'] = -1
        continue

share_json = open('share_volume_analysis.json', 'w')
share_json.write(json.dumps(share_stat_list, indent=4, default=custom_cvt))
share_json.close()


def stat_key(obj):
    return obj['avgVolVsFloat']


share_stat_list.sort(key=stat_key)

for share_stat in share_stat_list:
    avgVolVsFloat = share_stat['avgVolVsFloat']
    print(share_stat['tickerSymbol'],
          '{0:.3f}'.format(avgVolVsFloat),
          '{0:.2f}'.format(share_stat['pctChg4Wk']))
