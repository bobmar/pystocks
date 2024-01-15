from pkg.repo import yahoostat as ys
from pkg.repo import stockstat as ss
from pkg.repo import ibdstat as ibd
from datetime import datetime
import json

yahoo_db = ys.YahooStatDB()
stat_db = ss.StatisticsDB()
ibd_db = ibd.IbdStatisticDB()

price_date = datetime(2022, 5, 13, 7, 0, 0, 0)

yahoo_dict = {}
yahoo_stats = yahoo_db.find_by_price_date(price_date)

for stat in yahoo_stats:
    stat_key = stat['_id'].replace('|', ':')
    yahoo_dict[stat_key] = stat

type_list = ['PCTCHG4WK', 'PCTCHG8WK', 'PCTCHG12WK', 'DYVOLV20A', 'DYVOLV50A', 'DYVOLV200A', 'ZSCORE', 'TRMOM', 'DYPRCV50A']
for stat_type in type_list:
    stat_list = stat_db.find_stat_by_type_and_date(stat_type, price_date, 500)
    for stat in stat_list:
        try:
            yahoo_dict[stat['priceId']][stat['statisticType']] = stat['statisticValue']
        except KeyError:
            print('Key ' + stat['priceId'] + ' does not exist')

ibd_stat_list = ibd_db.find_stat_by_price_date(price_date)
for stat in ibd_stat_list:
    try:
        yahoo_stat = yahoo_dict[stat['_id']]
        yahoo_stat['compositeRating'] = stat['compositeRating']
        yahoo_stat['epsRating'] = stat['epsRating']
        yahoo_stat['relativeStrength'] = stat['relativeStrength']
        yahoo_stat['groupStrength'] = stat['groupStrength']
        yahoo_stat['accumDist'] = stat['accumDist']
        yahoo_stat['mgmtOwnPct'] = stat['mgmtOwnPct']
    except KeyError:
        print('KeyError: ' + stat['_id'])


def custom_cvt(o):
    if isinstance(o, datetime):
        return o.__str__


yahoo_json = open('yahoo.json', 'w')
yahoo_json.write(json.dumps(yahoo_dict, indent=4, default=custom_cvt))
yahoo_json.close()

yahoo_columns = ('tickerSymbol'
                  , 'enterpriseValue'
                  , 'profitMargins'
                  , 'floatShares'
                  , 'sharesOutstanding'
                  , 'shortRatio'
                  , 'beta'
                  , 'priceToBook'
                  , 'earningsQuarterlyGrowth'
                  , 'netIncomeToCommon'
                  , 'trailingEps'
                  , 'forwardEps'
                  , 'pegRatio'
                  , 'enterpriseToRevenue'
                  , 'enterpriseToEbitda'
                  , 'targetHighPrice'
                  , 'targetLowPrice'
                  , 'targetMeanPrice'
                  , 'targetMedianPrice'
                  , 'totalCashPerShare'
                  , 'debtToEquity'
                  , 'DYPRCV50A'
                  , 'TRMOM'
                  , 'ZSCORE'
                  , 'DYVOLV200A'
                  , 'DYVOLV50A'
                  , 'DYVOLV20A'
                  , 'PCTCHG4WK'
                  , 'PCTCHG8WK'
                  , 'PCTCHG12WK'
                 )

yahoo_file = open('yahoo_stat.csv', 'w')
tuple_str = ''

for col in yahoo_columns:
    tuple_str += col + ','

yahoo_file.write(tuple_str + '\n')
tuple_str = ''
for stat in yahoo_dict.values():
    yahoo_stat = (stat['tickerSymbol']
                  , str(stat.get('enterpriseValue'))
                  , str(stat.get('profitMargins'))
                  , str(stat.get('floatShares'))
                  , str(stat.get('sharesOutstanding'))
                  , str(stat.get('shortRatio'))
                  , str(stat.get('beta'))
                  , str(stat.get('priceToBook'))
                  , str(stat.get('earningsQuarterlyGrowth'))
                  , str(stat.get('netIncomeToCommon'))
                  , str(stat.get('trailingEps'))
                  , str(stat.get('forwardEps'))
                  , str(stat.get('pegRatio'))
                  , str(stat.get('enterpriseToRevenue'))
                  , str(stat.get('enterpriseToEbitda'))
                  , str(stat.get('targetHighPrice'))
                  , str(stat.get('targetLowPrice'))
                  , str(stat.get('targetMeanPrice'))
                  , str(stat.get('targetMedianPrice'))
                  , str(stat.get('totalCashPerShare'))
                  , str(stat.get('debtToEquity'))
                  , str(stat.get('DYPRCV50A'))
                  , str(stat.get('TRMOM'))
                  , str(stat.get('ZSCORE'))
                  , str(stat.get('DYVOLV200A'))
                  , str(stat.get('DYVOLV50A'))
                  , str(stat.get('DYVOLV20A'))
                  , str(stat.get('PCTCHG4WK'))
                  , str(stat.get('PCTCHG8WK'))
                  , str(stat.get('PCTCHG12WK'))
                  )

    for item in yahoo_stat:
        tuple_str += item + ','
    yahoo_file.write(tuple_str + '\n')
    tuple_str = ''

yahoo_file.close()
