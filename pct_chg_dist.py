from pkg.repo import stockstat as ss
from pkg.repo import stockdata as sd

stat_db = ss.StatisticsDB()
stock_db = sd.StocksDB()

stat_types = ['PCTCHG4WK', 'PCTCHG8WK', 'PCTCHG12WK']
stock_list = stock_db.ticker_symbol_list(600)
stat_counts = {
    'tier0': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': -30,
              'high': 0
              },
    'tier1': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 0,
              'high': 5
              },
    'tier2': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 5,
              'high': 15
              },
    'tier3': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 15,
              'high': 25
              },
    'tier4': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 25,
              'high': 35
              },
    'tier5': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 35,
              'high': 45
              },
    'tier6': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 45,
              'high': 55
              },
    'tier7': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 55,
              'high': 65
              },
    'tier8': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 65,
              'high': 75
              },
    'tier9': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 75,
              'high': 85
              },
    'tier10': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 85,
              'high': 95
              },
    'tier11': {stat_types[0]: 0,
              stat_types[1]: 0,
              stat_types[2]: 0,
              'low': 95,
              'high': 9999
              }
    }


def display_stats():
    for tier in stat_counts:
        stat_count = stat_counts[tier]
        print(tier,
              stat_types[0] + ':', stat_count[stat_types[0]],
              stat_types[1] + ':', stat_count[stat_types[1]],
              stat_types[2] + ':', stat_count[stat_types[2]],
              'Low:', stat_count['low'], 'High:', stat_count['high'])


def accumulate_stat(statistic):
    for tier in stat_counts:
        if stat_counts[tier]['low'] <= statistic['statisticValue'] < stat_counts[tier]['high']:
            stat_counts[tier][statistic['statisticType']] = stat_counts[tier][statistic['statisticType']] + 1
            break


for ticker in stock_list:
    print(ticker)
    for stat_type in stat_types:
        print(stat_type)
        for stat in stat_db.find_stat_by_ticker_and_type(ticker, stat_type):
            accumulate_stat(stat)

display_stats()