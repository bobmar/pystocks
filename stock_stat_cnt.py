from pkg.repo import stockdata as sd
from pkg.service import statanalysissvc as sa
from pkg.repo import fininfo as fr

sa_svc = sa.StatisticAnalysisSvc()
sdb = sd.StocksDB()
fr_db = fr.FinancialRatio()
ticker_list = sorted(sdb.ticker_symbol_list(800))
signal_cnt_dict = {}


def accum_signal(signal_list):
    for sig in signal_list:
        sig_cnt = signal_cnt_dict.get(sig["signalType"])
        if sig_cnt is None:
            signal_cnt_dict[sig["signalType"]] = 1
        else:
            signal_cnt_dict[sig["signalType"]] = sig_cnt + 1


def sort_dict(dict_to_sort):
    sorted_list = sorted(dict_to_sort.items(), key=lambda x: x[1], reverse=True)
    sorted_dict = dict(sorted_list)
    return sorted_dict


def format_number(float_val):
    if float_val >= 0:
        return '{0:.2f}'.format(float_val)
    else:
        return '(' + '{0:.2f}'.format(float_val) + ')'


def process_stat(curr, four_wk, aggr_stat_curr, aggr_stat, fin_growth_list):
    if len(fin_growth_list) > 0:
        fin_growth = fin_growth_list[0]
    else:
        fin_growth = {}
    print(curr, four_wk, "C:" + aggr_stat["ibdStat"]["compositeRating"],
          "RS:" + aggr_stat["ibdStat"]["relativeStrength"],
          "AD:" + aggr_stat["ibdStat"]["accumDist"],
          "G:" + aggr_stat["ibdStat"]["groupStrength"],
          "->",
          format_number(aggr_stat_curr["stat"]["PCTCHG4WK"]),
          "<-",
          'RevGrowth:' + format_number(fin_growth['revenueGrowth']),
          'NetIncGrowth:' + format_number(fin_growth['netIncomeGrowth']),
          'EPSGrowth:' + format_number(fin_growth['epsgrowth']),
          "Lists:", aggr_stat["ibdStat"]["listName"])


top_return_cnt = 0
top_loss_cnt = 0
for ticker_symbol in ticker_list:
    signals = ''
    stat_obj = sa_svc.retrieve_stats(ticker_symbol)
    stat_aggr = sa.aggregate_stats_by_price_id(stat_obj)
    fin_growth = fr_db.find_growth(ticker_symbol)
    stat_keys = list(stat_aggr)
    curr_idx = 0
    curr_key = 'No key'
    if len(stat_keys) > 20:
        curr_key = stat_keys[0]
        four_wk_key = stat_keys[curr_idx + 19]
        four_wk_aggr = stat_aggr[four_wk_key]
        curr_aggr = stat_aggr[curr_key]
        try:
            if curr_aggr["stat"]["PCTCHG4WK"] < -10:
                top_loss_cnt += 1
                process_stat(curr_key, four_wk_key, curr_aggr, four_wk_aggr, fin_growth)
            if curr_aggr["stat"]["PCTCHG4WK"] > 20:
                top_return_cnt += 1
                process_stat(curr_key, four_wk_key, curr_aggr, four_wk_aggr, fin_growth)
        except KeyError:
            print("KeyError for ", curr_key)
    else:
        continue

print("Total top stats: ", top_return_cnt)
print("Bottom return stats: ", top_loss_cnt)
