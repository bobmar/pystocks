from pkg.repo import stocksignal as sig
from pkg.repo import stockstat as stat
from pkg.repo import ibdstat as ibd


def aggregate_stats_by_price_id(stat_obj):
    ibd_list = stat_obj["ibd_list"]
    signal_dict = stat_obj["signal_dict"]
    stat_dict = stat_obj["stat_dict"]
    stat_aggr = {}
    for price_id in stat_dict.keys():
        if price_id in stat_aggr:
            stat_aggr_item = stat_aggr[price_id]
        else:
            stat_aggr_item = {}
            stat_aggr[price_id] = stat_aggr_item
        if price_id in signal_dict:
            stat_aggr_item["signalList"] = signal_dict[price_id]
        if price_id in stat_dict:
            stat_aggr_item["stat"] = stat_dict[price_id]
    return stat_aggr


def scan_ibd_stats(stat_aggr):
    keys = stat_aggr.keys()
    for key in keys:
        aggr_item = stat_aggr[key]
        ibd_stat = aggr_item["ibdStat"]
        print(ibd_stat["compositeRating"], ibd_stat["relativeStrength"], ibd_stat["groupStrength"], ibd_stat["accumDist"], ibd_stat["mgmtOwnPct"])


def display_tally_counts(tally_dict, min_size):
    keys = tally_dict.keys()
    for key in keys:
        tally_item = tally_dict[key]
        if len(tally_item) > min_size:
            print(len(tally_item), tally_item)


class StatisticAnalysisSvc:
    def __init__(self):
        self._signal_db = sig.SignalsDB()
        self._stat_db = stat.StatisticsDB()
        self._ibd_db = ibd.IbdStatisticDB()

    def retrieve_stats(self, ticker_symbol):
        stat_obj = {"tickerSymbol": ticker_symbol}
        signal_dict = {}
        sig_cnt_dict = {}
        signal_list = []
        for signal in self._signal_db.find_signal_by_ticker(ticker_symbol):
            try:
                signal_list = signal_dict[signal["priceId"]]
            except KeyError:
                signal_list = []
                signal_dict[signal["priceId"]] = signal_list
            signal_list.append(signal)
            try:
                sig_cnt = sig_cnt_dict[signal["signalType"]]
            except KeyError:
                sig_cnt = {signal["signalType"]: int(0)}
                sig_cnt_dict[signal["signalType"]] = sig_cnt
            sig_cnt[signal["signalType"]] = sig_cnt[signal["signalType"]] + 1
        stat_obj["signal_dict"] = signal_dict
        stat_obj["signal_list"] = signal_list
        stat_obj["sig_cnt_dict"] = sig_cnt_dict
        ibd_list = []
        for ibd_stat in self._ibd_db.find_stat_by_ticker(ticker_symbol):
            ibd_list.append(ibd_stat)
        stat_obj["ibd_list"] = ibd_list
        stat_dict = {}
        for a_stat in self._stat_db.find_stat_by_ticker(ticker_symbol):
            try:
                stat_item = stat_dict[a_stat["priceId"]]
            except KeyError:
                stat_item = {}
                stat_dict[a_stat["priceId"]] = stat_item
            stat_item[a_stat["statisticType"]] = a_stat["statisticValue"]
            stat_dict[a_stat["priceId"]] = stat_item
        stat_obj["stat_dict"] = stat_dict
        return stat_obj

    def tally_dashboard_stats(self):
        type_list = self._stat_db.stat_type_list()
        stat_occurrences = {}
        for a_stat_type in type_list:
            if a_stat_type.get("showInDashboard"):
                print(a_stat_type["statisticDesc"])
                stat_list = self._stat_db.find_stat_by_type(a_stat_type["_id"], 20)
                for stat_item in stat_list:
                    stat_tally = stat_occurrences.get(stat_item["tickerSymbol"])
                    if stat_tally is None:
                        stat_occurrences[stat_item["tickerSymbol"]] = [stat_item["_id"]]
                    else:
                        stat_tally = stat_occurrences[stat_item["tickerSymbol"]]
                        stat_tally.append(stat_item["_id"])
        return stat_occurrences
