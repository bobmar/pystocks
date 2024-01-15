from pkg.repo import ibdstat
from pkg.repo import stockdata

ibd_db = ibdstat.IbdStatisticDB()
sd_db = stockdata.StocksDB()


def dedupe_list(ibd_list):
    new_list = []
    for list_name in ibd_list:
        if list_name in new_list:
            continue
        else:
            new_list.append(list_name)
    return new_list


for ticker in sd_db.ticker_symbol_list(600):
    for ibd in ibd_db.find_stat_by_ticker(ticker):
        if len(ibd['listName']) > 1:
            new_list = dedupe_list(ibd['listName'])
            if len(ibd['listName']) != len(new_list):
                print(ibd['_id'], ibd['listName'], new_list)
                ibd_db.update_stat(ibd['_id'], 'listName', new_list)

