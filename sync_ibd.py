from pkg.repo import stockdata as sd

sdb = sd.StocksDB()

ibd_delta = sdb.ibd_ticker_delta()
print('Deleted ', sdb.delete_ticker_in_list(ibd_delta))