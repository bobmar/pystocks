import datetime
from pkg.repo import stockstat as ss
from pkg.repo import stockdata as sd

net_pct_chg = 0.0
up_cnt = 0
dn_cnt = 0
sd = sd.StocksDB()

ssat = ss.StatisticsDB()
price_dates = sd.find_price_dates(20)

for price_date in price_dates:
    stats = ssat.find_stat_by_type_and_date('DYPCTCHG', price_date, 500)
    for stat in stats:
        if stat['statisticValue'] > 0:
            up_cnt += 1
        else:
            dn_cnt += 1
        net_pct_chg += stat['statisticValue']
    print(price_date, 'up:', up_cnt, 'dn:', dn_cnt, 'net chg:', net_pct_chg)
    up_cnt = 0
    dn_cnt = 0
    net_pct_chg = 0.0
