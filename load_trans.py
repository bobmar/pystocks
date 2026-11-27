import json
import os
from pkg.repo import fininfo as fr
from datetime import datetime

file_dir = os.environ["USERPROFILE"] + "\\Downloads"
file_name = "Contributory_XXX264_Transactions.json"
fin_repo = fr.FinancialRatio()

tran_file = open(file_dir + '/' + file_name)
tran_json = json.load(tran_file)

def transform_tran(tran):
    tran_record = {}
    d = datetime.strptime(tran["Date"][0:10], "%m/%d/%Y").date()
    tran_record["tranDate"] = datetime(d.year, d.month, d.day)
    tran_record["tranMonth"] = tran["Date"][-4:] + tran["Date"][:2]
    tran_record["action"] = tran["Action"]
    tran_record["ticker"] = tran["Symbol"]
    if tran["Amount"] != "":
        tran_record["amount"] = float(tran["Amount"].replace("$", "").replace(",", ""))
    return tran_record


tran_list = []
for tran in tran_json["BrokerageTransactions"]:
    tran_list.append(transform_tran(tran))

fin_repo.clear_trans()
fin_repo.save_trans(tran_list)
aggregate_income = fin_repo.aggr_income()
prev_yr_mo = ''
total_income = 0.00
for item in aggregate_income:
    if prev_yr_mo == "":
        prev_yr_mo = item['_id']['yrMonth']
        print(prev_yr_mo)
    if item['_id']['yrMonth'] != prev_yr_mo:
        print(prev_yr_mo, f"${total_income:,.2f}")
        print()
        total_income = 0.00
        prev_yr_mo = item['_id']['yrMonth']
        print(prev_yr_mo)
    total_income = total_income + item['amt']
    print(item['_id']['action'], f"${item['amt']:,.2f}")

print(prev_yr_mo, f"${total_income:,.2f}")
