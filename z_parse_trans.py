import json

file_dir = "c:/Users/rober/Downloads"
file_name = "Contributory_XXX264_Transactions_20240831-010106.json"

tran_file = open(file_dir + '/' + file_name)
tran_json = json.load(tran_file)
sell_call_action = "Sell to Open"
cd_interest = "CD Interest"
qual_div = "Qualified Dividend"
non_qual_div = "Non-Qualified Div"
reinv_div = "Reinvest Dividend"


def sum_trans(action):
    summary = {}
    for tran in tran_json["BrokerageTransactions"]:
        if tran["Action"] == action:
            tran_yyyymm = tran["Date"][-4:] + tran["Date"][:2]
            tran_amt = float(tran["Amount"][1:])
            if tran_yyyymm not in summary:
                summary[tran_yyyymm] = tran_amt
            else:
                summary[tran_yyyymm] += tran_amt
    return summary


def print_summary(summary):
    for key in summary.keys():
        print(key, summary[key])


def aggregate_summary(summary, aggregate):
    for key in summary.keys():
        if key not in aggregate:
            aggregate[key] = summary[key]
        else:
            aggregate[key] += summary[key]
    return aggregate


total_income = {}

print("Covered Call Summary")
call_summary = sum_trans(sell_call_action)
print_summary(call_summary)
aggregate_summary(call_summary, total_income)

print("Dividends (Qualified)")
qual_dividends = sum_trans(qual_div)
print_summary(qual_dividends)
aggregate_summary(qual_dividends, total_income)

print("Dividends (Non-qualified)")
non_qual_dividends = sum_trans(non_qual_div)
print_summary(non_qual_dividends)
aggregate_summary(non_qual_dividends, total_income)

print("Reinvest Dividends")
reinv_summary = sum_trans(reinv_div)
print_summary(reinv_summary)
aggregate_summary(reinv_summary, total_income)

print("CD Interest")
cd_summary = sum_trans(cd_interest)
print_summary(cd_summary)
aggregate_summary(cd_summary, total_income)

print("Total Income")
print_summary(total_income)