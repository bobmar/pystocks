import json
import os

file_dir = os.environ["USERPROFILE"] + "\\Downloads"
file_name = "Contributory_XXX264_Transactions.json"

tran_file = open(file_dir + '/' + file_name)
tran_json = json.load(tran_file)
sell_call_action = "Sell to Open"
cd_interest = "CD Interest"
qual_div = "Qualified Dividend"
non_qual_div = "Non-Qualified Div"
reinv_div = "Reinvest Dividend"

action_list = [sell_call_action, cd_interest, qual_div, non_qual_div, reinv_div]


def sum_trans(action, summary):
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
        summary_amt = round(summary[key], 2)
        print(key, summary_amt)


def aggregate_summary(summary, aggregate):
    for key in summary.keys():
        if key not in aggregate:
            aggregate[key] = summary[key]
        else:
            aggregate[key] += summary[key]
    return aggregate


def accumulate_transactions(action):
    print('\n'+action)
    summary = {}
    summary = sum_trans(action, summary)
    print_summary(summary)
    aggregate_summary(summary, total_income)


total_income = {}
for action in action_list:
    accumulate_transactions(action)

print("\nTotal Income")
print_summary(total_income)
