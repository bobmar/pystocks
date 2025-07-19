import json
import os

file_dir = os.environ["USERPROFILE"] + "\\Downloads"
file_name = "Contributory_XXX264_Transactions.json"

tran_file = open(file_dir + '/' + file_name)
tran_json = json.load(tran_file)
sell_call_action = "Sell to Open"
cd_interest = "CD Interest"
qual_div = "Qualified Dividend"
qual_div_reinv = "Qual Div Reinvest"
spec_non_qual_div = "Special Non Qual Div"
non_qual_div = "Non-Qualified Div"
spec_non_qual_div = "Special Non Qual Div"
reinv_div = "Reinvest Dividend"
cash_div = "Cash Dividend"
qual_div_reinv = "Qual Div Reinvest"
spec_qual_div = "Special Qual Div"

<<<<<<< HEAD
action_list = [sell_call_action, cd_interest, qual_div, non_qual_div, reinv_div, qual_div_reinv, spec_non_qual_div]
=======

action_list = [sell_call_action, cd_interest, qual_div, spec_non_qual_div, non_qual_div,
               reinv_div, cash_div, qual_div_reinv, spec_qual_div]
>>>>>>> 01354b1bc98d73843c65e2a5f40aa3a6fc34a699


def sum_trans(action, summary):
    for tran in tran_json["BrokerageTransactions"]:
        if tran["Action"] == action:
            tran_yyyymm = tran["Date"][-4:] + tran["Date"][:2]
            tran_amt = float(tran["Amount"][1:].replace(',',''))
            if tran_yyyymm not in summary:
                summary[tran_yyyymm] = tran_amt
            else:
                summary[tran_yyyymm] += tran_amt
    return summary


def print_summary(summary):
    sorted_summary = dict(sorted(summary.items(), reverse=True))
    for key in sorted_summary.keys():
        summary_amt = round(sorted_summary[key], 2)
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
