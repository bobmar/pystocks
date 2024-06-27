import json

file_dir = "c:/Users/rober/Downloads"
file_name = "Contributory_XXX264_Transactions_20240625-024350.json"

tran_file = open(file_dir + '/' + file_name)
tran_json = json.load(tran_file)
sell_call_action = "Sell to Open"
cd_interest = "CD Interest"


def sum_cd_interest():
    cd_interest_summary = {}
    for tran in tran_json["BrokerageTransactions"]:
        if tran["Action"] == cd_interest:
            tran_yyyymm = tran["Date"][-4:] + tran["Date"][:2]
            tran_amt = float(tran["Amount"][1:])
            if tran_yyyymm not in cd_interest_summary:
                cd_interest_summary[tran_yyyymm] = tran_amt
            else:
                cd_interest_summary[tran_yyyymm] += tran_amt
    return cd_interest_summary


def sum_call_premium():
    call_premium_summary = {}
    for tran in tran_json["BrokerageTransactions"]:
        if tran["Action"] == sell_call_action:
            tran_yyyymm = tran["Date"][-4:] + tran["Date"][:2]
            tran_amt = float(tran["Amount"][1:])
            if tran_yyyymm not in call_premium_summary:
                call_premium_summary[tran_yyyymm] = tran_amt
            else:
                call_premium_summary[tran_yyyymm] += tran_amt
    return call_premium_summary


print("Covered Call Summary")
call_summary = sum_call_premium()
for key in call_summary.keys():
    print(key, call_summary[key])

print("CD Interest")
cd_summary = sum_cd_interest()
for key in cd_summary.keys():
    print(key, cd_summary[key])
