from pkg.repo import fininfo as fr

fin_ratio_names = ['currentRatio', 'quickRatio', 'netProfitMargin', 'debtEquityRatio', 'returnOnEquity',
                   'returnOnAssets', 'cashFlowToDebtRatio', 'freeCashFlowPerShare', 'operatingCashFlowSalesRatio',
                   'operatingProfitMargin', 'debtEquityRatio', 'priceSalesRatio', 'priceCashFlowRatio']
fin_growth_names = ['revenueGrowth', 'netIncomeGrowth', 'epsgrowth', 'grossProfitGrowth', 'operatingIncomeGrowth',
                    'threeYRevenueGrowthPerShare', 'threeYOperatingCFGrowthPerShare', 'threeYNetIncomeGrowthPerShare',
                    'operatingCashFlowGrowth', 'operatingCashFlowGrowth', 'freeCashFlowGrowth', 'assetGrowth']

fr_db = fr.FinancialRatio()


def find_fin_ratio(ticker_symbol):
    ratios = fr_db.find_ratios(ticker_symbol)
    if len(ratios) > 0:
        return ratios[0]
    else:
        return None


def find_fin_growth(ticker_symbol):
    fin_growth = fr_db.find_growth(ticker_symbol)
    if len(fin_growth) > 0:
        return fin_growth[0]
    else:
        return None


def retrieve_fin_growth(ticker):
    f_growth = find_fin_growth(ticker)
    fg_attr = None
    if f_growth is not None:
        fg_attr = {}
        for fg_name in fin_growth_names:
            if fg_name in f_growth.keys():
                fg_attr[fg_name] = f_growth[fg_name]
    return fg_attr


def retrieve_fin_ratio(ticker):
    f_ratio = find_fin_ratio(ticker)
    fr_attr = None
    if f_ratio is not None:
        fr_attr = {}
        for fr_name in fin_ratio_names:
            if fr_name in f_ratio.keys():
                fr_attr[fr_name] = f_ratio[fr_name]
    return fr_attr
