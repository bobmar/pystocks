from pkg.repo import stockdata as sd
import datetime
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Lasso
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor

db = sd.StocksDB()


def calc_price_id(weeks, ticker, price_date):
    delta = datetime.timedelta(weeks * 7)
    search_date = (price_date - delta)
    previous_day = 1
    if search_date.weekday() == 0:
        previous_day = 3
    search_date_1 = search_date - datetime.timedelta(previous_day)
    param_date = search_date.strftime('%Y-%m-%d')
    param_date_1 = search_date_1.strftime('%Y-%m-%d')
    price_id = ticker + ":" + param_date
    price_id_1 = ticker + ":" + param_date_1
    return price_id, price_id_1


def stat_type_list():
    st_list = ["AVG20V200", "DYPRCV200A", "DYPRCV50A", "NETABVBLW50", "TRMOM", "UPDNVOL50", "ZSCORE", "STDDEV2WK", "STDDEV10WK"]
    st_list.sort()
    return st_list


def stat_feature_dict(weeks, ticker_symbol, price_date):
    price_id, price_id_1 = calc_price_id(weeks, ticker_symbol, price_date)
    stat_list = db.find_selected_stat_by_price_id(price_id, stat_type_list())
    if stat_list is None:
        stat_list = db.find_selected_stat_by_price_id(price_id_1, stat_type_list())
        price_id = price_id_1
    stat_features = {}
    for stat in stat_list:
        stat_features.update({stat["statisticType"]: float(stat["statisticValue"])})

    if len(stat_features.keys()) != len(stat_type_list()):
        stat_features = {}
    return stat_features, price_id


def make_stats_dict(stat_list, num_weeks):
    """
    :param stat_list: list object of statistic keys
    :param num_weeks: number of weeks for price change (currently 4, 8 or 12)
    :return: dictionary object containing requested percent change and corresponding statistic value.
    """
    stats_dict = {}
    for stat in stat_list:
        stat_feat_dict, price_id = stat_feature_dict(num_weeks, stat["tickerSymbol"], stat["priceDate"])
        if bool(stat_feat_dict):
            if float(stat["statisticValue"]) > 0:
                stats_dict.update({stat["priceDate"]: (float(stat["statisticValue"]),) + tuple(stat_feat_dict.values())})
    return stats_dict


def create_price_stat_dataframe(ticker_symbol, stat_type):
    prices = db.find_price_by_ticker(ticker_symbol)
    stats = db.find_stat_by_ticker_and_type(ticker_symbol, stat_type)
    price_dict = {}
    stats_dict = {}
    for price in prices:
        price_dict.update({price["priceDate"]: float(price["closePrice"])})
    for stat in stats:
        price_date = stat["priceDate"]
        stats_dict.update({stat["priceDate"]: (float(price_dict[price_date]), float(stat["statisticValue"]))})
        df = pd.DataFrame(stats_dict.values(), stats_dict.keys(), columns=["Close", "Stat"])
    return df


def create_price_dataframe(ticker_symbol):
    """
    Create a DataFrame of stock prices for given ticker symbol.
    :param ticker_symbol:
    :return: DataFrame containing stock prices
    """
    prices = db.find_price_by_ticker(ticker_symbol)
    price_dict = {}
    for price in prices:
        price_dict.update({price["priceDate"]: (float(price["openPrice"]), float(price["highPrice"]), float(price["lowPrice"]), float(price["closePrice"]), float(price["volume"]))})
    df = pd.DataFrame(price_dict.values(), price_dict.keys(), columns=["Open", "High", "Low", "Close", "Volume"])
    return df


def do_linear_regression(df, x_columns, y_label):
    train_x, test_x, train_y, test_y = train_test_split(df[x_columns], df[y_label], test_size=0.2)
    lm = LinearRegression(normalize=True)
    lm.fit(train_x, train_y)
    print("{} Train: {}".format(y_label, lm.score(train_x, train_y)))
    pred_y = lm.predict(test_x)
    print("{} Test: {}".format(y_label, r2_score(test_y, pred_y)))
    return pred_y, test_x


def run_model(df, x_columns, y_label, model):
    train_x, test_x, train_y, test_y = train_test_split(df[x_columns], df[y_label], test_size=0.2)
    trained = train_model(train_x, train_y, model)
    print("{} Train: {}".format(y_label, model.score(train_x, train_y)))
    pred_y = trained.predict(test_x)
    print("{} Test: {}".format(y_label, r2_score(test_y, pred_y)))
    return pred_y, test_x


def train_model(x, y, model):
    model.fit(x, y)
    return model


def create_pipeline(model):
    pipeline_reg = make_pipeline(StandardScaler(), model)
    return pipeline_reg


def create_lasso_model():
    lm = Lasso(alpha=0.1)
    return lm


def create_linear_model():
    lm = LinearRegression(normalize=True)
    return lm


def create_ridge_model():
    lm = Ridge(alpha=0.5)
    return lm


def create_sgd_model():
    lm = SGDRegressor(max_iter=1000, tol=1e-3)
    return lm


def create_stats_dict(key, ticker_list):
    """
    :param key: a value representing the price change duration - PCTCHG4WK, PCTCHG8WK, PCTCHG12WK
    :param ticker_list: list object containing strings
    :return: a dictionary containing the price change values with the statistic values from the corresponding date.
    """
    stat_list = db.find_stats_by_tickers_and_stat_type(ticker_list, key)
    sf_dict = make_stats_dict(stat_list, db.stat_dict()[key]["numWeeks"])
    return sf_dict


def create_stats_dataframe(key, ticker_list):
    """
    :param key: a value representing the price change duration - PCTCHG4WK, PCTCHG8WK, PCTCHG12WK
    :param ticker_list: list object containing strings
    :return: a DataFrame containing the price change values with the statistic values from the corresponding date.
    """
    sf_dict = create_stats_dict(key, ticker_list)
    df = pd.DataFrame(sf_dict.values(), sf_dict.keys(), columns=[db.stat_dict()[key]["value"], ] + stat_type_list())
    return df


def create_ibd_dict(ticker):
    """
    :param ticker: stock's ticker symbol
    :return: dictionary of IBD statistics
    """
    ibd_stat_list = db.ibd_stat_list(ticker)
    ibd_dict = {}
    for ibd_stat in ibd_stat_list:
        ibd_dict.update({ibd_stat["_id"]: ibd_stat})
    return ibd_dict


def create_ibd_dataframe(ticker):
    """
    Create a Dataframe containing selected IBD statistics for ticker
    :param ticker: stock's ticker symbol
    :return: Dataframe containing selected IBD statistics
    """
    column_names = ["Composite", "EPS", "Rel Str"]
    ibd_dict = {}
    for ibd_stat in db.ibd_stat_list(ticker):
        ibd_row = (float(ibd_stat["compositeRating"]), float(ibd_stat["epsRating"]), float(ibd_stat["relativeStrength"]))
        ibd_dict.update({ibd_stat["_id"]: ibd_row})
    df = pd.DataFrame(ibd_dict.values(), ibd_dict.keys(), columns=column_names)
    return df


def do_regression(key, ticker_list):
    stat_list = db.find_stats_by_tickers_and_stat_type(ticker_list, key)
    stat_dict = db.stat_dict()
    sf_dict = make_stats_dict(stat_list, db.stat_dict()[key]["numWeeks"])
    df = pd.DataFrame(sf_dict.values(), sf_dict.keys(), columns=[stat_dict[key]["value"], ] + stat_type_list())
    df = df.dropna()
    df = df.round(2)
    print("\nRun linear model")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_linear_model())
    test["Predicted"] = pred
    print("Run linear model (standardized)")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_pipeline(create_linear_model()))
    test["Predicted"] = pred
    print("\nRun lasso model")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_lasso_model())
    test["Predicted"] = pred
    print("Run lasso model (standardized)")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_pipeline(create_lasso_model()))
    test["Predicted"] = pred
    print("\nRun ridge model")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_ridge_model())
    test["Predicted"] = pred
    print("Run ridge model (standardized)")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_pipeline(create_ridge_model()))
    test["Predicted"] = pred
    print("\nRun SGD model")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_sgd_model())
    test["Predicted"] = pred
    print("Run SGD model (standardized)")
    pred, test = run_model(df, stat_type_list(), stat_dict[key]["value"], create_pipeline(create_sgd_model()))
    test["Predicted"] = pred
