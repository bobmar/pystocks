from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score

class LinearModelFactory():
    def make_linear_regression(self, df, x_columns, y_label):
        train_X, test_X, train_Y, test_Y = train_test_split(df[x_columns],df[y_label],test_size=0.2)
        return LinearRegression(normalize=True).fit(train_X, train_Y), test_X, test_Y

    def do_regression(self, model, test_features, test_labels):
        pred = model.predict(test_features)
        score = r2_score(test_labels, pred)
        return score