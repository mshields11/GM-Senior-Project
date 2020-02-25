from datetime import datetime, timedelta
from enum import Enum

from statsmodels.tsa.arima_model import ARIMA
from bt.algos import SelectWhere

import pandas as pd

import bt
import ffn

class Tickers(Enum):
    GM = 0
    PFE = 1
    SPY = 2
    XPH = 3
    CARZ = 4


class BacktestEngine:
    def __init__(self, start, end, algo="ARIMA"):
        if not isinstance(start, datetime):
            raise ValueError("Illegal argument for start. {}".format(start))
        if not isinstance(end, datetime):
            raise ValueError("Illegal argument for end. {}".format(end))
        self._start_date = start.strftime('%Y-%m-%d')
        self._end_date = end.strftime('%Y-%m-%d')

        self._symbols = []
        self._signals = {}
        self._close_prices = {}
        self._adj_close_prices = {}

        self._init_tickers()
        self._init_data()

    def _init_tickers(self):
        symbols = ["GM", "PFE", "SPY", "XPH", "CARZ"]
        symbols = [symbol.lower() for symbol in symbols]
        self._symbols = symbols

    def _init_data(self):
        for symbol in self._symbols:
            tickers = [symbol + ":Close", symbol]
            tickers_str = ','.join(tickers)
            #print(tickers_str)
            data = ffn.data.get(tickers_str, start=self._start_date, end=self._end_date)
            #print(data)
            data_close_prices = data[tickers[0].replace(":","").lower()].tolist()
            data_close_prices = pd.Series(data_close_prices)
            #print(data_close_prices)
            data_adjclose_prices = data[tickers[1].replace(":","").lower()].tolist()
            data_adjclose_prices = pd.Series(data_adjclose_prices)
            #print(data_adjclose_prices)
            self._close_prices[symbol] = data_close_prices
            self._adj_close_prices[symbol] = data_adjclose_prices

    def run_arima(self):
        test_length = 20
        for symbol in self._symbols:
            data_close_prices = self._close_prices[symbol]
            arima = ARIMA(data_close_prices[:-1*test_length], order=(0,1,0))
            arima_fitted = arima.fit(disp=-1)
            arima_testdata = data_close_prices[-1*test_length:]
            self._signals[symbol] = []
            for _ in range(test_length):
                arima_forecast = arima_fitted.forecast()
                # TODO: apply logic for buy/sell/hold using arima forecast
                if (arima_forecast[0] - data_close_prices[_]) > 0:
                    self._signals[symbol].append(1)
                else:
                    self._signals[symbol].append(-1)


if __name__ == '__main__':
    backtest_start = datetime(2019,1,1)
    backtest_end = datetime(2020,1,1)
    backtest = BacktestEngine(backtest_start, backtest_end)
    backtest.run_arima()
    for symbol in backtest._symbols:
        print(backtest._signals[symbol])
    # tickers = "gm:Close,gm"
    # data = ffn.data.get(tickers, start='2010-01-01', end='2014-01-01')
    # arima = ARIMA(data["gmclose"][:-10], order=(0,1,0))
    # fit = arima.fit(disp=-1)
    # forecast = fit.forecast(steps=10)
    # test_data = data[-10:]

    #     ruleset = [
    #         SelectWhere(arima.forecast()),
    #         bt.algos.WeighEqually(),
    #         bt.algos.Rebalance()
    #     ]
    #     s = bt.Strategy('ARIMA', ruleset)
    #     test = bt.Backtest(s, bt.get(symbol, start=self._start_date, end=self._end_date))
    #     result = bt.run(test)
    #     result.display()

    # def _get_data_for_symbol(self, index=0):
    #     """Get pandas.series.Series for symbol at :index:, AdjClose"""
    #     symbol = self._symbols[index]
    #     data = bt.get(symbol, start=self._start_date)
    #     data = data[symbol].tolist()
    #     data = pd.Series(data)
    #     return data

    # def get_data(self):
    #     data = bt.get(self._symbols, start=self._start_date)
    #     ruleset = [
    #         bt.algos.RunMonthly(),
    #         bt.algos.SelectAll(),
    #         bt.algos.WeighEqually(),
    #         bt.algos.Rebalance()
    #     ]
    #     s = bt.Strategy('s1', ruleset)
    #     test = bt.Backtest(s, data)
    #     result = bt.run(test)
    #     result.display()
