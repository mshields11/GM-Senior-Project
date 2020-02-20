from datetime import datetime, timedelta
from bt.algos import SelectWhere

import pandas as pd
import bt


class DataFetch:
    def __init__(self):
        #declaring variables for symbols, time length, and start/end date
        self._symbols = ""
        self._datalength = 1096
        self._start_date = (datetime.now() - timedelta(days=self._datalength)).strftime('%Y-%m-%d')
        self._get_datasources()
        #inputting symbols
    def _get_datasources(self):
        symbols = ["GM", "PFE", "SPY", "XPH", "CARZ"]
        symbols = [symbol.lower() for symbol in symbols]
        symbols = ','.join(symbols)
        self._symbols = symbols
        #retrieving backtest for symbols with ruleset
    def get_data(self):
        data = bt.get(self._symbols, start=self._start_date)
        ruleset = [
            bt.algos.RunMonthly(),
            bt.algos.SelectAll(),
            bt.algos.WeighEqually(),
            bt.algos.Rebalance()
        ]
        s = bt.Strategy('s1', ruleset)
        test = bt.Backtest(s, data)
        result = bt.run(test)
        result.display()
        #sma algorithm
    def exec_sma(self):
        data = bt.get(self._symbols, start=self._start_date)
        sma = data.rolling(50).mean()
        ruleset = [
            SelectWhere(data > sma),
            bt.algos.WeighEqually(),
            bt.algos.Rebalance()
        ]
        s = bt.Strategy('above50sma', ruleset)
        test = bt.Backtest(s, data)
        result = bt.run(test)
        result.display()

        #running different backtests
if __name__ == '__main__':
    data_fetch = DataFetch()
    #default buy/hold from get_data
    print("BUY HOLD")
    print("-"*10)
    data_fetch.get_data()

    print("SMA")
    print("-"*10)
    data_fetch.exec_sma()
