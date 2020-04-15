import pandas as pd
from math import floor, fabs


class TradingSimulator:
    def __init__(self, engine, table_name):
        """
        Simulate trades for each ticker symbol with a portfolio starting with $10000
        :param engine: provides connection to MySQL Server
        :param table_name: table name where ticker symbols are stored
        """
        self.engine = engine
        self.table_name = table_name
        self.strategy_master = 'dbo_strategymaster'

    def trade_sim(self):
        """
        Run simulations based on individual trade strategies
        :return: none
        """

        # get ticker symbols from database
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)

        # get strategy codes from database
        code_query = 'SELECT strategycode FROM %s' % self.strategy_master
        strategyCodes = pd.read_sql_query(code_query, self.engine)

        # simulated each code
        for code in strategyCodes['strategycode']:
            code = "'" + code + "'"
            # ...for each instrument
            for ID in df['instrumentid']:
                delete_query = 'DELETE FROM dbo_statisticalreturns WHERE instrumentid={} ' \
                               'AND strategycode={}'.format(ID, code)
                self.engine.execute(delete_query)

                # get action signals for symbol
                query = 'SELECT A.*, B.close FROM dbo_actionsignals AS A, dbo_instrumentstatistics AS B WHERE ' \
                        'A.instrumentid={} AND strategycode={} AND A.instrumentid=B.instrumentid ' \
                        'AND a.date=B.date'.format(ID, code)
                data = pd.read_sql_query(query, self.engine)

                # start simulation with $10000 in cash
                cash = 10000
                positionSize = 0
                for n in range(len(data)):
                    insert_query = 'INSERT INTO dbo_statisticalreturns VALUES ({}, {}, {}, {}, {}, {})'
                    close = data['close'][n]
                    signal = data['signal'][n]
                    date = "'" + str(data["date"][n]) + "'"

                    # if no position currently
                    if positionSize == 0 and signal != 0:
                        # buy/sell as many whole shares as possible
                        positionSize = floor(cash / close)
                        if signal == -1:
                            positionSize = positionSize * -1
                        # track cash left over
                        cash = cash - (fabs(positionSize * close))
                        # total portfolio value
                        portfolioValue = cash + (fabs(positionSize * close))
                        insert_query = insert_query.format(date, ID, code, positionSize, cash, portfolioValue)
                        self.engine.execute(insert_query)

                    # close out an existing position
                    elif positionSize > 0 and signal == -1 or positionSize < 0 and signal == 1:
                        # return gain or loss to cash
                        cash = cash + (fabs(positionSize * close))
                        portfolioValue = cash
                        # reset position size
                        positionSize = 0
                        insert_query = insert_query.format(date, ID, code, positionSize, cash, portfolioValue)
                        self.engine.execute(insert_query)

                    # record updated portfolio value if no action for that day
                    else:
                        portfolioValue = cash + fabs(positionSize * close)
                        insert_query = insert_query.format(date, ID, code, positionSize, cash, portfolioValue)
                        self.engine.execute(insert_query)

    def combination_trade_sim(self):
        """
        Run simulations based on collective trade strategies
        Sum signals for all individula strategies
        Use this as one signal
        :return: none
        """

        # get ticker symbols from database
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)

        strategyCode = "'COMB'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            strategyName = "'Combination'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        # run simulation for each symbol
        for ID in df['instrumentid']:
            delete_query = 'DELETE FROM dbo_statisticalreturns WHERE instrumentid={} ' \
                           'AND strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # get sum of individual signals for each day
            query = 'SELECT A.date, SUM(A.signal) AS signalsum, A.instrumentid, B.close FROM dbo_actionsignals AS A, ' \
                    'dbo_instrumentstatistics AS B WHERE A.instrumentid={} AND A.instrumentid=B.instrumentid ' \
                    'AND A.date=B.date GROUP BY A.instrumentid, A.date, B.close ORDER BY A.date'.format(ID)
            data = pd.read_sql_query(query, self.engine)

            # start portfolio with $10000 in cash
            cash = 10000
            positionSize = 0
            for n in range(len(data)):
                insert_query = 'INSERT INTO dbo_statisticalreturns VALUES ({}, {}, {}, {}, {}, {})'
                close = data['close'][n]
                signal = data['signalsum'][n]
                date = "'" + str(data["date"][n]) + "'"

                # if no position currently
                if positionSize == 0:
                    positionSize = floor(cash / close)
                    if signal < 0:
                        positionSize = positionSize * -1
                    cash = cash - (fabs(positionSize * close))
                    portfolioValue = cash + (fabs(positionSize * close))
                    insert_query = insert_query.format(date, ID, strategyCode, positionSize, cash, portfolioValue)
                    self.engine.execute(insert_query)

                # close out an existing position
                elif positionSize > 0 > signal or positionSize < 0 < signal:
                    cash = cash + (fabs(positionSize * close))
                    portfolioValue = cash
                    positionSize = 0
                    insert_query = insert_query.format(date, ID, strategyCode, positionSize, cash, portfolioValue)
                    self.engine.execute(insert_query)

                # record updated portfolio value if no action for that day
                else:
                    portfolioValue = cash + fabs(positionSize * close)
                    insert_query = insert_query.format(date, ID, strategyCode, positionSize, cash, portfolioValue)
                    self.engine.execute(insert_query)

    def buy_hold_sim(self):
        """
        Run simulations based on buy and hold trade strategy
        :return: none
        """

        # get ticker symbols from database
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        code = "'BuyHold'"

        # run simulation for each ticker symbol
        for ID in df['instrumentid']:
            delete_query = 'DELETE FROM dbo_statisticalreturns WHERE instrumentid={} ' \
                           'AND strategycode={}'.format(ID, code)
            self.engine.execute(delete_query)

            # get market data for each day in database
            query = 'SELECT date, close FROM dbo_instrumentstatistics WHERE instrumentid={}'.format(ID)
            data = pd.read_sql_query(query, self.engine)

            # start simulation with $10000 in cash
            cash = 10000
            positionSize = 0
            for n in range(len(data)):
                insert_query = 'INSERT INTO dbo_statisticalreturns VALUES ({}, {}, {}, {}, {}, {})'
                close = data['close'][n]
                date = "'" + str(data["date"][n]) + "'"

                # if no position currently (only the first day)
                if positionSize == 0:
                    # buy/sell as many shares as possible
                    positionSize = floor(cash / close)
                    # track cash left over
                    cash = cash - (fabs(positionSize * close))
                    # total portfolio value
                    portfolioValue = cash + (fabs(positionSize * close))
                    insert_query = insert_query.format(date, ID, code, positionSize, cash, portfolioValue)
                    self.engine.execute(insert_query)

                # update portfolio value each day with most recent share price
                else:
                    portfolioValue = cash + fabs(positionSize * close)
                    insert_query = insert_query.format(date, ID, code, positionSize, cash, portfolioValue)
                    self.engine.execute(insert_query)
