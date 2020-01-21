# libraries to be used in this code module
import pandas as pd
from datetime import timedelta, date

# Declare and define a class
class BuySell:
    def __init__(self, engine, table_name):
        """
        Generate buy and sell signal for various trade strategies
        :param engine: provides connection to MySQL Server
        :param table_name: table name where ticker symbols are stored
        """
        self.engine = engine
        self.table_name = table_name
        self.buy = 1
        self.hold = 0
        self.sell = -1

    def cma_signal(self):
        """
        Cross Moving Averages Buy/Sell signals
        :return: none
        """
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        strategyCode = "'CMA'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0,0] == 0:
            strategyName = "'CrossMovingAverages'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        # loop through instruments
        for ID in df['instrumentid']:

            # remove calculations from database
            delete_query = 'DELETE FROM dbo_actionsignals WHERE instrumentid={} ' \
                           'AND strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # get relevant data for CMA signals
            query = 'SELECT A.date, A.close, B.wcma, B.scma, B.lcma  FROM dbo_instrumentstatistics AS A, ' \
                    'dbo_engineeredfeatures AS B WHERE A.instrumentid=B.instrumentid AND a.date=b.date ' \
                    'AND A.instrumentid=%s' % ID
            data = pd.read_sql_query(query, self.engine)

            # additional calculations to be used in CMA strategy
            # computed technical indicators first and stored them into the EngineeredFeatures table
            # so the pre-calculated values are available quickly, it reduces the overall runtime
            data['BUYweekApproach'] = data['wcma'].shift(1) / data['lcma']
            data['week_long'] = data['wcma'] / data['lcma']
            data['SELLweekApproach'] = data['wcma'].shift(1) / data['scma']
            data['week_Short'] = data['wcma'] / data['scma']
            data['momentumA'] = data['close'] / data['close'].shift(5)

            # insert buy or sell signal -  if no signal do nothing
            for n in range(len(data)):
                insert_query = "INSERT INTO dbo_actionsignals VALUES({},{},{},{})"
                date = "'" + str(data['date'][n]) + "'"
                if 0.977 < data['BUYweekApproach'][n] < 1.025 and data['week_long'][n] >= 1.018 and \
                        data['momentumA'][n] > 1.00:
                    insert_query = insert_query.format(date, ID, strategyCode, self.buy)
                    self.engine.execute(insert_query)
                elif 1.000 < data['SELLweekApproach'][n] and data['week_Short'][n] >= 0.93 and \
                        data['scma'][n-3] - data['scma'][n] > 0:
                    insert_query = insert_query.format(date, ID, strategyCode, self.sell)
                    self.engine.execute(insert_query)
                else:
                    insert_query = insert_query.format(date, ID, strategyCode, self.hold)
                    self.engine.execute(insert_query)

    def frl_signal(self):
        """
        Fibonacci Retracement Line Buy/Sell signals
        :return: NULL
        """
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        strategyCode = "'FRL'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            strategyName = "'FibonacciRetracementLines'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        for ID in df['instrumentid']:

            # remove calculations from database
            delete_query = 'DELETE FROM dbo_actionsignals WHERE instrumentid={} AND ' \
                           'strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # get relevant data for FRL signals
            query = 'SELECT A.date, A.close, B.ltrough, B.lpeak, B.highfrllinelong, B.medfrllinelong, B.lowfrllinelong  ' \
                    'FROM dbo_instrumentstatistics AS A, dbo_engineeredfeatures AS B WHERE A.instrumentid=B.instrumentid ' \
                    'AND A.date=B.date AND A.instrumentid=%s' % ID
            data = pd.read_sql_query(query, self.engine)

            # additional calculations
            # close divided by next day close value
            # close divided by 5th future day close value
            data['ActualChange'] = data['close'] / data['close'].shift(1)
            data['momentumA'] = data['close'] / data['close'].shift(5)

            # loop through data
            # compute FRL and Momentum values
            # checking key Fibonacci ratios
            for n in range(5, len(data)):
                insert_query = "INSERT INTO dbo_actionsignals VALUES({},{},{},{})"
                tradeDate = "'" + str(data['date'][n]) + "'"
                if (data['highfrllinelong'][n] * 1.0125 <= data['close'][n] <= (data['highfrllinelong'][n] * 1.0225))\
                    or (data['medfrllinelong'][n] * 1.0125 <= data['close'][n] <= (data['medfrllinelong'][n] * 1.0225))\
                    or (data['lowfrllinelong'][n] * 1.0125 <= data['close'][n] <= (data['lowfrllinelong'][n] * 1.0225))\
                    and data['momentumA'][n] < 0.99:
                    insert_query = insert_query.format(tradeDate, ID, strategyCode, self.buy)
                    self.engine.execute(insert_query)
                elif (data['highfrllinelong'][n] * 0.975 <= data['close'][n] <= (data['highfrllinelong'][n] * 0.985))\
                    or (data['medfrllinelong'][n] * 0.975 <= data['close'][n] <= (data['medfrllinelong'][n] * 0.985))\
                    or (data['lowfrllinelong'][n] * 0.975 <= data['close'][n] <= (data['lowfrllinelong'][n] * 0.985))\
                    and data['momentumA'][n] > 0.99 and data['ActualChange'][n-1]-data['ActualChange'][n] < 0.1:
                    insert_query = insert_query.format(tradeDate, ID, strategyCode, self.sell)
                    self.engine.execute(insert_query)
                else:
                    insert_query = insert_query.format(tradeDate, ID, strategyCode, self.hold)
                    self.engine.execute(insert_query)

    def ema_signal(self):
        """
        Exponential Moving Average Buy/Sell signals
        :return: none
        """
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        strategyCode = "'EMA'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            strategyName = "'ExponentialMovingAverages'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        # Loop through instruments
        for ID in df['instrumentid']:
            # remove calculations from database
            delete_query = 'DELETE FROM dbo_actionsignals WHERE instrumentid={} AND ' \
                           'strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # get relevant data for FRL signals
            query = 'SELECT A.date, A.close, B.sema, B.mema, B.lema FROM dbo_instrumentstatistics AS A, ' \
                    'dbo_engineeredfeatures AS B WHERE A.instrumentid=B.instrumentid AND A.date=B.date ' \
                    'AND A.instrumentid=%s' % ID
            data = pd.read_sql_query(query, self.engine)

            # additional calculations
            data['5DayAvg'] = data['close'].rolling(5).mean()
            #data['longrange'] = data['sema'] / data['lema']
            #data['midrange'] = data['sema'] / data['mema']
            data['sigLong'] = data['sema'] / data['lema']
            data['sigMid'] = data['sema'] / data['mema']
            data['momentum'] = data['close'] / data['5DayAvg']

            # Loop to insert into the actionsignals
            for n in range(len(data)):
                insert_query = "INSERT INTO dbo_actionsignals VALUES({},{},{},{})"
                date = "'" + str(data['date'][n]) + "'"
                if 0.97 < data['sigLong'][n] < 1.0 and data['momentum'][n] > .97:
                    insert_query = insert_query.format(date, ID, strategyCode, self.buy)
                    self.engine.execute(insert_query)
                elif 0.983 < data['sigMid'][n] < 1.004 and data['momentum'][n] < 1.012:
                    insert_query = insert_query.format(date, ID, strategyCode, self.sell)
                    self.engine.execute(insert_query)
                else:
                    insert_query = insert_query.format(date, ID, strategyCode, self.hold)
                    self.engine.execute(insert_query)

    def macd_signal(self):
        """
        MACD Buy/Sell signals
        :return: none
        """
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        strategyCode = "'MACD'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            strategyName = "'MACD'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        for ID in df['instrumentid']:
            # remove calculations from database
            delete_query = 'DELETE FROM dbo_actionsignals WHERE instrumentid={} AND ' \
                           'strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # get relevant data for MACD signals
            query = 'SELECT A.date, A.close, B.macd_v, B.macds_v FROM dbo_instrumentstatistics AS A, ' \
                    'dbo_engineeredfeatures AS B WHERE A.instrumentid=B.instrumentid AND A.date=B.date ' \
                    'AND A.instrumentid=%s' % ID
            data = pd.read_sql_query(query, self.engine)

            for n in range(1, len(data)):
                insert_query = "INSERT INTO dbo_actionsignals VALUES({},{},{},{})"
                date = "'" + str(data['date'][n]) + "'"
                if data['macd_v'][n] > data['macds_v'][n] and data['macd_v'][n-1] <= data['macds_v'][n-1]:
                    insert_query = insert_query.format(date, ID, strategyCode, self.buy)
                    self.engine.execute(insert_query)
                elif data['macd_v'][n] < data['macds_v'][n] and data['macd_v'][n-1] >= data['macds_v'][n-1]:
                    insert_query = insert_query.format(date, ID, strategyCode, self.sell)
                    self.engine.execute(insert_query)
                else:
                    insert_query = insert_query.format(date, ID, strategyCode, self.hold)
                    self.engine.execute(insert_query)

    def algo_signal(self):
        """
        MACD Buy/Sell signals
        :return: none
        """
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)
        strategyCode = "'algo'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_strategymaster WHERE strategycode=%s' % strategyCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            strategyName = "'algo'"
            insert_code_query = 'INSERT INTO dbo_strategymaster VALUES({},{})'.format(strategyCode, strategyName)
            self.engine.execute(insert_code_query)

        for ID in df['instrumentid']:
            # remove calculations from database
            delete_query = 'DELETE FROM dbo_actionsignals WHERE instrumentid={} AND ' \
                           'strategycode={}'.format(ID, strategyCode)
            self.engine.execute(delete_query)

            # use ARIMA and price prediction models for trading signals
            algoCode1 = "'ARIMA'"
            algoCode2 = "'PricePred'"

            # get relevant data for MACD signals
            query = 'SELECT A.date, A.close, B.forecastcloseprice AS forecast1, C.forecastcloseprice AS forecast2 ' \
                    'FROM dbo_instrumentstatistics AS A, dbo_algorithmforecast AS B, dbo_algorithmforecast AS C ' \
                    'WHERE A.instrumentid=B.instrumentid AND A.date=B.forecastdate AND B.algorithmcode={} ' \
                    'AND A.instrumentid=C.instrumentid AND A.date=C.forecastdate AND C.algorithmcode={} ' \
                    'AND A.instrumentid={}'.format(algoCode1, algoCode2, ID)
            data = pd.read_sql_query(query, self.engine)

            # loop to insert data into actionsignals table
            for n in range(1, len(data)):
                insert_query = "INSERT INTO dbo_actionsignals VALUES({},{},{},{})"
                date = "'" + str(data['date'][n]) + "'"
                if data['forecast1'][n] > data['close'][n-1] and data['forecast2'][n] > data['close'][n-1]:
                    insert_query = insert_query.format(date, ID, strategyCode, self.buy)
                    self.engine.execute(insert_query)
                elif data['forecast1'][n] < data['close'][n-1] and data['forecast2'][n] < data['close'][n-1]:
                    insert_query = insert_query.format(date, ID, strategyCode, self.sell)
                    self.engine.execute(insert_query)
                else:
                    insert_query = insert_query.format(date, ID, strategyCode, self.hold)
                    self.engine.execute(insert_query)

# END CODE MODULE