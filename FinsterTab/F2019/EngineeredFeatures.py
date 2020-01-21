import pandas as pd
from stockstats import StockDataFrame as Sdf
import sqlalchemy as sal


class EngineeredFeatures:
    def __init__(self, engine, table_name):
        """
        Calculate Technical Indicators and store in dbo_EngineeredFeatures
        :param engine: provides connection to MySQL Server
        :param table_name: table name where ticker symbols are stored
        """
        self.engine = engine
        self.table_name = table_name

    def calculate(self):
        """
        Calculate Technical Indicators and store in dbo_EngineeredFeatures
        """

        # get list of ticker symbols
        pd.set_option('mode.chained_assignment', None)
        query = 'SELECT * FROM %s' % self.table_name
        df = pd.read_sql_query(query, self.engine)

        # length variables for moving averages and FR lines
        week_n = 7
        short_n = 20
        mid_n = 50
        long_n = 100
        low_n = 14
        high_n = 100
        k_n = 1000

        # loop through each ticker symbol and retrieve historic data
        for ID in df['instrumentid']:
            query = 'SELECT * FROM dbo_instrumentstatistics WHERE instrumentid=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(query, self.engine)
            # rename because RSI uses close
            data.rename(columns={'Date': 'date', 'High': 'high', 'Low': 'low',
                                 'Open': 'open', 'Close': 'close', 'Volume': 'volume',
                                 'Adj Close': 'adj close', 'instrumentid': 'instrumentid'}, inplace=True)

            # SDF wrapper to calculate indicators
            stock_df = Sdf.retype(data)

            # RSI, MACD, Bollinger Bands calculations
            data['rsi'] = stock_df['rsi_14']
            data['macd_v'] = stock_df['macd']
            data['macds_v'] = stock_df['macds']
            data['boll_v'] = stock_df['boll']
            data['boll_lb_v'] = stock_df['boll_lb']
            data['boll_ub_v'] = stock_df['boll_ub']
            data['open_2_sma'] = stock_df['open_2_sma']
            data['volume_delta'] = stock_df['volume_delta']

            # delete unnecessary columns
            del data['high'], data['low'], data['open'], data['volume'], data['close_-1_s'], data['close_-1_d'], \
                data['closepm'], data['closenm'], data['closepm_14_smma'], data['closenm_14_smma'], data['rs_14'], \
                data['rsi']

            # calculate moving averages
            data['wcma'] = data['close'].rolling(week_n).mean()
            data['scma'] = data['close'].rolling(short_n).mean()
            data['lcma'] = data['close'].rolling(long_n).mean()

            # calculate fibonacci retracement lines
            data['ltrough'] = data['close'].rolling(high_n).min()
            data['lpeak'] = data['close'].rolling(high_n).max()
            data['highfrllinelong'] = data['lpeak'] - ((data['lpeak'] - data['ltrough']) * 0.236)
            data['medfrllinelong'] = data['lpeak'] - ((data['lpeak'] - data['ltrough']) * 0.382)
            data['lowfrllinelong'] = data['lpeak'] - ((data['lpeak'] - data['ltrough']) * 0.618)
            data['strough'] = data['close'].rolling(low_n).min()
            data['speak'] = data['close'].rolling(low_n).max()
            data['ktrough'] = data['close'].rolling(k_n).min()
            data['kpeak'] = data['close'].rolling(k_n).max()

            # calculate exponential moving averages
            data['sema'] = data['close'].ewm(span=short_n).mean()
            data['mema'] = data['close'].ewm(span=mid_n).mean()
            data['lema'] = data['close'].ewm(span=long_n).mean()

            # delete unnecessary columns
            del data['close'], data['adj close']

            # unset date as index - works better with mysql
            data = data.reset_index()

            # store indicators in dbo_EngineeredFeatures
            data.to_sql('dbo_engineeredfeatures', self.engine, if_exists=('replace' if ID == 1 else 'append'),
                        index=False, dtype={'date': sal.Date, 'instrumentid': sal.INT, 'rsi_14': sal.FLOAT,
                                            'macd_v': sal.FLOAT, 'macds_v': sal.FLOAT, 'boll_v': sal.FLOAT,
                                            'boll_ub_v': sal.FLOAT, 'boll_lb_v': sal.FLOAT, 'open_2_sma': sal.FLOAT,
                                            'volume_delta': sal.FLOAT, 'wcma': sal.FLOAT, 'scma': sal.FLOAT,
                                            'lcma': sal.FLOAT, 'ltrough': sal.FLOAT, 'lpeak': sal.FLOAT,
                                            'highfrllinelong': sal.FLOAT, 'medfrllinelong': sal.FLOAT,
                                            'lowfrllinelong': sal.FLOAT, 'strough': sal.FLOAT, 'speak': sal.FLOAT,
                                            'ktrough': sal.FLOAT, 'kpeak': sal.FLOAT, 'sema': sal.FLOAT,
                                            'mema': sal.FLOAT, 'lema': sal.FLOAT})
