# import libraries to be used in this code module
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from math import sqrt
from statistics import stdev
import numpy as np
import xgboost as xgb
import datetime
import sqlalchemy as sal
from sklearn.model_selection import train_test_split    # not used at this time
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import matplotlib.pyplot as plt


# class declaration and definition
class DataForecast:
    def __init__(self, engine, table_name):
        """
        Calculate historic one day returns and 10 days of future price forecast
        based on various methods
        Store results in dbo_AlgorithmForecast table
        :param engine: provides connection to MySQL Server
        :param table_name: table name where ticker symbols are stored
        """
        self.engine = engine
        self.table_name = table_name

    def calculate_forecast(self):
        """
        Calculate historic one day returns based on traditional forecast model
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast
        Improved forecast where we took out today's close price to predict today's price
        10 prior business days close prices are used as inputs to predict next day's price
        """

        # retrieve InstrumentMaster table from the database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'PricePred'"   # Master `algocode` for improved prediction from previous group, user created codes

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'PricePrediction'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:

            # remove all future prediction dates
            remove_future_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND prederror=0 AND ' \
                                  'instrumentid={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT forecastdate FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} ' \
                         'ORDER BY forecastdate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine) # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today if before market close at 4pm
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['forecastdate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} AND ' \
                               'forecastdate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT A.date, A.close, B.ltrough, B.lpeak, B.lema, B.lcma, B.highfrllinelong, ' \
                         'B. medfrllinelong, B.lowfrllinelong FROM dbo_instrumentstatistics AS A, '\
                         'dbo_engineeredfeatures AS B WHERE A.instrumentid=B.instrumentid AND A.date=B.date ' \
                         'AND A.instrumentid=%s ORDER BY Date ASC' %ID
            data = pd.read_sql_query(data_query, self.engine)

            # prediction formula inputs
            # IF THESE VALUES ARE CHANGED, ALL RELATED PREDICTIONS STORED IN THE DATABASE BECOME INVALID!
            sMomentum = 2
            lMomentum = 5
            sDev = 10
            ma = 10
            start = max(sMomentum, lMomentum, sDev, ma)

            # calculate prediction inputs
            data['sMomentum'] = data['close'].diff(sMomentum)
            data['lMomentum'] = data['close'].diff(lMomentum)
            data['stDev'] = data['close'].rolling(sDev).std()
            data['movAvg'] = data['close'].rolling(ma).mean()

            # first predictions can be made after 'start' number of days
            for n in range(start, len(data)):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

                # populate entire table if empty
                # or add new dates based on information in Statistics table
                """Look into this to add SMA"""
                if latest_date.empty or latest_date['forecastdate'][0] <= data['date'][n]:
                    if data['sMomentum'][n-1] >= 0 and data['lMomentum'][n-1] >= 0:
                        forecastClose = data['close'][n-1] + (2.576 * data['stDev'][n-1] / sqrt(sDev))
                    elif data['sMomentum'][n-1] <= 0 and data['lMomentum'][n-1] <= 0:
                        forecastClose = data['close'][n - 1] + (2.576 * data['stDev'][n - 1] / sqrt(sDev))
                    else:
                        forecastClose = data['movAvg'][n-1]
                    predError = 100 * abs(forecastClose - data['close'][n])/data['close'][n]
                    forecastDate = "'" + str(data['date'][n]) + "'"

                    #insert new prediction into table
                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

            # model for future price movements
            data['momentumA'] = data['close'].diff(10)
            data['lagMomentum'] = data['momentumA'].shift(5)

            fdate = "'" + str(data['date'][n]) + "'"
            # number of weekdays
            weekdays = 10
            # 3 weeks of weekdays
            days = 15
            forecast = []

            forecast_dates_query = 'SELECT date from dbo_datedim WHERE date > {} AND weekend=0 AND isholiday=0 ' \
                                   'ORDER BY date ASC LIMIT {}'.format(fdate, weekdays)
            future_dates = pd.read_sql_query(forecast_dates_query, self.engine)

            insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

            # Forecast close price tomorrow
            if data['sMomentum'][n] >= 0 and data['lMomentum'][n] >= 0:
                forecastClose = data['close'][n] + (2.576 * data['stDev'][n] / sqrt(sDev))
            elif data['sMomentum'][n] <= 0 and data['lMomentum'][n] <= 0:
                forecastClose = data['close'][n] + (2.576 * data['stDev'][n] / sqrt(sDev))
            else:
                forecastClose = data['movAvg'][n]
            predError = 0
            forecastDate = "'" + str(future_dates['date'][0]) + "'"
            insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
            self.engine.execute(insert_query)

            # forecast next 9 days
            # for i in range # of weekdays
            """Forecast for future from here down"""
            for i in range(1, len(future_dates)):

                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

                # if the momentum is negative
                if data['momentumA'].tail(1).iloc[0] < 0.00:

                    # Set Fibonacci extensions accordingly
                    data['fibExtHighNeg'] = data['lpeak'] - (
                            (data['lpeak'] - data['ltrough']) * 1.236)
                    data['fibExtLowNeg'] = data['lpeak'] - (
                            (data['lpeak'] - data['ltrough']) * 1.382)
                    highfrllinelong = data['highfrllinelong'].tail(1).iloc[0]

                    # Compute average over last 3 weeks of weekdays
                    avg_days = np.average(data['close'].tail(days))

                    # Compute standard Deviation over the last 3 weeks and the average.
                    std_days = stdev(data['close'].tail(days), avg_days)

                    # Compute Standard Error and apply to variable decrease
                    # assign CMA and EMA values
                    decrease = avg_days - (1.960 * std_days) / (sqrt(days))
                    data['fibExtHighPos'] = 0
                    data['fibExtLowPos'] = 0
                    l_cma = data['lcma'].tail(1)
                    l_cma = l_cma.values[0]
                    l_ema = data['lema'].tail(1)
                    l_ema = l_ema.values[0]

                    # Loop through each upcoming day in the week
                    for x in range(weekdays-1):

                        # Compare to current location of cma and frl values
                        # if CMA and FRL are lower than forecast
                        # Forecast lower with a medium magnitude
                        if decrease > l_cma or decrease >= (highfrllinelong + (highfrllinelong * 0.01)) \
                                or decrease > l_ema:
                            decrease -= .5 * std_days
                            forecast.append(decrease)

                        # If CMA and FRL are higher than forecast
                        # Forecast to rise with an aggressive magnitude
                        elif decrease <= l_cma and decrease <= (
                                highfrllinelong - (highfrllinelong * 0.01)) and decrease <= l_ema:
                            decrease += 1.5 * std_days
                            forecast.append(decrease)
                    x = x + 1

                # if the momentum is positive
                elif data['momentumA'].tail(1).iloc[0] > 0.00:
                    # ...Set fibonacci extensions accordingly
                    data['fibExtHighPos'] = data['lpeak'] + (
                            (data['lpeak'] - data['ltrough']) * 1.236)
                    data['fibExtLowPos'] = data['lpeak'] + (
                            (data['lpeak'] - data['ltrough']) * 1.382)
                    highfrllinelong = data['highfrllinelong'].tail(1).iloc[0]

                    # Compute average over last 3 weeks of weekdays
                    avg_days = np.average(data['close'].tail(days))

                    # Compute standard Deviation over the last 3 weeks and the average.
                    std_days = stdev(data['close'].tail(days), avg_days)

                    # Compute Standard Error and apply to variable increase
                    increase = avg_days + (1.960 * std_days) / (sqrt(days))
                    data['fibExtHighNeg'] = 0
                    data['fibExtLowNeg'] = 0
                    l_cma = data['lcma'].tail(1)
                    l_cma = l_cma.values[0]
                    l_ema = data['lema'].tail(1)
                    l_ema = l_ema.values[0]

                    for x in range(weekdays-1):

                        # Compare to current location of cma and frl values
                        # if CMA and FRL are lower than forecast
                        # Forecast lower with a normal magnitude
                        if increase > l_cma and increase >= (highfrllinelong - (highfrllinelong * 0.01)) \
                                and increase > l_ema:
                            increase -= std_days
                            forecast.append(increase)

                        # if CMA and FRL are lower than forecast
                        # Forecast lower with an aggressive magnitude
                        elif increase <= l_cma or increase <= (
                                highfrllinelong - (highfrllinelong * 0.01)) or increase <= l_ema:
                            increase += 1.5 * std_days
                            forecast.append(increase)

                forecastDateStr = "'" + str(future_dates['date'][i]) + "'"
                # Send the addition of new variables to SQL

                # predicted values error is 0 because the actual close prices for the future is not available
                predError = 0
                insert_query = insert_query.format(forecastDateStr, ID, forecast[i], algoCode, predError)
                self.engine.execute(insert_query)

    """Look into why warnings due to incorrect inputs"""
    def calculate_arima_forecast(self):
        """
        Calculate historic next-day returns based on ARIMA forecast model
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast
        To predict next day's value, prior 50 business day's close prices are used
        """

        # retrieve InstrumentsMaster table from database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'ARIMA'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'ARIMA'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:

            # remove all future prediction dates
            remove_future_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND prederror=0 AND ' \
                                  'instrumentid={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT forecastdate FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} ' \
                         'ORDER BY forecastdate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine)  # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today if before market close at 4pm
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['forecastdate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} AND ' \
                               'forecastdate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT date, close FROM dbo_instrumentstatistics WHERE instrumentid=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(data_query, self.engine)
            """Below here to look at for ARIMA warnings and to tweak"""
            # training data size
            # IF THIS CHANGES ALL PREDICTIONS STORED IN DATABASE BECOME INVALID!
            input_length = 10

            for n in range((input_length-1), len(data)):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

                # populate entire table if empty
                # or add new dates based on information in Statistics table

                if latest_date.empty or latest_date['forecastdate'][0] <= data['date'][n]:
                    training_data = data['close'][n-(input_length-1):n]
                    arima = ARIMA(training_data, order=(0,1,0))    # most suited order combination after many trials
                    fitted_arima = arima.fit(disp=-1)
                    forecastClose = data['close'][n] + fitted_arima.fittedvalues[n-1]

                    predError = 100 * abs(forecastClose - data['close'][n]) / data['close'][n]
                    forecastDate = "'" + str(data['date'][n]) + "'"

                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

            # training and test data set sizes
            forecast_length = 10
            forecast_input = 50

            # find ARIMA model for future price movements
            training_data = data['close'][-forecast_input:]
            model = ARIMA(training_data, order=(0, 1, 0))
            fitted = model.fit(disp=0)
            fc, se, conf = fitted.forecast(forecast_length, alpha=0.5)

            forecast_dates_query = 'SELECT date from dbo_datedim WHERE date > {} AND weekend=0 AND isholiday=0 ' \
                                   'ORDER BY date ASC LIMIT {}'.format(forecastDate, forecast_length)
            future_dates = pd.read_sql_query(forecast_dates_query, self.engine)

            # insert predition into database
            date = data['date'][n]
            for n in range(0, forecast_length):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'
                forecastClose = fc[n]
                predError = 0
                forecastDate = "'" + str(future_dates['date'][n]) + "'"
                insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                self.engine.execute(insert_query)

    def calculate_random_forest_forecast(self):
        """
        Calculate historic next-day returns based on Random Forest forecast model
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast table in the database
        """

        # retrieve InstrumentsMaster table from database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'RandomForest'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'RandomForest'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:
            # remove all future prediction dates - these need to be recalculated daily
            remove_future_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND prederror=0 AND ' \
                                  'instrumentid={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT forecastdate FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} ' \
                         'ORDER BY forecastdate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine)  # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today if before market close at 4pm
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['forecastdate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} AND ' \
                               'forecastdate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT date, close FROM dbo_instrumentstatistics WHERE instrumentid=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(data_query, self.engine)

            # training data size
            # IF THIS CHANGES ALL PREDICTIONS STORED IN DATABASE BECOME INVALID!
            input_length = 10

            for n in range((input_length - 1), len(data)):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

                # populate entire table if empty
                # or add new dates based on information in Statistics table
                if latest_date.empty or latest_date['forecastdate'][0] <= data['date'][n]:

                    # historical next-day random forest forecast
                    x_train = [i for i in range(input_length-1)]
                    y_train = data['close'][n - (input_length - 1):n]
                    x_test = [input_length-1]

                    x_train = np.array(x_train)
                    y_train = np.array(y_train)
                    x_test = np.array(x_test)
                    x_train = x_train.reshape(-1, 1)
                    x_test = x_test.reshape(-1, 1)

                    clf_rf = RandomForestRegressor(n_estimators=100)   # meta estimator with classifying decision trees
                    clf_rf.fit(x_train, y_train)                       # x and y train fit into classifier
                    forecastClose = clf_rf.predict(x_test)[0]
                    predError = 100 * abs(forecastClose-data['close'][n])/data['close'][n]   # standard MBE formula
                    forecastDate = "'" + str(data['date'][n]) + "'"

                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

            # training and test data set sizes
            forecast_length = 10
            forecast_input = 50

            # find Random Forest model for future price movements
            x_train = [i for i in range(forecast_input)]
            y_train = data['close'][-forecast_input:]
            x_test = [i for i in range(forecast_length)]

            x_train = np.array(x_train)
            y_train = np.array(y_train)
            x_test = np.array(x_test)
            x_train = x_train.reshape(-1, 1)
            x_test = x_test.reshape(-1, 1)

            clf_rf = RandomForestRegressor(n_estimators=100)
            clf_rf.fit(x_train, y_train)
            forecast = clf_rf.predict(x_test)

            forecast_dates_query = 'SELECT date from dbo_datedim WHERE date > {} AND weekend=0 AND isholiday=0 ' \
                                   'ORDER BY date ASC LIMIT {}'.format(forecastDate, forecast_length)
            future_dates = pd.read_sql_query(forecast_dates_query, self.engine)

            # insert prediction into database
            date = data['date'][n]
            for n in range(0, forecast_length):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'
                forecastClose = forecast[n]
                predError = 0
                forecastDate = "'" + str(future_dates['date'][n]) + "'"
                insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                self.engine.execute(insert_query)

    """Delete Forecast old"""
    def calculate_forecast_old(self):
        """
        Calculate historic one day returns based on traditional forecast model
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast
        This method was from Winter 2019 or before and is not really useful because
        it uses each day's actual close price (after the market closes) to predict that day's close price -
        it is only included for comparison with our improved `PricePred` algorithm`
        Prior 10 days close prices are used to predict the price for the next day
        """
        # retrieve InstrumentsMaster table from database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'PricePredOld'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'PricePredictionOld'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:

            # remove all future prediction dates
            remove_future_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND prederror=0 AND ' \
                                  'instrumentid={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT forecastdate FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} ' \
                         'ORDER BY forecastdate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine)  # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today when market closes at 4pm, not before that
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['forecastdate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_algorithmforecast WHERE algorithmcode={} AND instrumentid={} AND ' \
                               'forecastdate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT date, close FROM dbo_instrumentstatistics WHERE instrumentid=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(data_query, self.engine)

            # prediction formula inputs
            # IF THESE CHANGE ALL RELATED PREDICTIONS STORED IN DATABASE BECOME INVALID!
            momentum = 5
            sDev = 10
            ma = 10
            start = max(momentum, sDev, ma)

            # calculate prediction inputs
            data['momentum'] = data['close'].diff(momentum)
            data['stDev'] = data['close'].rolling(sDev).std()
            data['movAvg'] = data['close'].rolling(ma).mean()

            # first predictions can me made after 'start' number of days, its 10 days
            for n in range(start, len(data)):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'

                # populate entire table if empty
                # or add new dates based on information in Statistics table
                if latest_date.empty or latest_date['forecastdate'][0] <= data['date'][n]:
                    if data['momentum'][n] >= 0:
                        forecastClose = data['close'][n] + (2.576 * data['stDev'][n] / sqrt(sDev))
                    else:
                        forecastClose = data['close'][n] - (2.576 * data['stDev'][n] / sqrt(sDev))

                    predError = 100 * abs(forecastClose - data['close'][n]) / data['close'][n]
                    forecastDate = "'" + str(data['date'][n]) + "'"

                    # insert new prediction into table
                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

    """Use these forecast to generate buy sell signals"""
    def calculate_svm_forecast(self):
        """
        Calculate historic next-day returns based on SVM
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast
        Each prediction is made using prior 10 business days' close prices
        """
        # retrieve InstrumentsMaster table from database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'svm'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'SVM'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:
            # remove all future prediction dates - these need to be recalculated daily
            remove_future_query = 'DELETE FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND PredError=0 AND ' \
                                  'InstrumentID={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT ForecastDate FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND InstrumentID={} ' \
                         'ORDER BY ForecastDate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine)  # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today if before market close at 4pm
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['ForecastDate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND InstrumentID={} AND ' \
                               'ForecastDate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT Date, Close FROM dbo_InstrumentStatistics WHERE InstrumentID=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(data_query, self.engine)

            # training data size
            # IF THIS CHANGES ALL PREDICTIONS STORED IN DATABASE BECOME INVALID!
            input_length = 10

            for n in range((input_length - 1), len(data)):
                insert_query = 'INSERT INTO dbo_AlgorithmForecast VALUES ({}, {}, {}, {}, {})'

                # populate entire table if empty
                # or add new dates based on information in Statistics table
                if latest_date.empty or latest_date['ForecastDate'][0] <= data['Date'][n]:
                    # historical next-day random forest forecast
                    x_train = [i for i in range(input_length-1)]
                    y_train = data['Close'][n - (input_length - 1):n]
                    x_test = [input_length-1]

                    x_train = np.array(x_train)
                    y_train = np.array(y_train)
                    x_test = np.array(x_test)
                    x_train = x_train.reshape(-1, 1)
                    x_test = x_test.reshape(-1, 1)

                    clf_svr = SVR(kernel='rbf', C=1e3, gamma=0.1)
                    clf_svr.fit(x_train, y_train)
                    forecastClose = clf_svr.predict(x_test)[0]
                    predError = 100 * abs(forecastClose-data['Close'][n])/data['Close'][n]
                    forecastDate = "'" + str(data['Date'][n]) + "'"

                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

            # training and test data set sizes
            forecast_length = 10
            forecast_input = 50

            # Train Random Forest model for future price movements
            x_train = [i for i in range(forecast_input)]
            y_train = data['Close'][-forecast_input:]
            x_test = [i for i in range(forecast_length)]

            x_train = np.array(x_train)
            y_train = np.array(y_train)
            x_test = np.array(x_test)
            x_train = x_train.reshape(-1, 1)
            x_test = x_test.reshape(-1, 1)

            clf_svr = SVR(kernel='rbf', C=1e3, gamma=0.1)
            clf_svr.fit(x_train, y_train)
            forecast = clf_svr.predict(x_test)

            forecast_dates_query = 'SELECT date from dbo_datedim WHERE date > {} AND weekend=0 AND isholiday=0 ' \
                                   'ORDER BY date ASC LIMIT {}'.format(forecastDate, forecast_length)
            future_dates = pd.read_sql_query(forecast_dates_query, self.engine)

            # insert prediction into database
            for n in range(0, forecast_length):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'
                forecastClose = forecast[n]
                predError = 0
                forecastDate = "'" + str(future_dates['date'][n]) + "'"
                insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                self.engine.execute(insert_query)

    def calculate_xgboost_forecast(self):
        """
        Calculate historic next-day returns based on XGBoost
        and 10 days of future price forecast
        Store results in dbo_AlgorithmForecast
        Each prediction is made using the prior 50 days close prices
        """
        # retrieve InstrumentsMaster table from database
        query = 'SELECT * FROM {}'.format(self.table_name)
        df = pd.read_sql_query(query, self.engine)
        algoCode = "'xgb'"

        # add code to database if it doesn't exist
        code_query = 'SELECT COUNT(*) FROM dbo_algorithmmaster WHERE algorithmcode=%s' % algoCode
        count = pd.read_sql_query(code_query, self.engine)
        if count.iat[0, 0] == 0:
            algoName = "'xgb'"
            insert_code_query = 'INSERT INTO dbo_algorithmmaster VALUES({},{})'.format(algoCode, algoName)
            self.engine.execute(insert_code_query)

        # loop through each ticker symbol
        for ID in df['instrumentid']:
            # remove all future prediction dates - these need to be recalculated daily
            remove_future_query = 'DELETE FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND PredError=0 AND ' \
                                  'InstrumentID={}'.format(algoCode, ID)
            self.engine.execute(remove_future_query)

            # find the latest forecast date
            date_query = 'SELECT ForecastDate FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND InstrumentID={} ' \
                         'ORDER BY ForecastDate DESC LIMIT 1'.format(algoCode, ID)
            latest_date = pd.read_sql_query(date_query, self.engine)  # most recent forecast date calculation

            # if table has forecast prices already find the latest one and delete it
            # need to use most recent data for today if before market close at 4pm
            if not latest_date.empty:
                latest_date_str = "'" + str(latest_date['ForecastDate'][0]) + "'"
                delete_query = 'DELETE FROM dbo_AlgorithmForecast WHERE AlgorithmCode={} AND InstrumentID={} AND ' \
                               'ForecastDate={}'.format(algoCode, ID, latest_date_str)
                self.engine.execute(delete_query)

            # get raw price data from database
            data_query = 'SELECT Date, Close FROM dbo_InstrumentStatistics WHERE InstrumentID=%s ORDER BY Date ASC' % ID
            data = pd.read_sql_query(data_query, self.engine)

            # training data size
            # IF THIS CHANGES ALL RELATED PREDICTIONS STORED IN THE DATABASE BECOME INVALID!
            input_length = 10

            for n in range((input_length - 1), len(data)):
                insert_query = 'INSERT INTO dbo_AlgorithmForecast VALUES ({}, {}, {}, {}, {})'
                # populate entire table if empty
                # or add new dates based on information in Statistics table
                if latest_date.empty or latest_date['ForecastDate'][0] <= data['Date'][n]:
                    # historical next-day random forest forecast
                    x_train = [i for i in range(input_length-1)]
                    y_train = data['Close'][n - (input_length - 1):n]
                    x_test = [input_length-1]

                    x_train = np.array(x_train)
                    y_train = np.array(y_train)
                    x_test = np.array(x_test)
                    x_train = x_train.reshape(-1, 1)
                    x_test = x_test.reshape(-1, 1)

                    #XG BOOST Regressor with tree depth, subsample ratio of tree growth...etc.
                    xg_reg = xgb.XGBRegressor(max_depth=3, learning_rate=0.30, n_estimators=15,
                                              objective="reg:linear", subsample=0.5,
                                              colsample_bytree=0.8, seed=10)
                    xg_reg.fit(x_train, y_train)

                    forecastClose = xg_reg.predict(x_test)[0]
                    predError = 100 * abs(forecastClose-data['Close'][n])/data['Close'][n]
                    forecastDate = "'" + str(data['Date'][n]) + "'"

                    insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                    self.engine.execute(insert_query)

            # training and test data set sizes
            forecast_length = 10
            forecast_input = 50

            # find XG BOOST model for future price movements
            x_train = [i for i in range(forecast_input)]
            y_train = data['Close'][-forecast_input:]
            x_test = [i for i in range(forecast_length)]

            x_train = np.array(x_train)
            y_train = np.array(y_train)
            x_test = np.array(x_test)
            x_train = x_train.reshape(-1, 1)
            x_test = x_test.reshape(-1, 1)

            #XGBoost Regressor Predictions added 11/16/19
            xg_reg = xgb.XGBRegressor(max_depth=3, learning_rate=0.30, n_estimators=15,
                                      objective="reg:linear", subsample=0.5,
                                      colsample_bytree=0.8, seed=10)

            xg_reg.fit(x_train, y_train)
            forecast = xg_reg.predict(x_test)

            forecast_dates_query = 'SELECT date from dbo_datedim WHERE date > {} AND weekend=0 AND isholiday=0 ' \
                                   'ORDER BY date ASC LIMIT {}'.format(forecastDate, forecast_length)
            future_dates = pd.read_sql_query(forecast_dates_query, self.engine)

            # insert prediction into MySQL database
            # predError will be 0, there are no close prices available for future dates
            for n in range(0, forecast_length):
                insert_query = 'INSERT INTO dbo_algorithmforecast VALUES ({}, {}, {}, {}, {})'
                forecastClose = forecast[n]
                predError = 0
                forecastDate = "'" + str(future_dates['date'][n]) + "'"
                insert_query = insert_query.format(forecastDate, ID, forecastClose, algoCode, predError)
                self.engine.execute(insert_query)


    def MacroForecast(self):
        query = 'SELECT macroID FROM dbo_macroeconmaster'
        id = pd.read_sql_query(query, self.engine)
        id = id.reset_index(drop=True)

        query = 'SELECT instrumentID FROM dbo_instrumentmaster'
        id2 = pd.read_sql_query(query, self.engine)
        id2 = id2.reset_index(drop = True)

        n = 9                                                                                                           # Sets value for number of datapoints you would like to work with

        GDP = []
        U = []
        IR = []
        M = []
        Y = []
        data = []
        for v in id2['instrumentID']:
            currentDate = str(datetime.date.today())                                                                    #Initiailizes a variable to represent today's date, used to fetch forecast dates
            currentDate = ("'" + currentDate + "'")                                                                     #Applies quotes to current date so it can be read as a string
            query = "SELECT close, instrumentID FROM dbo_instrumentstatistics WHERE instrumentid = {} " \
                    "AND date BETWEEN '2014-03-21' AND {}".format(v, currentDate)                                       #Queries the DB to retrieve the intrumentstatistics (currently just for the S&P 500)


            query = "SELECT close, instrumentID FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                    "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                    "dbo_instrumentstatistics WHERE instrumentID = {} AND date BETWEEN '2014-03-21' AND {} ) z " \
                    "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                    "MONTH(z.date) = 12)".format(v, currentDate)

            df2 = pd.read_sql_query(query, self.engine)                                                                 #Executes the query and stores the result in a dataframe variable

            for x in id['macroID']:
                if x == 5:
                    break;
                #Retrieves Relevant Data from Database
                query = 'SELECT * FROM dbo_macroeconstatistics WHERE macroid = {}'.format(x)                            #Queries the DB to retrieeve the macoeconstatistics (currently just for GDP)
                df = pd.read_sql_query(query, self.engine)                                                              #Executes the query and stores the result in a dataframe variable
                macro = df.tail(n)                                                                                      #Retrieves the last n rows of the dataframe variable and stores it in GDP, a new dataframe variable
                SP = df2.tail(n)                                                                                        #Performs same operation, this is because we only want to work with a set amount of data points for now
                temp = df.tail(n+1)                                                                                     #Retrieves the nth + 1 row from the GDP tables so we can calculate percent change of the first GDP value
                temp = temp.reset_index()                                                                               #Resets the index so it is easy to work with

                #Converts macro variables to precent change
                macroPercentChange = macro                                                                              #Creates a new dataframe variable and initializes to the GDP table of n rows
                macro = macro.reset_index(drop=True)                                                                    #Resets the index of the GDP dataframe so it is easy to work with
                SP = SP.reset_index(drop=True)                                                                          #Same here
                macroPercentChange = macroPercentChange.reset_index(drop=True)                                          #And same here as well


                for i in range(0, n):                                                                                   #Creates a for loop to calculate the percent change

                    if (i == 0):                                                                                        #On the first iteration grab the extra row stored in temp to compute the first GDP % change value of the table
                        macrov = (macro['statistics'][i]-temp['statistics'][i])/temp['statistics'][i]
                        macroPercentChange['statistics'].iloc[i] = macrov * 100
                        if x == 1:
                            GDP.append([macro['date'].iloc[i], macrov, x])
                        elif x == 2:
                            U.append([macro['date'].iloc[i], macrov, x])
                        elif x == 3:
                            IR.append([macro['date'].iloc[i], macrov, x])
                        elif x == 4:
                            M.append([macro['date'].iloc[i], macrov, x])

                    else:                                                                                               #If it is not the first iteration then calculate % change using previous row as normal
                        macrov = (macro['statistics'][i]-macro['statistics'][i - 1])/macro['statistics'][i - 1]
                        macroPercentChange['statistics'].iloc[i] = macrov * 100
                        if x == 1:
                            GDP.append([macro['date'].iloc[i], macrov, x])
                        elif x == 2:
                            U.append([macro['date'].iloc[i], macrov, x])
                        elif x == 3:
                            IR.append([macro['date'].iloc[i], macrov, x])
                        elif x == 4:
                            M.append([macro['date'].iloc[i], macrov, x])
                        #testingPercent.append([macro['date'].iloc[i], macrov, x])

                currentDate = datetime.date.today()                                                                     #Reinitialize currentDate variable so it is a datetime variable rather than a string

                #Getting Dates for Future Forecast
                date = []                                                                                               #Creates a list to store future forecast dates
                count = 0
                if (currentDate.month < 4):                                                                             #This will set the value of count according to which month we are in, this is to avoid having past forecast dates in the list
                    count = 0
                elif (currentDate.month < 7 and currentDate.month >= 4):
                    count = 1
                elif (currentDate.month < 10 and currentDate.month >= 7):
                    count = 2
                else:
                    count = 3
                year = currentDate.year                                                                                 #Initialize a variable to the current year
                for i in range(n):                                                                                      #Setup a for loop to loop through and append the date list with the date of the start of the next quarter
                                                                                                                        #For loop will run n times, corresponding to amount of data points we are working with
                    if (count == 0):                                                                                    #If the count is 0 then we are still in the first quarter
                        date.append(str(year) + "-03-" + "31")                                                          #Append the date list with corresponding quarter and year
                        count += 1                                                                                      #Increase count so this date is not repeated for this year
                    elif (count == 1):                                                                                  #Do the same for the next quarter
                        date.append(str(year) + "-06-" + "30")
                        count += 1
                    elif (count == 2):                                                                                  #And for the next quarter
                        date.append(str(year) + "-09-" + "30")
                        count += 1
                    else:                                                                                               #Until we account for the last quarter of the year
                        date.append(str(year) + "-12-" + "31")
                        count = 0
                        year = year + 1                                                                                 #Where we then incrament the year for the next iterations


                #Algorithm for forecast price
                G, S = DataForecast.calc(self, macroPercentChange, SP, n)                                               #Calculates the average GDP and S&P values for the given data points over n days and performs operations on GDP average


                #Insert forecast values into database
                                                                                                                        #Initialize a list to store the values for each column
                for i in range(n):                                                                                      #Setsup a for loop to calculate the final forecast price and add data to the list variable data
                    S = (S * G) + S
                    #S = (S*((G+1)**n))
                    data.append([date[i], S, macroPercentChange['macroID'][i], SP['instrumentID'][i]])                  #Column organization setup according to dbo_macroeconforecast


        table = pd.DataFrame(data, columns=['date', 'forecast price', 'macroID',
                                            'instrumentID'])  # Convert data list to dataframe variable

        table.to_sql('dbo_macroeconforecast', self.engine, if_exists=('replace'),
                     index=False, dtype={'date': sal.Date, 'forecast price': sal.FLOAT,
                                         'macroid': sal.INT})  # Insert dataframe variable into SQL database



    def calc(self, df1, df2, n):
        G = 0
        S = 0
        for i in range(n):                                                                                              #Calculates average Macro Variable % change and S&P closing prices over past n days
            G = df1['statistics'][i] + G
            S = df2['close'][i] + S
        G = G / n
        S = S / n                                                                                                       #Divide percent change by 2
        G = G / 100                                                                                                     #Then convert from percent to number
        return G,S                                                                                                      #And return both values

    def MacroRegressionForecast(self):
        GDP = []
        U = []
        IR = []
        M = []
        Y = []

        M = pd.DataFrame(M, columns=['date', 'statistics', 'macroID'])
        GDP = pd.DataFrame(GDP, columns=['date', 'statistics', 'macroID'])
        IR = pd.DataFrame(IR, columns=['date', 'statistics', 'macroID'])
        U = pd.DataFrame(U, columns=['date', 'statistics', 'macroID'])

        #query = "SELECT close FROM dbo_instrumentstatistics WHERE instrumentid = 3 AND date = {}".format()
        for i in range(n):
            Y.append((GDP['statistics'].iloc[i]*1.9) - (U['statistics'].iloc[i]*.4 + IR['statistics'].iloc[i] *.3) - (M['statistics'].iloc[i] * M['statistics'].iloc[i]))
        print(Y)
        x_PR = np.array([i for i in range(len(Y))])


        #Polynomial regression
        x_axis = x_PR
        y_axis = Y

        df = pd.DataFrame({'date': x_axis, 'percent': y_axis})

        X = np.array(pd.to_datetime(df['date']), dtype=float)
        X = np.array(X)
        X = X.reshape(-1,1)
        y = np.array(df['percent'])

        X = np.array([i for i in range(len(Y))])
        X = X.reshape(-1, 1)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

        poly_reg = PolynomialFeatures(degree=3)
        X_poly = poly_reg.fit_transform(X)
        pol_reg = LinearRegression()
        pol_reg.fit(X_poly, y)
        print(poly_reg)
        plt.scatter(X, y, color='red')
        plt.plot(X, pol_reg.predict(poly_reg.fit_transform(X)), color='blue')
        plt.title('Prediction')
        plt.xlabel('Date')
        plt.ylabel('Percentage Change')
        plt.show()

# END CODE MODULE