# import libraries to be used in this code module
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from math import sqrt
from statistics import stdev
import numpy as np
import xgboost as xgb
import calendar
import datetime
from datetime import timedelta, datetime
import FinsterTab.F2019.AccuracyTest
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

            # insert prediction into database
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


    def MSF1(self):
        #Queires the database to grab all of the Macro Economic Variable codes
        query = "SELECT macroeconcode FROM dbo_macroeconmaster WHERE activecode = 'A'"
        id = pd.read_sql_query(query, self.engine)
        id = id.reset_index(drop=True)

        #Queries the database to grab all of the instrument IDs
        query = 'SELECT instrumentid FROM dbo_instrumentmaster'
        id2 = pd.read_sql_query(query, self.engine)
        id2 = id2.reset_index(drop = True)

        # Sets value for number of datapoints you would like to work with
        n = 9

        # Getting Dates for Future Forecast#
        #Initialize the currentDate variable for use when grabbing the forecasted dates
        currentDate = datetime.today()

        # Creates a list to store future forecast dates
        date = []

        # This will set the value of count according to which month we are in, this is to avoid having past forecast dates in the list
        if (currentDate.month < 4):
            count = 0
        elif (currentDate.month < 7 and currentDate.month >= 4):
            count = 1
        elif (currentDate.month < 10 and currentDate.month >= 7):
            count = 2
        else:
            count = 3

        # Initialize a variable to the current year
        year = currentDate.year

        # Setup a for loop to loop through and append the date list with the date of the start of the next quarter
        # For loop will run n times, corresponding to amount of data points we are working with
        for i in range(n):

            # If the count is 0 then we are still in the first quarter
            if (count == 0):
                # Append the date list with corresponding quarter and year
                date.append(str(year) + "-03-" + "31")
                # Increase count so this date is not repeated for this year
                count += 1

            #Do it again for the next quarter
            elif (count == 1):
                date.append(str(year) + "-06-" + "30")
                count += 1

            #And again for the next quarter
            elif (count == 2):
                date.append(str(year) + "-09-" + "30")
                count += 1

            # Until we account for the last quarter of the year
            else:
                date.append(str(year) + "-12-" + "31")
                count = 0
                # Where we then incrament the year for the next iterations
                year = year + 1

        #Initializes a list for which we will eventually be storing all data to add to the macroeconalgorithm database table
        data = []

        #Create a for loop to iterate through all of the instrument ids
        for v in id2['instrumentid']:

            #Median_forecast will be a dictionary where the key is the date and the value is a list of forecasted prices
            median_forecast = {}
            #This will be used to easily combine all of the forecasts for different dates to determine the median forecast value
            for i in date:
                temp = {i: []}
                median_forecast.update(temp)

            # Initiailizes a variable to represent today's date, used to fetch forecast dates
            currentDate = str(datetime.today())
            # Applies quotes to current date so it can be read as a string
            currentDate = ("'" + currentDate + "'")


            #This query will grab quarterly instrument prices from between 2014 and the current date to be used in the forecasting
            query = "SELECT close, instrumentid FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                    "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                    "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN '2014-03-21' AND {} ) z " \
                    "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                    "MONTH(z.date) = 12)".format(v, currentDate)

            # Executes the query and stores the result in a dataframe variable
            df2 = pd.read_sql_query(query, self.engine)

            #This for loop iterates through the different macro economic codes to calculate the percent change for each macroeconomic variable
            for x in id['macroeconcode']:

                #Retrieves Relevant Data from Database

                query = 'SELECT * FROM dbo_macroeconstatistics WHERE macroeconcode = {}'.format('"' + str(x) + '"')     #Queries the DB to retrieeve the macoeconstatistics (currently just for GDP)
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
                    else:                                                                                               #If it is not the first iteration then calculate % change using previous row as normal
                        macrov = (macro['statistics'][i]-macro['statistics'][i - 1])/macro['statistics'][i - 1]
                        macroPercentChange['statistics'].iloc[i] = macrov * 100


                #Algorithm for forecast price
                G, S = DataForecast.calc(self, macroPercentChange, SP, n)                                               #Calculates the average GDP and S&P values for the given data points over n days and performs operations on GDP average


                # Setup a for loop to calculate the final forecast price and add data to the list variable data
                for i in range(n):
                    if x in [2, 3, 4]:
                        S = (S*((-G) + 1))
                    else:
                        S = (S*((G)+1))
                    #Once the forecast price is calculated append it to median_forecast list
                    median_forecast[date[i]].append(S)

            #Calculates the median value for each date using a list of prices forecasted by each individual macro economic variable
            forecast_prices = []
            for i in date:
                #Sort the forecasted prices based on date
                sorted_prices = sorted(median_forecast[i])
                #calculate the median forecasted price for each date
                if len(sorted_prices) % 2 == 0:
                    center = int(len(sorted_prices)/2)
                    forecast_prices.append(sorted_prices[center])
                else:
                    center = int(len(sorted_prices)/2)
                    forecast_prices.append((sorted_prices[center] + sorted_prices[center - 1])/2)

            #Set up a for loop to construct a list using variables associated with macroeconalgorithm database table
            for i in range(len(forecast_prices)):
                data.append([date[i], v, 'ALL', forecast_prices[i], 'MSF1', 0])

        # Convert data list to dataframe variable
        table = pd.DataFrame(data, columns=['forecastdate','instrumentid' , 'macroeconcode',
                                            'forecastprice', 'algorithmcode', 'prederror'])

        #Fill the database with the relevant information
        table.to_sql('dbo_macroeconalgorithmforecast', self.engine, if_exists=('replace'),
                     index=False)





    def MSF2(self):
        # Query to grab the macroeconcodes and macroeconnames from the macroeconmaster database table
        query = "SELECT macroeconcode, macroeconname FROM dbo_macroeconmaster WHERE activecode = 'A'"
        data = pd.read_sql_query(query, self.engine)

        # Query to grab the instrumentid and instrument name from the instrumentmaster database table
        query = 'SELECT instrumentid, instrumentname FROM dbo_instrumentmaster'
        data1 = pd.read_sql_query(query, self.engine)

        # Keys is a dictionary that will be used to store the macro econ code for each macro econ name
        keys = {}
        for i in range(len(data)):
            keys.update({data['macroeconname'].iloc[i]: data['macroeconcode'].iloc[i]})

        # ikeys is a dictionary that will be used to store instrument ids for each instrument name
        ikeys = {}
        for x in range(len(data1)):
            ikeys.update({data1['instrumentname'].iloc[x]: data1['instrumentid'].iloc[x]})

        # Vars is a dictionary used to store the macro economic variable percent change for each macro economic code
        vars = {}
        for i in data['macroeconname']:
            # Vars is only populated with the relevant macro economic variables (GDP, UR, IR, and MI)
            if(i == 'GDP' or i == 'Unemployment Rate' or i == 'Inflation Rate' or i == 'Misery Index'):
                d = {i: []}
                vars.update(d)

        # Result will hold the resulting forecast prices for each instrument ID
        result = {}
        for i in data1['instrumentid']:
            d = {i: []}
            result.update(d)


        # Weightings are determined through a function written in accuracytest.py
        # The weightings returned are used in the calculation below
        weightings = FinsterTab.F2019.AccuracyTest.create_weightings_MSF2(self.engine)

        n = 8

        # Getting Dates for Future Forecast #
        # --------------------------------------------------------------------------------------------------------------#
        # Initialize the currentDate variable for use when grabbing the forecasted dates
        currentDate = datetime.today()

        # Creates a list to store future forecast dates
        date = []

        # This will set the value of count according to which month we are in, this is to avoid having past forecast dates in the list
        if (currentDate.month < 4):
            count = 0
        elif (currentDate.month < 7 and currentDate.month >= 4):
            count = 1
        elif (currentDate.month < 10 and currentDate.month >= 7):
            count = 2
        else:
            count = 3

        # Initialize a variable to the current year
        year = currentDate.year

        # Setup a for loop to loop through and append the date list with the date of the start of the next quarter
        # For loop will run n times, corresponding to amount of data points we are working with
        for i in range(n):
            # If the count is 0 then we are still in the first quarter
            if (count == 0):
                # Append the date list with corresponding quarter and year
                date.append(str(year) + "-03-" + "31")
                # Increase count so this date is not repeated for this year
                count += 1

            # Do the same for the next quarter
            elif (count == 1):
                date.append(str(year) + "-06-" + "30")
                count += 1

            # And for the next quarter
            elif (count == 2):
                date.append(str(year) + "-09-" + "30")
                count += 1

            # Until we account for the last quarter of the year
            else:
                date.append(str(year) + "-12-" + "31")
                # Where we then reinitialize count to 0
                count = 0
                # And then incrament the year for the next iterations
                year = year + 1
        # --------------------------------------------------------------------------------------------------------------#

        # reinitializes currentDate to todays date, also typecasts it to a string so it can be read by MySQL
        currentDate = str(datetime.today())
        currentDate = ("'" + currentDate + "'")

        # For loop to loop through the macroeconomic codes to calculate the macro economic variable percent change
        for i in keys:
            # Check to make sure the macroeconcode we are working with is one of the relevant ones
            if i in vars:
                # Query to grab the macroeconomic statistics from the database using the relevant macro economic codes
                query = 'SELECT date, statistics, macroeconcode FROM dbo_macroeconstatistics WHERE macroeconcode = {}'.format('"' + keys[i] + '"')
                data = pd.read_sql_query(query, self.engine)

                # For loop to retrieve macro statistics and calculate percent change
                for j in range(n):
                    # This will grab the n+1 statistic to use to calculate the percent change to the n statistic
                    temp = data.tail(n + 1)
                    # This will grab the most recent n statistics from the query, as we are working only with n points
                    data = data.tail(n)

                    # For the first iteration we need to use the n+1th statistic to calculate percent change on the oldest point
                    if j == 0:
                        macrov = (data['statistics'].iloc[j] - temp['statistics'].iloc[0]) / temp['statistics'].iloc[0]
                        vars[i].append(macrov)
                    else:
                        macrov = (data['statistics'].iloc[j] - data['statistics'].iloc[j - 1]) / \
                                 data['statistics'].iloc[j - 1]
                        vars[i].append(macrov)

        # We now iterate through the instrument ids
        for x in ikeys:

            # This query will grab the quarterly instrument statistics from 2014 to now
            query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentid, ROW_NUMBER() OVER " \
                    "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                    "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN '2014-03-21' AND {} ) z " \
                    "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                    "MONTH(z.date) = 12)".format(ikeys[x], currentDate)

            # Then we execute the query and store the returned values in instrumentStats, and grab the last n stats from the dataframe as we are only using n datapoints
            instrumentStats = pd.read_sql_query(query, self.engine)
            instrumentStats = instrumentStats.tail(n)

            # Temp result will then store the resulting forecast prices throughout the calculation of n datapoints
            temp_result = []

            # This for loop is where the actual calculation takes place
            for i in range(n):
                stat = vars['GDP'][i] * weightings[ikeys[x]][0] - (vars['Unemployment Rate'][i] * weightings[ikeys[x]][1] + vars['Inflation Rate'][i] * weightings[ikeys[x]][2]) - (vars['Misery Index'][i] * vars['Misery Index'][i])
                stat = (stat * instrumentStats['close'].iloc[i]) + instrumentStats['close'].iloc[i]
                temp_result.append(stat)

            # We then append the resulting forcasted prices over n quarters to result, a dictionary where each
            # Instrument ID is mapped to n forecast prices
            result[ikeys[x]].append(temp_result)

        #Table will represent a temporary table with the data appended matching the columns of the macroeconalgorithmforecast database table
        table = []
        #This forloop will populate table[] with the correct values according to the database structure
        for i, k in result.items():
            cnt = 0
            for j in k:
                for l in range(n):
                    table.append([date[cnt], i, 'ALL', j[cnt], 'MSF2', 0])
                    cnt += 1

        #Once table is populated we then push it into the macroeconalgorithmforecast table
        table = pd.DataFrame(table, columns=['forecastdate','instrumentid' , 'macroeconcode',
                                            'forecastprice', 'algorithmcode', 'prederror'])
        table.to_sql('dbo_macroeconalgorithmforecast', self.engine, if_exists=('append'), index=False)


    def MSF3(self):
        # Query to grab the macroeconcodes and macroeconnames from the macroeconmaster database table
        query = "SELECT macroeconcode, macroeconname FROM dbo_macroeconmaster WHERE activecode = 'A'"
        data = pd.read_sql_query(query, self.engine)

        # Query to grab the instrumentid and instrument name from the instrumentmaster database table
        query = 'SELECT instrumentid, instrumentname FROM dbo_instrumentmaster'
        data1 = pd.read_sql_query(query, self.engine)

        # Keys is a dictionary that will be used to store the macro econ code for each macro econ name
        keys = {}
        for i in range(len(data)):
            keys.update({data['macroeconname'].iloc[i]: data['macroeconcode'].iloc[i]})

        # ikeys is a dictionary that will be used to store instrument ids for each instrument name
        ikeys = {}
        for x in range(len(data1)):
            ikeys.update({data1['instrumentname'].iloc[x]: data1['instrumentid'].iloc[x]})

        # Vars is a dictionary used to store the macro economic variable percent change for each macro economic code
        vars = {}
        for i in data['macroeconcode']:
            # Vars is only populated with the relevant macro economic variables (GDP, UR, IR, and MI)
            if(i == 'GDP' or i == 'COVI' or i == 'CPIUC' or i == 'FSI'):
                d = {i: []}
                vars.update(d)

        # Result will hold the resulting forecast prices for each instrument ID
        result = {}
        for i in data1['instrumentid']:
            d = {i: []}
            result.update(d)

        # Weightings are determined through a function written in accuracytest.py
        # The weightings returned are used in the calculation below
        weightings = FinsterTab.F2019.AccuracyTest.create_weightings_MSF3(self.engine)

        n = 8

        # Getting Dates for Future Forecast #
        # --------------------------------------------------------------------------------------------------------------#

        # Initialize the currentDate variable for use when grabbing the forecasted dates
        currentDate = datetime.today()

        # Creates a list to store future forecast dates
        date = []

        # This will set the value of count according to which month we are in, this is to avoid having past forecast dates in the list
        if (currentDate.month < 4):
            count = 0
        elif (currentDate.month < 7 and currentDate.month >= 4):
            count = 1
        elif (currentDate.month < 10 and currentDate.month >= 7):
            count = 2
        else:
            count = 3

        # Initialize a variable to the current year
        year = currentDate.year

        # Setup a for loop to loop through and append the date list with the date of the start of the next quarter
        # For loop will run n times, corresponding to amount of data points we are working with
        for i in range(n):
            # If the count is 0 then we are still in the first quarter
            if (count == 0):
                # Append the date list with corresponding quarter and year
                date.append(str(year) + "-03-" + "31")
                # Increase count so this date is not repeated for this year
                count += 1

            # Do the same for the next quarter
            elif (count == 1):
                date.append(str(year) + "-06-" + "30")
                count += 1

            # And for the next quarter
            elif (count == 2):
                date.append(str(year) + "-09-" + "30")
                count += 1

            # Until we account for the last quarter of the year
            else:
                date.append(str(year) + "-12-" + "31")
                # Where we then reinitialize count to 0
                count = 0
                # And then incrament the year for the next iterations
                year = year + 1
        # --------------------------------------------------------------------------------------------------------------#

        # reinitializes currentDate to todays date, also typecasts it to a string so it can be read by MySQL
        currentDate = str(datetime.today())
        currentDate = ("'" + currentDate + "'")

        # For loop to loop through the macroeconomic codes to calculate the macro economic variable percent change
        for i in keys:
            # Check to make sure the macroeconcode we are working with is one of the relevant ones
            if keys[i] in vars:
                # Query to grab the macroeconomic statistics from the database using the relevant macro economic codes
                query = 'SELECT date, statistics, macroeconcode FROM dbo_macroeconstatistics WHERE macroeconcode = {}'.format(
                    '"' + keys[i] + '"')
                data = pd.read_sql_query(query, self.engine)

                # For loop to retrieve macro statistics and calculate percent change
                for j in range(n):
                    # This will grab the n+1 statistic to use to calculate the percent change to the n statistic
                    temp = data.tail(n + 1)
                    # This will grab the most recent n statistics from the query, as we are working only with n points
                    data = data.tail(n)

                    # For the first iteration we need to use the n+1th statistic to calculate percent change on the oldest point
                    if j == 0:
                        macrov = (data['statistics'].iloc[j] - temp['statistics'].iloc[0]) / temp['statistics'].iloc[0]
                        vars[keys[i]].append(macrov)
                    else:
                        macrov = (data['statistics'].iloc[j] - data['statistics'].iloc[j - 1]) / \
                                 data['statistics'].iloc[j - 1]
                        vars[keys[i]].append(macrov)

        # We now iterate through the instrument ids
        for x in ikeys:

            # This query will grab the quarterly instrument statistics from 2014 to now
            query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentid, ROW_NUMBER() OVER " \
                    "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                    "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN '2014-03-21' AND {} ) z " \
                    "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                    "MONTH(z.date) = 12)".format(ikeys[x], currentDate)

            # Then we execute the query and store the returned values in instrumentStats, and grab the last n stats from the dataframe as we are only using n datapoints
            instrumentStats = pd.read_sql_query(query, self.engine)
            instrumentStats = instrumentStats.tail(n)

            # Temp result will then store the resulting forecast prices throughout the calculation of n datapoints
            temp_result = []

            # This for loop is where the actual calculation takes place
            for i in range(n):
                stat = vars['GDP'][i] * weightings[ikeys[x]][0] - (vars['COVI'][i] * weightings[ikeys[x]][1] + vars['FSI'][i] * weightings[ikeys[x]][2]) - \
                       (vars['CPIUC'][i] * vars['CPIUC'][i])
                stat = (stat * instrumentStats['close'].iloc[i]) + instrumentStats['close'].iloc[i]
                temp_result.append(stat)

            # We then append the resulting forcasted prices over n quarters to result, a dictionary where each
            # Instrument ID is mapped to n forecast prices
            result[ikeys[x]].append(temp_result)

        # Table will represent a temporary table with the data appended matching the columns of the macroeconalgorithmforecast database table
        table = []
        # This forloop will populate table[] with the correct values according to the database structure
        for i, k in result.items():
            cnt = 0
            for j in k:
                for l in range(n):
                    table.append([date[cnt], i, 'ALL', j[cnt], 'MSF3', 0])
                    cnt += 1

        # Once table is populated we then push it into the macroeconalgorithmforecast table
        table = pd.DataFrame(table, columns=['forecastdate','instrumentid' , 'macroeconcode',
                                            'forecastprice', 'algorithmcode', 'prederror'])
        table.to_sql('dbo_macroeconalgorithmforecast', self.engine, if_exists=('append'), index=False)

    # Calculation function used in MSF1
    def calc(self, df1, df2, n):
        G = 0
        S = 0
        # Calculates average Macro Variable % change and S&P closing prices over past n days
        for i in range(n):
            G = df1['statistics'][i] + G
            S = df2['close'][i] + S

        G = G / n
        S = S / n
        G = G / 100
        return (G*2),S




# END CODE MODULE