from sqlalchemy import true
import FinsterTab.F2019.DataForecast
from datetime import datetime
from dbEngine import DBEngine
import pandas as pd
import numpy

# Tests the accuracy of the old functions
def accuracy(self):
    query = 'SELECT * FROM dbo_algorithmmaster'
    algorithm_df = pd.read_sql_query(query, self.engine)

    query = 'SELECT * FROM dbo_instrumentmaster'
    instrument_master_df = pd.read_sql_query(query, self.engine)

    # Changes algorithm code
    for code in range(len(algorithm_df)):
        # Dynamic range for changing instrument ID starting at 1
        for ID in range(1, len(instrument_master_df) + 1):
            query = 'SELECT * FROM dbo_algorithmforecast AS a, dbo_instrumentstatistics AS b WHERE a.forecastdate = b.date AND' \
                    ' a.instrumentid = %d AND b.instrumentid = %d AND a.algorithmcode = "%s"' % (
                        ID, ID, algorithm_df['algorithmcode'][code])
            df = pd.read_sql_query(query, self.engine)
            count = 0
            # Calculates accuracy
            for x in range((len(df) - 1)):
                # Check if upward or downward trend
                if (df['close'][x + 1] > df['close'][x] and df['forecastcloseprice'][x + 1] > df['forecastcloseprice'][
                    x]) or \
                        (df['close'][x + 1] < df['close'][x] and df['forecastcloseprice'][x + 1] <
                         df['forecastcloseprice'][
                             x]):
                    count += 1


            # Populates absolute_percent_error with the calculated percent error for a specific data point
            absolute_percent_error = []
            for i in range(len(df)):
                absolute_percent_error.append(
                    abs((df['close'].loc[i] - df['forecastcloseprice'].loc[i]) / df['close'].loc[i]))

            # Calculate sum of percent error and find average
            average_percent_error = 0
            for i in absolute_percent_error:
                average_percent_error = average_percent_error + i
            average_percent_error = average_percent_error / len(df)

            # return the average percent error calculated above
            print("Average percent error for instrument: %d and algorithm: %s " % (ID, algorithm_df['algorithmcode'][code]), average_percent_error)
            #print('Algorithm:', algorithm_df['algorithmcode'][code])
            #print('instrumentid: %d' % ID, instrument_master_df['instrumentname'][ID - 1])
            #print('length of data is:', len(df))
            #print('number correct: ', count)
            d = len(df)
            b = (count / d) * 100
            #print('The accuracy is: %.2f%%\n' % b)

# Isolated tests for ARIMA as we where trying to determine why it was so accurate
def arima_accuracy(self):
    query = 'SELECT * FROM dbo_algorithmforecast AS a, dbo_instrumentstatistics AS b WHERE a.forecastdate = b.date AND' \
            ' a.instrumentid = 1 AND b.instrumentid = 1 AND a.algorithmcode = "ARIMA"'
    df = pd.read_sql_query(query, self.engine)
    df = df.tail(10)
    df = df.reset_index(drop=true)

    #print(df)
    arima_count = 0
    for x in range((len(df) - 1)):
        # Check if upward or downward trend
        if df['close'][x + 1] > df['close'][x] and df['forecastcloseprice'][x + 1] > df['forecastcloseprice'][x] \
                or (df['close'][x + 1] < df['close'][x] and df['forecastcloseprice'][x + 1] < df['forecastcloseprice'][x]):
            arima_count += 1
    #print(df['close'], df['forecastcloseprice'])
    #print(arima_count)
    #print(arima_count/len(df))

# Accuracy test for the new function MSF1
def MSF1_accuracy(self):
    # Queires the database to grab all of the Macro Economic Variable codes
    query = "SELECT macroeconcode FROM dbo_macroeconmaster WHERE activecode = 'A'"
    id = pd.read_sql_query(query, self.engine)
    id = id.reset_index(drop=True)

    # Queries the database to grab all of the instrument IDs
    query = 'SELECT instrumentid FROM dbo_instrumentmaster'
    id2 = pd.read_sql_query(query, self.engine)
    id2 = id2.reset_index(drop=True)

    # These are the date ranges we are working with
    # start_date represents the starting date for the forecasts and the end of the training dates
    start_date = "'2018-01-01'"
    # end_date represents the date for which the forecasting ends
    end_date = "'2020-01-01'"
    # train_date represents the date we start collecting the instrument statistics used to forecast prices
    train_date = "'2016-01-01'"

    # Bool to determine whether we append to dbo_tempvisualize or replace the values
    to_append = False

    # Create a for loop to iterate through all of the instrument ids
    for v in id2['instrumentid']:
        # Initializes a list for which we will eventually be storing all data to add to the macroeconalgorithm database table
        data = []
        # Data1 will be used to store the forecastdate, instrumentid, forecastprice, and algorithm code
        # It will be used to graph our backtested forecast against the actual instrument prices
        data1 = []

        # Getting Dates for Future Forecast as well as actual close prices for instrumentID#
        # We chose 2018 - 2020, to alter this date range simply change the dates in the 3rd line of the query for the dates you want to test on
        # Make sure they are valid dates as some instruments only have statistics that go back so far, check the instrument statistic table to figure out how far back each instrument goes
        query = "SELECT date, close FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(v, start_date, end_date)

        # instrument_stats will hold the closing prices and the dates for the dates we are forecasting for
        instrument_stats = pd.read_sql_query(query, self.engine)


        # We isolate the dates and closing prices into individual arrays to make them easier to work with
        date = []
        close = []
        for i in instrument_stats['date']:
            date.append(i)
        for i in instrument_stats['close']:
            close.append(i)

        # n will always correspond to the amount of dates, as the amount of dates is the number of data points being compared
        n = len(date)

        # Median_forecast will be a dictionary where the key is the date and the value is a list of forecasted prices
        median_forecast = {}

        # This disctionary will be used to easily combine all of the forecasts for different dates to determine the median forecast value
        for i in date:
            temp = {i: []}
            median_forecast.update(temp)


        # This query will grab quarterly instrument prices from between 2014 and the current date to be used in the forecasting
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentid, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(v, train_date, start_date)

        # Executes the query and stores the result in a dataframe variable
        df2 = pd.read_sql_query(query, self.engine)

        # This for loop iterates through the different macro economic codes to calculate the percent change for each macroeconomic variable
        for x in id['macroeconcode']:

            # Retrieves the most recent macro economic statistics prior to the date for which we are testing our algorithm
            query = "SELECT * FROM dbo_macroeconstatistics WHERE macroeconcode = {} and date <= {} ".format('"' + str(x) + '"', start_date)
            df = pd.read_sql_query(query, self.engine)
            macro = df.tail(n)
            SP = df2.tail(n)
            temp = df.tail(n + 1)
            temp = temp.reset_index()

            # Converts macro variables to precent change
            macroPercentChange = macro
            macro = macro.reset_index(drop=True)
            SP = SP.reset_index(drop=True)
            macroPercentChange = macroPercentChange.reset_index(drop=True)

            for i in range(0, n):
                if (i == 0):
                    macrov = (macro['statistics'][i] - temp['statistics'][i]) / temp['statistics'][i]
                    macroPercentChange['statistics'].iloc[i] = macrov * 100
                else:
                    macrov = (macro['statistics'][i] - macro['statistics'][i - 1]) / macro['statistics'][i - 1]
                    macroPercentChange['statistics'].iloc[i] = macrov * 100

            # Algorithm for forecast price
            G, S = calc(self, macroPercentChange, SP,n)  # Calculates the average GDP and S&P values for the given data points over n days and performs operations on GDP average



            # Setup a for loop to calculate the final forecast price and add data to the list variable data
            for i in range(n):
                if x in [2, 3, 4]:
                    S = (S * ((-G) + 1))
                else:
                    S = (S * ((G) + 1))
                # Once the forecast price is calculated append it to median_forecast list
                median_forecast[date[i]].append(S)

        # Calculates the median value for each date using a list of prices forecasted by each individual macro economic variable
        forecast_prices = []
        for i in date:
            # Sort the forecasted prices based on date
            sorted_prices = sorted(median_forecast[i])
            # calculate the median forecasted price for each date
            if len(sorted_prices) % 2 == 0:
                center = int(len(sorted_prices) / 2)
                forecast_prices.append(sorted_prices[center])
            else:
                center = int(len(sorted_prices) / 2)
                forecast_prices.append((sorted_prices[center] + sorted_prices[center - 1]) / 2)

        # Set up a for loop to construct a list using variables associated with macroeconalgorithm database table
        for i in range(len(forecast_prices)):
            data.append([date[i], v, 'ALL', forecast_prices[i], close[i], 'MSF1', 0])
            data1.append([date[i], v, forecast_prices[i], 'MSF1'])

        # Convert data list to dataframe variable
        df = pd.DataFrame(data, columns=['forecastdate', 'instrumentid', 'macroeconcode',
                                        'forecastcloseprice', 'close', 'algorithmcode', 'prederror'])


        df1 = pd.DataFrame(data1, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'algorithmcode'])
        df1.to_sql('dbo_tempvisualize', self.engine, if_exists=('replace' if not to_append else 'append'), index=False)
        to_append = True


        # Populates absolute_percent_error with the calculated percent error for a specific data point
        absolute_percent_error = []
        for i in range(n):
            absolute_percent_error.append(abs((df['close'].loc[i] - df['forecastcloseprice'].loc[i]) / df['close'].loc[i]))

        # Calculate sum of percent error and find average
        average_percent_error = 0
        for i in absolute_percent_error:
            average_percent_error = average_percent_error + i
        average_percent_error = average_percent_error / n

        count = 0
        # Calculates trend accuracy
        for x in range((len(df) - 1)):
            # Check if upward or downward trend
            if (df['close'][x + 1] > df['close'][x] and df['forecastcloseprice'][x + 1] > df['forecastcloseprice'][
                x]) or \
                    (df['close'][x + 1] < df['close'][x] and df['forecastcloseprice'][x + 1] <
                     df['forecastcloseprice'][
                         x]):
                count += 1

        length = len(df)
        trend_error = (count / length) * 100
        #print("Trend accuracy for %s for instrument %d is %.2f%%" % ('MSF1', v, trend_error))
        print("The average percent error for %s for instrument %d is %.2f%%" % ('MSF1', v, average_percent_error * 100))

        # return the average percent error calculated above




# This function is not currently used, it can be used to check the accuracy of MSF2 but will need set weightings
# The functions below this one will test the accuracy using a variety of weightings and choose the weightings with the best results
def MSF2_accuracy(self):
    n = 8

    #Gets the macro economic variables codes and names to loop through the inidividual macro variables
    query = "SELECT macroeconcode, macroeconname FROM dbo_macroeconmaster WHERE activecode = 'A'"
    data = pd.read_sql_query(query, self.engine)
    macrocodes = []
    indicators = {}
    for i in range(len(data['macroeconcode'])):
        macrocodes.append(data['macroeconcode'].loc[i])
        d = {data['macroeconcode'].loc[i]: []}
        indicators.update(d)

    #Gets the instrument ids to loop through the individual instruments
    query = 'SELECT instrumentid, instrumentname FROM dbo_instrumentmaster'
    data = pd.read_sql_query(query, self.engine)
    instrumentids = []
    for i in data['instrumentid']:
        instrumentids.append(i)

    # These are the date ranges we are working with
    # start_date represents the starting date for the forecasts and the end of the training dates
    start_date = "'2018-01-01'"
    # end_date represents the date for which the forecasting ends
    end_date = "'2020-01-01'"
    # train_date represents the date we start collecting the instrument statistics used to forecast prices
    train_date = "'2016-01-01'"


    #Loops through each instrument id to preform error calculations 1 instrument at a time
    for i in instrumentids:

        #Gets the instrument statistics to run through the function
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(i, train_date, start_date)
        train_data = pd.read_sql_query(query, self.engine)

        #Gets the instrument statistics to check against the forecast prices
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(i, start_date, end_date)
        check_data = pd.read_sql_query(query, self.engine)

        #Gets the dates for the future forecast prices so they match the instrument statistics
        dates = []
        for l in check_data['date']:
            dates.append(str(l))

        #Loops through the macro economic variable codes to calculate percent change
        for j in macrocodes:
            #Retrieves macro economic statistics for each macro variables
            query = "SELECT date, statistics, macroeconcode FROM dbo_macroeconstatistics WHERE macroeconcode = {} AND date <= {}".format('"' + j + '"', start_date)
            data = pd.read_sql_query(query, self.engine)

            # For loop to retrieve macro statistics and calculate percent change
            for k in range(n):
                temp = data.tail(n + 1)
                data = data.tail(n)
                if j == k:
                    macrov = (data['statistics'].iloc[k] - temp['statistics'].iloc[0]) / temp['statistics'].iloc[0]
                    indicators[j].append(macrov)
                else:
                    macrov = (data['statistics'].iloc[k] - data['statistics'].iloc[k - 1]) / data['statistics'].iloc[
                        k - 1]
                    indicators[j].append(macrov)

        #Preforms the actual calculations and stores them in an array called calculated forecast
        calculated_forecast = []
        for k in range(n):
            stat = indicators['GDP'][k] * 1 - (indicators['UR'][k] * 0 + indicators['IR'][k] * .5) - (
                        indicators['MI'][k] * indicators['MI'][k])
            stat = (stat * train_data['close'].iloc[k]) + train_data['close'].iloc[k]
            calculated_forecast.append(stat)


        #Creates and inserts the forecast dates, instrument ids, calculated forecast prices, and actual close prices into an array
        results = []
        for k in range(n):
            results.append([dates[k], i, calculated_forecast[k], check_data['close'].loc[k]])

        #Creates a dataframe out of the array created above
        df = pd.DataFrame(results, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'close'])
        #print(df)


        count = 0
        # Calculates accuracy
        percent_error = []
        temp_error = 0
        for x in range((len(df) - 1)):
            # Check if upward or downward trend
            if (df['close'][x + 1] > df['close'][x] and df['forecastcloseprice'][x + 1] > df['forecastcloseprice'][x]) or \
                    (df['close'][x + 1] < df['close'][x] and df['forecastcloseprice'][x + 1] < df['forecastcloseprice'][x]):
                count += 1
            temp_error = abs((df['close'][x] - df['forecastcloseprice'][x]))/df['close']

        #Percent Error calculation

        temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
        absolute_percent_error = [abs(ele) for ele in temp_error]
        percent_error.append(absolute_percent_error)

        if df['instrumentid'][i] == 1:
            gm_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            gm_absolute_percent_error = [abs(ele) for ele in gm_temp_error]

            #Calculate sum of percent error and find average

            gm_average_percent_error = sum(gm_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on GM stock is: ", gm_average_percent_error * 100, "%")

        if df['instrumentid'][i] == 2:
            pfe_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            pfe_absolute_percent_error = [abs(ele) for ele in pfe_temp_error]

            #Calculate sum of percent error and find average

            pfe_average_percent_error = sum(pfe_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on PFE stock is: ", pfe_average_percent_error * 100, "%")

        if df['instrumentid'][i] == 3:
            spy_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            spy_absolute_percent_error = [abs(ele) for ele in spy_temp_error]

            #Calculate sum of percent error and find average

            spy_average_percent_error = sum(spy_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on S&P 500 stock is: ", spy_average_percent_error * 100, "%")

        if df['instrumentid'][i] == 4:
            xph_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            xph_absolute_percent_error = [abs(ele) for ele in xph_temp_error]

            #Calculate sum of percent error and find average

            xph_average_percent_error = sum(xph_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on XPH stock is: ", xph_average_percent_error * 100, "%")

        if df['instrumentid'][i] == 5:
            carz_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            carz_absolute_percent_error = [abs(ele) for ele in carz_temp_error]

            #Calculate sum of percent error and find average

            carz_average_percent_error = sum(carz_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on CARZ index stock is: ", carz_average_percent_error * 100, "%")

        if df['instrumentid'][i] == 6:
            tyx_temp_error = (df['close'] - df['forecastcloseprice']) / df['close']
            tyx_absolute_percent_error = [abs(ele) for ele in tyx_temp_error]

            #Calculate sum of percent error and find average

            tyx_average_percent_error = sum(tyx_absolute_percent_error) / 8
            #print("Average percent error of MSF2 on TYX 30-YR bond is: ", tyx_average_percent_error * 100, "%")



        d = len(df)
        b = (count / d) * 100
        #Prints the trend accuracy
        #print('The accuracy for instrument %d: %.2f%%\n' % (i, b))


#Create weightings MSF2 runs the MSF2 algorithm for past dates and compares them to actual instrument prices, generating a percent error calculation
#We then iterate through several different weightings and we compare each percent error for each instrument and determine the weightings with the lowest percent error
def create_weightings_MSF2(self):

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

    #Vars is a dictionary used to store the macro economic variable percent change for each macro economic code
    vars = {}
    #Vars is only populated with the relevant macro economic variables (GDP, COVI, CPIUC, and FSI)
    for i in data['macroeconcode']:
        if (i == 'GDP' or i == 'UR' or i == 'IR' or i == 'MI'):
            d = {i: []}
            vars.update(d)

    #Weightings is used to store the best weightings for each instrument id which is returned to dataforecast and used for actual prediction
    weightings = {}

    #n represents the number of datapoints we are working with (represented in quarters)
    n = 8

    # These are the date ranges we are working with
    # start_date represents the starting date for the forecasts and the end of the training dates
    start_date = "'2018-01-01'"
    # end_date represents the date for which the forecasting ends
    end_date = "'2020-01-01'"
    # train_date represents the date we start collecting the instrument statistics used to forecast prices
    train_date = "'2016-01-01'"

    #For loop to loop through the macroeconomic codes to calculate the macro economic variable percent change
    for i in keys:
        #Check to make sure the macroeconcode we are working with is one of the relevant ones
        if keys[i] in vars:
            #Query to grab the macroeconomic statistics from the database using the relevant macro economic codes
            query = "SELECT date, statistics, macroeconcode FROM dbo_macroeconstatistics WHERE macroeconcode = {} AND date <= {}".format('"' + keys[i] + '"', start_date)
            data = pd.read_sql_query(query, self.engine)

            # For loop to retrieve macro statistics and calculate percent change
            for j in range(n):
                #This will grab the n+1 statistic to use to calculate the percent change to the n statistic
                temp = data.tail(n + 1)
                #This will grab the most recent n statistics from the query, as we are working only with n points
                data = data.tail(n)

                #For the first iteration we need to use the n+1th statistic to calculate percent change on the oldest point
                if j == 0:
                    macrov = (data['statistics'].iloc[j] - temp['statistics'].iloc[0]) / temp['statistics'].iloc[0]
                    vars[keys[i]].append(macrov)
                else:
                    macrov = (data['statistics'].iloc[j] - data['statistics'].iloc[j - 1]) / \
                             data['statistics'].iloc[j - 1]
                    vars[keys[i]].append(macrov)
    # We now iterate through the instrument ids
    for x in ikeys:

        # This query will grab the quarterly instrument statistics from 2016 to 2018
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentid, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(ikeys[x], train_date, start_date)

        # Then we execute the query and store the returned values in instrumentStats, and grab the last n stats from the dataframe as we are only using n datapoints
        instrumentStats = pd.read_sql_query(query, self.engine)
        instrumentStats = instrumentStats.tail(n)

        #Best weightings will be used to store the best weightings for each instrument
        best_weightings = [0, 0, 0]

        #Best avg error will be used to store the best average percent error for each isntrument
        best_avg_error = -1

        #Best trend error will be used to store the best trend error for each instrument
        best_trend_error = -1

        #Best forecast prices will be used to store the forecast prices for the best weightings to store them in a database for visual comparison later
        best_forecast_prices = []

        # We now iterate through all 3 different possible weightings
        for weight in numpy.arange(0, 2, .1):
            for uweight in numpy.arange(0, 1, .1):
                for iweight in numpy.arange(0, 1, .1):

                    # We intialize a list to store the resulting forecasted prices to compare in another function
                    stat_check = []

                    # This is the actual calculation of MSF3 where we store the result in stat_check to compare to actual instrument prices
                    for i in range(n):
                        stat = vars['GDP'][i] * weight - (vars['UR'][i] * uweight + vars['IR'][i] * iweight) - (
                                    vars['MI'][i] * vars['MI'][i])
                        stat = (stat * instrumentStats['close'].iloc[i]) + instrumentStats['close'].iloc[i]
                        stat_check.append(stat)

                    # We call to the weight check function using the list of forecasted prices, the current instrument id, the amount of datapoints we are working with, and the name of the function we are testing
                    # It then returns the average percent error and trend error for the forecasted prices, as well as the dates we are forecasting for so we can insert them into the visualize table
                    temp_avg_error, temp_trend_error, dates = weight_check(DBEngine().mysql_engine(), stat_check, ikeys[x], n, 'MSF2', start_date, end_date)

                    # Check to see if the best_avg_error has been initialized to a valid average percent error, if not then no average error or trend error has been calculated yet
                    if (best_avg_error < 0):
                        # If so store the average percent error, the best weightings, best trend error, and the resulting forecasted prices for comparison with other weightings
                        best_avg_error = temp_avg_error
                        best_weightings = [weight, uweight, iweight]
                        best_trend_error = temp_trend_error
                        best_forecast_prices = stat_check

                    # Otherwise check if the newly calculated average percent error is worse than the newly calculated one
                    elif (best_avg_error > temp_avg_error):
                        # And if so set the values for all the relevant variables
                        best_avg_error = temp_avg_error
                        best_weightings = [weight, uweight, iweight]
                        best_trend_error = temp_trend_error
                        best_forecast_prices = stat_check

        # Print statements to view the average percent error, trend error, and best weightings
        print("The lowest avg percent error is %.7f%% for instrumentID %d" % (best_avg_error, ikeys[x]), ' for function: MSF2')
        print("The weightings are: ", best_weightings, ' for function: MSF2')
        print('The trend error is: ', best_trend_error)

        # initializes weightings dictionary as the best weightings found for each instrument id
        weightings[ikeys[x]] = best_weightings

        # visual_comparisons will be used to store the past forecasted prices so we can visualize them compared to actual instrument prices on a graph
        visual_comparisons = []
        for k in range(n):
            visual_comparisons.append([dates[k], ikeys[x], best_forecast_prices[k], 'MSF2'])
        df1 = pd.DataFrame(visual_comparisons, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'algorithmcode'])
        df1.to_sql('dbo_tempvisualize', self.engine,
                    if_exists=('append'), index=False)

    # The weightings for each instrument ID are returned to dataforecast and used for prediction
    return weightings

#Create weightings MSF3 runs the MSF3 algorithm for past dates and compares them to actual instrument prices, generating a percent error calculation
#We then iterate through several different weightings and we compare each percent error for each instrument and determine the weightings with the lowest percent error
def create_weightings_MSF3(self):

    #Query to grab the macroeconcodes and macroeconnames from the macroeconmaster database table
    query = "SELECT macroeconcode, macroeconname FROM dbo_macroeconmaster WHERE activecode = 'A'"
    data = pd.read_sql_query(query, self.engine)

    #Query to grab the instrumentid and instrument name from the instrumentmaster database table
    query = 'SELECT instrumentid, instrumentname FROM dbo_instrumentmaster'
    data1 = pd.read_sql_query(query, self.engine)


    #Keys is a dictionary that will be used to store the macro econ code for each macro econ name
    keys = {}
    for i in range(len(data)):
        keys.update({data['macroeconname'].iloc[i]: data['macroeconcode'].iloc[i]})

    #ikeys is a dictionary that will be used to store instrument ids for each instrument name
    ikeys = {}
    for x in range(len(data1)):
        ikeys.update({data1['instrumentname'].iloc[x]: data1['instrumentid'].iloc[x]})

    #Vars is a dictionary used to store the macro economic variable percent change for each macro economic code
    vars = {}
    #Vars is only populated with the relevant macro economic variables (GDP, COVI, CPIUC, and FSI)
    for i in data['macroeconcode']:
        if (i == 'GDP' or i == 'COVI' or i == 'CPIUC' or i == 'FSI'):
            d = {i: []}
            vars.update(d)

    #Weightings is used to store the best weightings for each instrument id which is returned to dataforecast and used for actual prediction
    weightings = {}

    #n represents the number of datapoints we are working with (represented in quarters)
    n = 8

    # These are the date ranges we are working with
    # start_date represents the starting date for the forecasts and the end of the training dates
    start_date = "'2018-01-01'"
    # end_date represents the date for which the forecasting ends
    end_date = "'2020-01-01'"
    # train_date represents the date we start collecting the instrument statistics used to forecast prices
    train_date = "'2016-01-01'"

    #For loop to loop through the macroeconomic codes to calculate the macro economic variable percent change
    for i in keys:
        #Check to make sure the macroeconcode we are working with is one of the relevant ones
        if keys[i] in vars:
            #Query to grab the macroeconomic statistics from the database using the relevant macro economic codes
            query = "SELECT date, statistics, macroeconcode FROM dbo_macroeconstatistics WHERE macroeconcode = {} AND date <= {}".format('"' + keys[i] + '"', start_date)
            data = pd.read_sql_query(query, self.engine)

            # For loop to retrieve macro statistics and calculate percent change
            for j in range(n):
                #This will grab the n+1 statistic to use to calculate the percent change to the n statistic
                temp = data.tail(n + 1)
                #This will grab the most recent n statistics from the query, as we are working only with n points
                data = data.tail(n)

                #For the first iteration we need to use the n+1th statistic to calculate percent change on the oldest point
                if j == 0:
                    macrov = (data['statistics'].iloc[j] - temp['statistics'].iloc[0]) / temp['statistics'].iloc[0]
                    vars[keys[i]].append(macrov)
                else:
                    macrov = (data['statistics'].iloc[j] - data['statistics'].iloc[j - 1]) / \
                             data['statistics'].iloc[j - 1]
                    vars[keys[i]].append(macrov)
    #We now iterate through the instrument ids
    for x in ikeys:

        #This query will grab the quarterly instrument statistics from 2016 to 2018
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentid, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(ikeys[x], train_date, start_date)

        #Then we execute the query and store the returned values in instrumentStats, and grab the last n stats from the dataframe as we are only using n datapoints
        instrumentStats = pd.read_sql_query(query, self.engine)
        instrumentStats = instrumentStats.tail(n)

        #Best weightings will be used to store the best weightings for each instrument
        best_weightings = [0, 0, 0]

        #Best avg error will be used to store the best average percent error for each isntrument
        best_avg_error = -1

        #Best trend error will be used to store the best trend error for each instrument
        best_trend_error = -1

        #Best forecast prices will be used to store the forecast prices for the best weightings to store them in a database for visual comparison later
        best_forecast_prices = []

        #We now iterate through all 3 different possible weightings
        for weight in numpy.arange(0, 2, .1):
            for uweight in numpy.arange(0, 1, .1):
                for iweight in numpy.arange(0, 1, .1):

                    #We intialize a list to store the resulting forecasted prices to compare in another function
                    stat_check = []

                    #This is the actual calculation of MSF3 where we store the result in stat_check to compare to actual instrument prices
                    for i in range(n):
                        stat = vars['GDP'][i] * weight - (vars['COVI'][i] * uweight + vars['FSI'][i] * iweight) - (
                                    vars['CPIUC'][i] * vars['CPIUC'][i])
                        stat = (stat * instrumentStats['close'].iloc[i]) + instrumentStats['close'].iloc[i]
                        stat_check.append(stat)

                    #We call to the weight check function using the list of forecasted prices, the current instrument id, the amount of datapoints we are working with, and the name of the function we are testing
                    #It then returns the average percent error and trend error for the forecasted prices, as well as the dates we are forecasting for so we can insert them into the visualize table
                    temp_avg_error, temp_trend_error, dates = weight_check(DBEngine().mysql_engine(), stat_check, ikeys[x], n, 'MSF3', start_date, end_date)

                    #Check to see if the best_avg_error has been initialized to a valid average percent error, if not then no average error or trend error has been calculated yet
                    if (best_avg_error < 0):
                        #So store the average percent error, the best weightings, best trend error, and the resulting forecasted prices for comparison with other weightings
                        best_avg_error = temp_avg_error
                        best_weightings = [weight, uweight, iweight]
                        best_trend_error = temp_trend_error
                        best_forecast_prices = stat_check

                    #Otherwise check if the newly calculated average percent error is worse than the newly calculated one
                    elif (best_avg_error > temp_avg_error):
                        #And if so set the values for all the relevant variables
                        best_avg_error = temp_avg_error
                        best_weightings = [weight, uweight, iweight]
                        best_trend_error = temp_trend_error
                        best_forecast_prices = stat_check

        #Print statements to view the average percent error, trend error, and best weightings
        print("The lowest avg percent error is %.7f%% for instrumentID %d" % (best_avg_error, ikeys[x]), ' for function: MSF3')
        print("The weightings are: ", best_weightings, ' for function: MSF3')
        print('The trend error is: ',  best_trend_error)

        #initializes weightings dictionary as the best weightings found for each instrument id
        weightings[ikeys[x]] = best_weightings

        #visual_comparisons will be used to store the past forecasted prices so we can visualize them compared to actual instrument prices on a graph
        visual_comparisons = []
        for k in range(n):
            visual_comparisons.append([dates[k], ikeys[x], best_forecast_prices[k], 'MSF3'])

        df1 = pd.DataFrame(visual_comparisons, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'algorithmcode'])
        df1.to_sql('dbo_tempvisualize', self.engine, if_exists=('append'), index=False)

    #The weightings for each instrument ID are returned to dataforecast and used for prediction
    return weightings

#Function to check the forecasted prices of the weightings used in MSF2 and MSF3 create weight functions
def weight_check(self, calculated_forecast, instrumentid, n, function_name, start_date, end_date):
    #Retrieves the actual stock closing prices quarterly from 2018 to 2020 to compare with our calculated forecast prices
    query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
            "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
            "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN {} AND {} ) z " \
            "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
            "MONTH(z.date) = 12)".format(instrumentid, start_date, end_date)
    check_data = pd.read_sql_query(query, self.engine)

    # Gets the dates for the future forecast prices so they match the instrument statistics
    dates = []
    for l in check_data['date']:
        dates.append(str(l))

    # Creates and inserts the forecast dates, instrument ids, calculated forecast prices, and actual close prices into an array
    results = []
    for k in range(n):
        results.append([dates[k], instrumentid, calculated_forecast[k], check_data['close'].loc[k]])

    # Creates a dataframe out of the array created above
    df = pd.DataFrame(results, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'close'])

    #Populates absolute_percent_error with the calculated percent error for a specific data point
    absolute_percent_error = []
    for i in range(n):
        absolute_percent_error.append(abs((df['close'].loc[i] - df['forecastcloseprice'].loc[i]) / df['close'].loc[i]))

    # Calculate sum of percent error and find average
    average_percent_error = 0
    for i in absolute_percent_error:
        average_percent_error = average_percent_error + i
    average_percent_error = average_percent_error/n

    count = 0
    # Calculates trend accuracy
    for x in range((len(df) - 1)):
        # Check if upward or downward trend
        if (df['close'][x + 1] > df['close'][x] and df['forecastcloseprice'][x + 1] > df['forecastcloseprice'][
            x]) or \
                (df['close'][x + 1] < df['close'][x] and df['forecastcloseprice'][x + 1] <
                 df['forecastcloseprice'][
                     x]):
            count += 1

    length = len(df)
    trend_error = (count / length) * 100
    #print("Trend accuracy for %s is %.2f%%" % (function_name, b))
    #return the average percent error calculated above
    return average_percent_error, trend_error, dates

# Calc function used for calculation in MSF1
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
    return (G * 2), S

db_engine = DBEngine().mysql_engine()
#arima_accuracy(db_engine)
#accuracy(db_engine)

#MSF2_accuracy(db_engine)
#create_weightings(db_engine)

# instrument_master = 'dbo_instrumentmaster'
# forecast = DataForecast(db_engine, instrument_master)
# forecast.calculate_accuracy_train()


