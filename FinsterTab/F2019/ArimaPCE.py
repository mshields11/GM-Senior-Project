from sqlalchemy import true
import DataForecast
from datetime import datetime
from dbEngine import DBEngine
import pandas as pd
import numpy



def Arima_PCE(self):
    n = 8

    #Gets the instrument ids to loop through the individual instruments
    query = 'SELECT instrumentid, instrumentname FROM dbo_instrumentmaster'
    data = pd.read_sql_query(query, self.engine)
    instrumentids = []
    for i in data['instrumentid']:
        instrumentids.append(i)

    #Gets the algorithm codes
    query = "SELECT algorithmcode FROM dbo_algorithmforecast"
    data = pd.read_sql_query(query, self.engine)
    algorithmcode = []
    for k in data['algorithmcode']:
        algorithmcode.append(k)


    #Loops through each instrument id to preform error calculations 1 instrument at a time
    for i in instrumentids:

        #Gets the algorithm statistics to run through the function
        query = "SELECT forecastdate, forecastcloseprice, instrumentid FROM ( SELECT forecastdate, forecastcloseprice, instrumentID, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(forecastdate), MONTH(forecastdate) ORDER BY DAY(forecastdate) DESC) AS rowNum FROM " \
                "dbo_algorithmforecast WHERE instrumentid = {} AND algorithmcode = 'ARIMA' AND forecastdate BETWEEN '2018-01-01' AND '2020-01-01' ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.forecastdate) = 3 OR MONTH(z.forecastdate) = 6 OR MONTH(z.forecastdate) = 9 OR " \
                "MONTH(z.forecastdate) = 12)".format(i)
        algorithm_close = pd.read_sql_query(query, self.engine)

        #Gets the instrument statistics to check against the forecast prices
        query = "SELECT date, close, instrumentid FROM ( SELECT date, close, instrumentID, ROW_NUMBER() OVER " \
                "(PARTITION BY YEAR(date), MONTH(date) ORDER BY DAY(date) DESC) AS rowNum FROM " \
                "dbo_instrumentstatistics WHERE instrumentid = {} AND date BETWEEN '2018-01-01' AND '2020-01-01' ) z " \
                "WHERE rowNum = 1 AND ( MONTH(z.date) = 3 OR MONTH(z.date) = 6 OR MONTH(z.date) = 9 OR " \
                "MONTH(z.date) = 12)".format(i)
        real_close = pd.read_sql_query(query, self.engine)


        #Gets the dates for the future forecast prices so they match the instrument statistics
        dates = []
        for l in real_close['date']:
            dates.append(str(l))

        results = []
        for j in range(n):
            results.append([dates[j], i, algorithm_close['forecastcloseprice'].loc[j], real_close['close'].loc[j]])

        #Creates a dataframe out of the array created above
        arima_df = pd.DataFrame(results, columns=['forecastdate', 'instrumentid', 'forecastcloseprice', 'close'])



        #Percent Error calculation
        temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
        absolute_percent_error = [abs(ele) for ele in temp_error]
        average_percent_error = sum((absolute_percent_error)) / (8)

        if arima_df['instrumentid'][i] == 1:
            gm_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            gm_absolute_percent_error = [abs(ele) for ele in gm_temp_error]

            #Calculate sum of percent error and find average

            gm_average_percent_error = sum((gm_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on GM stock is: ", gm_average_percent_error, "%")

        if arima_df['instrumentid'][i] == 2:
            pfe_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            pfe_absolute_percent_error = [abs(ele) for ele in pfe_temp_error]

            #Calculate sum of percent error and find average

            pfe_average_percent_error = sum((pfe_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on PFE stock is: ", pfe_average_percent_error, "%")

        if arima_df['instrumentid'][i] == 3:
            spy_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            spy_absolute_percent_error = [abs(ele) for ele in spy_temp_error]

            #Calculate sum of percent error and find average

            spy_average_percent_error = sum((spy_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on S&P 500 stock is: ", spy_average_percent_error, "%")

        if arima_df['instrumentid'][i] == 4:
            xph_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            xph_absolute_percent_error = [abs(ele) for ele in xph_temp_error]

            #Calculate sum of percent error and find average

            xph_average_percent_error = sum((xph_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on XPH stock is: ", xph_average_percent_error, "%")

        if arima_df['instrumentid'][i] == 5:
            carz_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            carz_absolute_percent_error = [abs(ele) for ele in carz_temp_error]

            #Calculate sum of percent error and find average

            carz_average_percent_error = sum((carz_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on CARZ index stock is: ", carz_average_percent_error, "%")

        if arima_df['instrumentid'][i] == 6:
            tyx_temp_error = (arima_df['close'] - arima_df['forecastcloseprice']) / arima_df['close']
            tyx_absolute_percent_error = [abs(ele) for ele in tyx_temp_error]

            #Calculate sum of percent error and find average

            tyx_average_percent_error = sum((tyx_absolute_percent_error)) / (8)
            print("Average percent error of ARIMA on TYX 30-YR bond is: ", tyx_average_percent_error, "%")






db_engine = DBEngine().mysql_engine()
#arima_accuracy(db_engine)
#accuracy(db_engine)
#MSF2_accuracy(db_engine)
#create_weightings_MSF3(db_engine)
Arima_PCE(db_engine)

# instrument_master = 'dbo_instrumentmaster'
# forecast = DataForecast(db_engine, instrument_master)
# forecast.calculate_accuracy_train()
