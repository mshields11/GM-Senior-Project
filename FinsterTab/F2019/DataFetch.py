# Import Libraries to be used in this code module
import pandas_datareader.data as dr               # pandas library for data manipulation and analysis
import pandas as pd
from datetime import datetime, timedelta, date    # library for date and time calculations
import sqlalchemy as sal                          # SQL toolkit, Object-Relational Mapper for Python
from pandas.tseries.holiday import get_calendar, HolidayCalendarFactory, GoodFriday  # calendar module to use a pre-configured calendar
import quandl


# Declaration and Definition of DataFetch class

class DataFetch:
    def __init__(self, engine, table_name):
        """
        Get raw data for each ticker symbol
        :param engine: provides connection to MySQL Server
        :param table_name: table name where ticker symbols are stored
        """
        self.engine = engine
        self.table_name = table_name
        self.datasource = 'yahoo'
        self.datalength = 1096    # last 3 years, based on actual calendar days of 365

    def get_datasources(self):
        """
        Method to query MySQL database for ticker symbols
        Use pandas read_sql_query function to pass query
        :return symbols: pandas data frame object containing the ticker symbols
        """
        query = 'SELECT * FROM %s' % self.table_name
        symbols = pd.read_sql_query(query, self.engine)
        return symbols

    def get_data(self, sources):
        """
        Get raw data from Yahoo! Finance for each ticker symbol
        Store data in MySQL database
        :param sources: provides ticker symbols of instruments being tracked
        """
        now = datetime.now()  # Date Variables

        start = datetime.now()-timedelta(days=self.datalength)  # get date value from 3 years ago
        end = now.strftime("%Y-%m-%d")

        # Cycle through each ticker symbol
        for n in range(len(sources)):
            # data will be a 2D Pandas Dataframe
            data = dr.DataReader(sources.iat[n, sources.columns.get_loc('instrumentname')], self.datasource, start, end)

            symbol = [sources['instrumentid'][n]] * len(data)      # add column to identify instrument id number
            data['instrumentid'] = symbol

            data = data.reset_index()      # no designated index - easier to work with mysql database


            # Yahoo! Finance columns to match column names in MySQL database.
            # Column names are kept same to avoid any ambiguity.
            # Column names are not case-sensitive.

            data.rename(columns={'Date': 'date', 'High': 'high', 'Low': 'low',  'Open': 'open', 'Close': 'close',
                                 'Adj Close': 'adj close', 'Volume': 'volume'}, inplace=True)

            data.sort_values(by=['date'])    # make sure data is ordered by trade date

        # send data to database
        # replace data each time program is run

            data.to_sql('dbo_instrumentstatistics', self.engine, if_exists=('replace' if n == 0 else 'append'),
                        index=False, dtype={'date': sal.Date, 'open': sal.FLOAT, 'high': sal.FLOAT, 'low': sal.FLOAT,
                                            'close': sal.FLOAT, 'adj close': sal.FLOAT, 'volume': sal.FLOAT})

    def get_calendar(self):
        """
        Get date data to track weekends, holidays, quarter, etc
        Store in database table dbo_DateDim
        """
        # drop data from table each time program is run
        truncate_query = 'TRUNCATE TABLE dbo_datedim'
        self.engine.execute(truncate_query)

        # 3 years of past data and up to 50 days of future forecasts
        begin = date.today() - timedelta(days=self.datalength)
        end = date.today() + timedelta(days=50)

        # list of US holidays
        cal = get_calendar('USFederalHolidayCalendar')  # Create calendar instance
        cal.rules.pop(7)   # Remove Veteran's Day
        cal.rules.pop(6)   # Remove Columbus Day
        tradingCal = HolidayCalendarFactory('TradingCalendar', cal, GoodFriday)   # Good Friday is OFF in Stock Market

        # new instance of class for STOCK MARKET holidays
        tradingHolidays = tradingCal()
        holidays = tradingHolidays.holidays(begin, end)

        # 3 years of past data
        day = date.today() - timedelta(days=self.datalength)

        while day < end:
            date_query = 'INSERT INTO dbo_datedim VALUES({},{},{},{},{},{})'   # insert query into the database
            day = day + timedelta(days=1)
            day_str = "'" + str(day) + "'"
            qtr = (int((day.month - 1) / 3)) + 1    # calculate quarter value

            # check if the day is a weekend?
            weekend = (1 if day.isoweekday() == 6 or day.isoweekday() == 7 else 0)

            # is day a holiday in US (NY day, MLK, President's Day, Good Friday, Memorial Day,
            # July 4, Labor Day, Thanksgiving, Christmas
            isholiday = (1 if day in holidays else 0)
            date_query = date_query.format(day_str, day.year, day.month, qtr, weekend, isholiday)
            self.engine.execute(date_query)

    def macroFetch(self):
        vars = ["FRBP/GDPPLUS", "USMISERY/INDEX"]                                                                       #Hardcoded Quandl keys used to retrieve data from Quandl
        for n in range (len(vars)):
            data = quandl.get(vars[n], authtoken="izGuybqHXPynXPY1Yz29", start_date="2001-12-31",                       #Retrieves data source corresponding to var[n]
                              end_date="2020-02-05")
            data = data.reset_index()                                                                                   #Resets the index so easier to work with

            if (len(data.columns) == 2):                                                                                #Checks if the number of columns is 2 as if so it is straightforward to draw the data
                data.rename(columns={'Date': 'date', 'Value': 'statistics'}, inplace=True)                              #Renames the columns of a 2 column table
                data.sort_values(by=['date'])                                                                           #Ensures the rows are ordered according to date
                data['instrumentID'] = n+1                                                                              #Adds a new column for the instrument ID
                data.to_sql('dbo_macrostatistics', self.engine, if_exists=('replace' if n == 0 else 'append'),          # Inserts the data into SQL
                            index=False, dtype={'date': sal.Date, 'statistics': sal.FLOAT, 'instrumentID': sal.INT})

            elif (len(data.columns) > 2):                                                                               #Checks if the number of columns is greater than 2 and if so it becomes more complex to pull
                colName = list(data.columns)                                                                            #Create a list of the column names from the data drawn
                colNewName = list(data.columns)                                                                         #Create a list of the column names but to be used to store new column names
                for x in range (len(colName)):                                                                          #Loops through the colName list
                    if x == 0:                                                                                          #The first column will always be date, so we set the first index value to 'date'
                        colNewName[x] = 'date'
                    else:                                                                                               #Otherwise we rename the columns statistic + the number of the column
                        colNewName[x] = 'statistics' + str(x)                                                           #This allows us to differentiate rows and dynamically insert the data into mySQL

                for l in range(len(colName)):
                    data.rename(columns={colName[l]: colNewName[l]}, inplace=True)                                      #This then actually renames the columns of the dataframe variable

                for j in range (len(colNewName)-1):                                                                     #For loop to loop through the dataframe variable and insert into mySQL
                    data1 = data[[colNewName[0], colNewName[j+1]]]                                                      #First we get the first column which is always date and then the first column we have yet to insert and assign it to a new dataframe variable
                    data1.rename(columns={'date': 'date', colNewName[j+1]: 'statistics'}, inplace=True)                 #We then dynamically rename the column name of the new dataframe variable
                    #print(data1)
                    data1['instrumentID'] = (j + n) + 1                                                                 #Then add an instrument ID column that adds the value of the indexing variable of the outer for loop to the indexing of the inner for loop + 1
                    data1.to_sql('dbo_macrostatistics', self.engine, if_exists=('replace' if n == 0 else 'append'),     #And finally insert the new dataframe variable into MySQL
                                index=False)

    # def GDPForecast(self):
    #     query = 'SELECT * FROM dbo_macrostatistics WHERE instrumentid = 1'
    #     df = pd.read_sql_query(query, self.engine)
    #     query = "SELECT close FROM dbo_instrumentstatistics WHERE instrumentid = 3 AND date BETWEEN '2014-03-21' AND '2016-03-31'"
    #     df2 = pd.read_sql_query(query, self.engine)
    #
    #     import datetime
    #     currentDate = datetime.datetime.now()
    #
    #     for year in range(currentDate.year, currentDate.year + 2):
    #
    #         firstQuarter  = str(year) + "-03-" + "31"
    #         secondQuarter = str(year) + "-06-" + "30"
    #         thirdQuarter  = str(year) + "-09-" + "30"
    #         fourthQuarter = str(year) + "-12-" + "31"
    #
    #         print(firstQuarter)
    #
    #         query = 'SELECT * FROM dbo_instrumentstatistics WHERE date = %s' % firstQuarter
    #         df = pd.read_sql_query(query, self.engine)
    #         print(df)

# END CODE MODULE