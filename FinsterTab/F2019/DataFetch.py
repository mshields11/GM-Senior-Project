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
        data = quandl.get("USMISERY/INDEX", authtoken="izGuybqHXPynXPY1Yz29", start_date="2001-12-31", end_date="2019-12-31")                                   #Fetches data from quandl, stores in a dataframe
        data.rename(columns={'Date': 'date', 'Uneployment Rate': 'unemployment', 'Inflation Rate': 'inflation', 'Misery Index' : 'misery'}, inplace=True)       #Renames the columns of the dataframe variable storing data from quandl
        data = data.reset_index()                                                                                                                               #Resets the index so it is easier to work with
        data.sort_values(by=['Date'])                                                                                                                           #Ensures the values are sorted by date
        data.to_sql('macrovar', self.engine, if_exists=('replace'),                                                                                             #Inserts the data into SQL
                    index=False, dtype={'date': sal.Date, 'unemployment': sal.FLOAT, 'inflation': sal.FLOAT, 'misery': sal.FLOAT})

        #SQL Code to truncate datetime to just date (Add a query for this here)
        #ALTER TABLE `gmfsp_db`.`macrovar2`
                #CHANGE COLUMN `Date` `Date` DATE NULL DEFAULT NULL ;

        #Unemplyment Old Code:  data = quandl.get("FRED/NROUST", authtoken="izGuybqHXPynXPY1Yz29", start_date="2001-12-31", end_date="2019-12-31")
                        #data.rename(columns={'Date': 'date', 'Value' : 'unemployment'}, inplace=True)
                        #data = data.reset_index()
                        #data.sort_values(by=['Date'])
                        #data.to_sql('macrovar', self.engine, if_exists=('replace'),
                            #index=False, dtype={'date': sal.Date, 'gdp': sal.FLOAT})



# END CODE MODULE