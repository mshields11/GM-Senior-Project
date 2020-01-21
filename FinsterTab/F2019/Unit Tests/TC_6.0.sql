-- Script ID : TC_6.0 -- 

use GMFSP_db;
 
SELECT  instrumentid InstrumentID, 
		avg(forecastcloseprice) ForecastPriceAverage, 
        min(forecastdate) NextDate,
        max(forecastdate) LastDate,
        count(distinct forecastdate) NumOfDaysAhead
FROM dbo_algorithmforecast
WHERE forecastdate > current_date()
GROUP BY instrumentid


