-- Script ID  :  TC_7.0 -- 

use GMFSP_db;

SELECT  algorithmcode AlgorithmName, 
		avg(forecastcloseprice) ForecastPriceAverage,
        min(forecastdate) NextDate,
        max(forecastdate) LastDate,
        count(distinct forecastdate) NumOfDaysAhead
FROM dbo_algorithmforecast
WHERE forecastdate > current_date()
GROUP BY algorithmcode

