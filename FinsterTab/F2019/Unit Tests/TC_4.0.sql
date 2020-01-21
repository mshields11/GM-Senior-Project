-- Script ID      : TC_4.0 --

SELECT
      a.instrumentid,
      count(distinct a.forecastdate) cnt

FROM dbo_algorithmforecast as a

where a.forecastdate > current_date()

group by
       a.instrumentid
;
