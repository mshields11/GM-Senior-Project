-- Script ID      : TC_11.0 --

SELECT 
      count(distinct forecastdate)  date,
      count(distinct algorithmcode) algo,
      avg(prederror)                prederror
      
FROM dbo_algorithmforecast as a
        left outer join dbo_datedim as d on a.forecastdate = d.date 
       
where forecastdate > current_date()
  and d.isholiday = 0
  and d.weekend = 0
;
