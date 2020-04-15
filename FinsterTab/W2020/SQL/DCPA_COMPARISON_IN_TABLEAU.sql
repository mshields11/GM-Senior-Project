/*DCPA_COMPARISON_IN_TABLEAU_MYSQL*/

use GMFSP_db;

drop table if exists `main`;
drop table if exists `PrevForecast`;
drop table if exists `ImpForecast`;

create temporary table main
select distinct
  a.forecastdate
, a.instrumentid
, m.instrumentname
, i.`Adj Close`
, i.close

FROM dbo_algorithmforecast as a
		left outer join dbo_instrumentstatistics as i on a.forecastdate = i.date and a.instrumentid = i.instrumentid
        left outer join dbo_instrumentmaster     as m on a.instrumentid = m.instrumentid
		left outer join dbo_datedim              as d on a.forecastdate = d.date
where 
   a.forecastdate > cast(date_add(now(), interval -3 year) as date) and 
   d.weekend = 0 and 
   d.isholiday = 0
;


create temporary table PrevForecast
select
  a.forecastdate
, a.instrumentid
, a.forecastcloseprice as `Prev Forecast`

FROM dbo_datedim as d
       left outer join dbo_algorithmforecast as a on d.date = a.forecastdate
where 
   d.date > cast(date_add(now(), interval -3 year) as date) and 
   a.forecastcloseprice is not null and
   d.weekend = 0 and 
   d.isholiday = 0 and 
   a.algorithmcode in ('PricePredOld')
;

create temporary table ImpForecast
select
  a.forecastdate
, a.instrumentid
, a.forecastcloseprice as `Imp Forecast`

FROM dbo_datedim as d 
       left outer join dbo_algorithmforecast as a on d.date = a.forecastdate 
where a.forecastdate > cast(date_add(now(), interval -3 year) as date) and 
      a.forecastcloseprice is not null and
      d.weekend = 0 and 
      d.isholiday = 0 and 
      a.algorithmcode in ('PricePred')
;


select m.*, a.`Prev Forecast` , b.`Imp Forecast`
from m
       left outer join a on m.instrumentid = a.instrumentid and m.forecastdate = a.forecastdate
	   left outer join b on m.instrumentid = b.instrumentid and m.forecastdate = b.forecastdate
order by m.forecastdate desc, m.instrumentid asc