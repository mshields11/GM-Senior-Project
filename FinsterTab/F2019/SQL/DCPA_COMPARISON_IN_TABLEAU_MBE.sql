/*DCPA_COMPARISON_IN_TABLEAU_MYSQL_MBE*/

use GMFSP_db;

drop table if exists `m`;
drop table if exists `a`;
drop table if exists `b`;

/*InstrumentStatistics*/
create temporary table m
select distinct
  a.forecastdate
, a.instrumentid
, m.instrumentname
, i.`Adj Close`
, i.`close`

FROM 
	dbo_algorithmforecast as a
		left outer join dbo_instrumentstatistics as i on a.forecastdate = i.`date` and a.instrumentid = i.instrumentid
        left outer join dbo_instrumentmaster     as m on a.instrumentid = m.instrumentid
		left outer join dbo_datedim              as d on a.forecastdate = d.`date`
where 
      a.forecastdate > cast(date_add(now(), interval -3 year) as date) and 
      d.weekend = 0 and 
      d.isholiday = 0
;


/*Prev Forecast MAPE*/
create temporary table a
select
  a.forecastdate
, a.instrumentid
, a.prederror as `Prev Forecast MAPE`
FROM dbo_algorithmforecast as a
		left outer join dbo_datedim as d on a.forecastdate = d.`date`
where 
    a.forecastdate > cast(date_add(now(), interval -3 year) as date) and 
    a.forecastcloseprice is not null and 
    d.weekend = 0 and 
    d.isholiday = 0 and 
    a.algorithmcode = 'PricePredOld'
;


/*Imp Forecast MAPE*/
create temporary table b

;

select
  a.forecastdate
, a.instrumentid
, a.prederror as `Imp Forecast MAPE`

FROM dbo_algorithmforecast as a
       left outer join dbo_datedim as d on a.forecastdate = d.date
where a.forecastdate > cast(date_add(now(), interval -3 year) as date) and 
      a.forecastcloseprice is not null and 
      d.weekend = 0 and 
      d.isholiday = 0 and 
      a.algorithmcode = 'PricePred'
select m.*, 
       a.`Prev Forecast MAPE`, 
       b.`Imp Forecast MAPE`
from m
       left outer join a on m.instrumentid = a.instrumentid and m.forecastdate = a.forecastdate
	   left outer join b on m.instrumentid = b.instrumentid and m.forecastdate = b.forecastdate

order by m.forecastdate desc, m.instrumentid asc
;