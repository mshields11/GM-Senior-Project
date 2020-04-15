/*FORECAST_IN_TABLEAU_ALL_ALGORITHMS*/

use GMFSP_db;

drop table if exists `allalgo`;
drop table if exists `main`;

create temporary table allalgo
select d.`date`, 
       a.instrumentid, 
	   a.forecastcloseprice,
	   a.algorithmcode,
       a.prederror,
       m.instrumentname

FROM dbo_datedim as d
      left outer join dbo_algorithmforecast as a on d.`date` = a.forecastdate
      left outer join dbo_instrumentmaster  as m on a.instrumentid = m.instrumentid

where d.weekend = 0 and 
      d.isholiday = 0 and
      a.algorithmcode in ('arima','PricePred','RandomForest','PricePredOld','svm','xgb')
;

create temporary table main
select d.`date`, 
       i.instrumentid, 
	   i.`Adj Close`         as adjclose,
       i.`close`
       
FROM dbo_datedim as d
      left outer join dbo_instrumentstatistics as i on d.`date` = i.`date`

where d.weekend = 0 and 
      d.isholiday = 0
;


select 
	   a.`date`, 
       a.instrumentid, 
       a.forecastcloseprice, 
       a.algorithmcode, 
       a.prederror, 
       a.instrumentname, 
       m.adjclose, 
       m.`close`
from allalgo as a
      left outer join main as m  on a.instrumentid = m.instrumentid and 
                                    a.`date` = m.`date`
;
