/*FORECAST_IN_TABLEAU_SVM_ONLY_MYSQL*/

use GMFSP_db;

drop table if exists `svm`;
drop table if exists `main`;

create temporary table svm
select d.`date`, 
       a.instrumentid, 
	   a.forecastcloseprice,
	   a.algorithmcode,
       a.prederror,
       m.instrumentname

FROM dbo_datedim as d
      left outer join dbo_algorithmforecast    as a on d.`date` = a.forecastdate
      left outer join dbo_instrumentmaster     as m on a.instrumentid = m.instrumentid

where d.weekend = 0 and 
      d.isholiday = 0 and
      a.algorithmcode = 'svm'
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


select a.`date`, 
       a.instrumentid, 
	   a.forecastcloseprice, 
       a.algorithmcode, 
       a.prederror, 
       a.instrumentname, 
       m.adjclose, 
       m.`close`
from svm as a
      left outer join main as m  on a.instrumentid = m.instrumentid and a.`date` = m.`date`
;