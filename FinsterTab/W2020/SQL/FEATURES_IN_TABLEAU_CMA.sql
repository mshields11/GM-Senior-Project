/*FEATURES_IN_TABLEAU_CMA*/

use GMFSP_db;

drop table if exists `main`;
drop table if exists `engfeat`;


create temporary table main
select
  d.`date`
, i.instrumentid
, m.instrumentname
, i.`Adj Close`
, i.`Close`

FROM dbo_datedim as d
		left outer join dbo_instrumentstatistics as i on d.date = i.date
        left outer join dbo_instrumentmaster     as m on i.instrumentid = m.instrumentid

where d.date > cast(date_add(now(), interval -3 year) as date) and 
      d.weekend = 0 and 
      d.isholiday = 0 and
      i.`close` is not null
;


create temporary table engfeat
select e.`date`, 
       e.instrumentid,
	   e.lcma,
       e.wcma,
       e.scma
FROM dbo_engineeredfeatures as e
       left outer join dbo_datedim as d on e.`date` = d.`date`

where d.`date` > cast(date_add(now(), interval -3 year) as date) and 
      d.weekend = 0 and 
      d.isholiday = 0
;



select 
  main.date,
  main.instrumentid,
  main.instrumentname,
  main.`adj close`,
  main.`close`,
  e.lcma,
  e.wcma,
  e.scma
  
from main
      left outer join engfeat as e on main.instrumentid = e.instrumentid and main.`date` = e.`date`             
;
