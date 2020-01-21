/*FRL_FEATURES_IN_TABLEAU_MYSQL*/

use GMFSP_db;

DROP TABLE IF EXISTS `main`;
DROP TABLE IF EXISTS `engfeat`;


create temporary table main
SELECT 
       d.`date`,
       i.instrumentid,
       e.lpeak,
       e.ltrough,
       e.highfrllinelong,
       e.medfrllinelong,
       e.lowfrllinelong,
       m.instrumentname,
	   i.`Adj Close`,
       i.`Close`,
       i.`Open`,
       i.Volume

FROM dbo_datedim as d
       left outer join dbo_instrumentstatistics as i on d.`date` = i.`date` 
       left outer join dbo_engineeredfeatures   as e on i.`date` = e.`date` and i.instrumentid = e.instrumentid
	   left outer join dbo_instrumentmaster     as m  on e.instrumentid = m.instrumentid
       
where d.weekend = 0 and 
      d.isholiday = 0 and
      i.`close` is not null
;


create temporary table engfeat
select e.`date`, 
       e.instrumentid,
       e.lowfrllinelong,
       e.medfrllinelong,
       e.highfrllinelong,
	   e.strough,
       e.speak,
       e.ltrough,
       e.lpeak,
       e.ktrough,
       e.kpeak

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
  main.close,
  e.lowfrllinelong,
  e.medfrllinelong,
  e.highfrllinelong,
  e.strough,
  e.speak,
  e.ltrough,
  e.lpeak,
  e.ktrough,
  e.kpeak  

from main
      left outer join engfeat as e on main.instrumentid = e.instrumentid and main.`date` = e.`date`             
;
