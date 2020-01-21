/*STATISTICAL_RETURNS_IN_TABLEAU_MYSQL*/

use GMFSP_db;

drop table if exists `main`;
drop table if exists `sell`;
drop table if exists `buy`;
drop table if exists `lastday`;


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
      d.isholiday = 0
;


create temporary table buy
select d.date,
       coalesce(i.instrumentid, a.instrumentid)     as instrumentid, 
	   case when a.`signal` = 1 then i.`close` end  as Buy,
	   a.strategycode                               as strategycode

FROM dbo_datedim as d
      left outer join dbo_actionsignals        as a on d.date = a.date
      left outer join dbo_instrumentstatistics as i on d.date = i.Date and a.instrumentid = i.instrumentid
      
where d.weekend = 0 and 
      d.isholiday = 0 and
      a.`signal` = 1
;



create temporary table sell
select d.`date`, 
       a.instrumentid, 
	   case when a.`signal` = -1 then i.`close` end as Sell,
	   a.strategycode                               as strategycode

FROM dbo_datedim as d
		left outer join dbo_actionsignals        as a on d.`date` = a.`date`
		left outer join dbo_instrumentstatistics as i on a.`date` = i.`date` and a.instrumentid = i.instrumentid
        
where d.weekend = 0 and 
      d.isholiday = 0 and
      a.`signal` = -1
;



create temporary table lastday
select d.`date`, 
       a.instrumentid, 
       case when (a.`signal` = 0 or a.signal is null) then i.`close` end as Hold,
	   a.strategycode                                                    as strategycode

FROM dbo_actionsignals as a
		inner join (select max(date) as date 
					from dbo_instrumentstatistics
				    ) as n on a.`date` = n.`date`
		inner join dbo_instrumentstatistics as i on a.`date` = i.`date` and a.instrumentid = i.instrumentid 
        inner join dbo_datedim              as d on d.`date` = i.`date`
where d.weekend = 0 and 
      d.isholiday = 0
;


select r.* ,
       im.instrumentname,
       m.`close`,
       m.`adj close`,
       b.Buy,
       s.Sell,
       l.Hold
       
from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster     as im on r.instrumentid = im.instrumentid
        left outer join main    as m on r.instrumentid = m.instrumentid and r.`date` = m.`date` 
        left outer join buy     as b on r.instrumentid = b.instrumentid and r.`date` = b.`date` and r.strategycode = b.strategycode
        left outer join sell    as s on r.instrumentid = s.instrumentid and r.`date` = s.`date` and r.strategycode = s.strategycode
	    left outer join lastday as l on r.instrumentid = l.instrumentid and r.`date` = l.`date` and r.strategycode = l.strategycode

order by r.`date`
;
