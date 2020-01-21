/*BUY_SELL_SIGNALS_IN_TABLEAU_MYSQL_CMA*/

use GMFSP_db;

drop table if exists `main`;
drop table if exists `sell`;
drop table if exists `buy`;
drop table if exists `lastday`;
drop table if exists `engfeat`;

create temporary table main
select
  d.date
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
      a.StrategyCode = 'cma' and 
      a.`signal` = 1
;



create temporary table sell
select d.date, 
       a.instrumentid, 
	   case when a.`signal` = -1 then i.`close` end as Sell,
	   a.strategycode                               as strategycode

FROM dbo_datedim as d
		left outer join dbo_actionsignals        as a on d.date = a.date
		left outer join dbo_instrumentstatistics as i on a.date = i.date and a.instrumentid = i.instrumentid
where d.weekend = 0 and 
      d.isholiday = 0 and 
      a.StrategyCode = 'cma' and 
      a.`signal` = -1
;


create temporary table lastday
select d.`date`, 
       a.instrumentid, 
       case when a.`signal` = 0  then i.`close` end as Hold,
	   a.strategycode                               as strategycode

FROM dbo_datedim as d
		left outer join (
                         select max(date) as date,
                                instrumentid,
                                `signal`,
                                strategycode
                         from dbo_actionsignals
                         group by instrumentid,
							  	  `signal`,
                                  strategycode
                         ) as a on d.`date` = a.`date`
		left outer join dbo_instrumentstatistics as i on a.`date` = i.`date` and a.instrumentid = i.instrumentid
where d.weekend = 0 and 
      d.isholiday = 0 and 
      a.StrategyCode = 'cma' and
      a.`signal` = 0
;


create temporary table engfeat
select e.`date`, 
       e.instrumentid,
       e.lcma,
       e.scma,
       e.wcma
FROM dbo_engineeredfeatures as e
;



select 
  main.*, 
  coalesce(buy.strategycode,sell.strategycode) strategycode,
  buy.buy      as buy, 
  sell.sell    as sell,
  lastday.Hold as hold,
  engfeat.lcma,
  engfeat.scma,
  engfeat.wcma
  
from main
      left outer join sell    on main.instrumentid = sell.instrumentid    and main.`date` = sell.`date`
      left outer join buy     on main.instrumentid = buy.instrumentid     and main.`date` = buy.`date`
      left outer join lastday on main.instrumentid = lastday.instrumentid and main.`date` = lastday.`date`
      left outer join engfeat on main.instrumentid = engfeat.instrumentid and main.`date` = engfeat.`date`             
;
