-- TRACKING TASK REQUESTED BY CLIENT --
use gmfsp_db;

-- CMA_FRL_EMA_MACD SUMMED SIGNALS --
select a.`date`, 
       a.instrumentid, 
       sum(a.`signal`) `signal`      

from dbo_actionsignals as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.`date` = d.`date`
        
where d.isholiday = 0 and 
	  d.weekend = 0 and 
      a.`date` = current_date()-1 and
      a.strategycode in ('frl','cma','ema','macd') and 
      a.instrumentid = 1

group by a.`date`, 
		 a.instrumentid
;

-- PREDICTIONS --
select a.forecastdate, 
       a.instrumentid,
       a.algorithmcode,
       a.forecastcloseprice 
       
from dbo_algorithmforecast as a
        left outer join dbo_instrumentmaster     as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim              as d on a.`forecastdate` = d.`date`
where 
      d.isholiday = 0 and 
      d.weekend = 0 and
      a.algorithmcode in ('PricePred', 'ARIMA') and
      a.instrumentid = 1 and
      a.forecastdate = '2019-12-05'
;


select a.date, a.instrumentid, a.signal, concat(a.strategycode,'=ARIMA+PrevGrp') as strategycode, i.`close`      
from dbo_actionsignals as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.`date` = d.`date`
        left outer join dbo_instrumentstatistics as i  on a.instrumentid = i.instrumentid and a.`date` = i.`date`
where d.isholiday = 0 and d.weekend = 0 and a.strategycode in ('algo')
;

-- SIGNALS --
select a.date, a.instrumentid, a.signal, a.strategycode, i.`close`      
from dbo_actionsignals as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.`date` = d.`date`
        left outer join dbo_instrumentstatistics as i  on a.instrumentid = i.instrumentid and a.`date` = i.`date`
where d.isholiday = 0 and d.weekend = 0 and a.strategycode in ('frl','cma','ema','macd')
and a.date = '2019-11-25' and a.instrumentid = 1
;




-- RETURNS --
select 
      cast(current_date() as date) as RunDate,
      a.*
from dbo_statisticalreturns as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.`date` = d.`date`
where 
      d.isholiday = 0 and
      d.weekend = 0 and
      a.strategycode = 'comb'
 ;




select i.date, i.close
from dbo_instrumentstatistics as i 
where i.instrumentid = 1
;

select a.`date`, 
       a.instrumentid, 
       sum(a.`signal`) `signal`      

from dbo_actionsignals as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.`date` = d.`date`
        
where d.isholiday = 0 and 
	  d.weekend = 0 and 
      a.`date` = current_date() and
      a.strategycode in ('arima') and 
      a.instrumentid = 1

group by a.`date`, 
		 a.instrumentid
;


