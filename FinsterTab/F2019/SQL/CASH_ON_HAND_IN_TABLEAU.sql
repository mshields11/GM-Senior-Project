/*CASH_ON_HAND_IN_TABLEAU_MYSQL*/

use GMFSP_db;

select  r.date,
	    r.cashonhand,
        r.strategycode,
       im.instrumentname       
from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster as im on r.instrumentid = im.instrumentid
        left outer join dbo_datedim          as d  on r.date = d.date
where d.isholiday = 0 and
      d.weekend = 0 
order by r.date
;
