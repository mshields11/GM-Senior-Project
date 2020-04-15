/*PORTFOLIO_VALUE_IN_TABLEAU_FRL*/

use GMFSP_db;

select  r.`date`,
	    r.portfoliovalue,
        r.strategycode,
        im.instrumentname,
        i.close,
        i.`adj close` as adjclose
       
from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster     as im on r.instrumentid = im.instrumentid
        left outer join dbo_datedim              as d  on r.`date` = d.`date`
        left outer join dbo_instrumentstatistics as i  on r.instrumentid = i.instrumentid and r.date = i.date
        
where d.isholiday = 0 and
      d.weekend = 0 and
      r.strategycode = 'frl'
;

