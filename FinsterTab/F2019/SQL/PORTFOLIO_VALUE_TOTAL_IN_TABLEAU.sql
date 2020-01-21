/*PORTFOLIO_VALUE_TOTAL_IN_TABLEAU*/

use GMFSP_db;

select r.date ,
       r.instrumentid,
       r.cashonhand,
       r.portfoliovalue,
       r.positionsize,
       case when r.strategycode = 'COMB' then 'Custom' else r.strategycode end as strategycode,
       im.instrumentname       

from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster     as im on r.instrumentid = im.instrumentid

where r.date = (select max(date) from dbo_statisticalreturns)
;
