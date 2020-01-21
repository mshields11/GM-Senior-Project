-- Script ID : TC_28.0 --

use gmfsp_db;

SELECT  date,
        instrumentid,
        strategycode,
        cashonhand
       
FROM dbo_statisticalreturns

WHERE cashonhand < 0
;

