-- Script ID : TC_29.0 --

use gmfsp_db;
SELECT  `date`,
        instrumentid,
        strategycode,
        cashonhand,
        portfoliovalue
       
FROM dbo_statisticalreturns
WHERE cashonhand > portfoliovalue
;

