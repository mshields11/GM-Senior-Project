-- Script ID : TC_35.0 --

use gmfsp_db;

SELECT min(cashonhand) mincoh,
	   max(cashonhand) maxcoh,
       max(portfoliovalue) maxportfolio
       
FROM dbo_statisticalreturns
;
