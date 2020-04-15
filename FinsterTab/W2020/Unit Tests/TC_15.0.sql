-- Script ID : TC_15.0 --

use gmfsp_db;

SELECT i.`date`,
       i.instrumentid,
       i.`close`
       
FROM dbo_instrumentstatistics as i 
where i.`close` is null or i.date is null
;

