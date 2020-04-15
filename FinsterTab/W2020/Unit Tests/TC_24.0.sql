-- Script ID : TC_24.0 --

use gmfsp_db;

SELECT  min(date) startdate,
        max(date) enddate
       
FROM dbo_instrumentstatistics

;

