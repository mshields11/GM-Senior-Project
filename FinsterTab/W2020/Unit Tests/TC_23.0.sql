-- Script ID : TC_23.0 --

use gmfsp_db;

SELECT *
       
FROM dbo_instrumentstatistics

where
     volume < 0
;

