-- Script ID : TC_22.0 --

use gmfsp_db;

SELECT *
       
FROM dbo_instrumentstatistics

where
	 low > high or
     low > `open` or
     low > `close`
;

