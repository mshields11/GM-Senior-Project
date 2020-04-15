-- Script ID : TC_21.0 --

use gmfsp_db;

SELECT *
       
FROM dbo_instrumentstatistics

where
	 high < low or
     high < `open` or
     high < `close`
;

