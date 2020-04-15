-- Script ID : TC_30.0 --

use gmfsp_db;

SELECT  TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE

FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_name = 'dbo_statisticalreturns' AND 
	  COLUMN_NAME = 'positionsize'
;

