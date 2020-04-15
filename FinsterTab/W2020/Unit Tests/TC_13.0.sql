-- Script ID : TC_13.0 --

use gmfsp_db;

SELECT COUNT(*) as cnt
FROM information_schema.tables 
WHERE table_schema = 'gmfsp_db' and table_name like 'dbo_%'
;

