-- Script ID : TC_27.0 --

use gmfsp_db;

SELECT  count(*) cnt,
        date
FROM dbo_datedim
GROUP by date
HAVING count(*) > 1
;

