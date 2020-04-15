-- Script ID : TC_25.0 --

use gmfsp_db;

SELECT  min(date) todaysdate,
        max(date) tendaysintofuture,
        datediff(cast(date_add(now(), interval 50 day) as date) , current_date()) daysdiff
       
FROM dbo_datedim

WHERE 
	 `date` >= current_date() and
     `date` < cast(date_add(now(), interval 50 day) as date)
;

