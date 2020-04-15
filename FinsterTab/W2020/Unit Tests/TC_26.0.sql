-- Script ID : TC_26.0 --

use gmfsp_db;

SELECT  min(date) todaysdate,
        max(date) tendaysintofuture,
        datediff(cast(date_add(now(), interval 3 year) as date) , current_date()) daysdiff
       
FROM dbo_datedim

WHERE 
	 `date` <= current_date() and
     `date` >= cast(date_add(now(), interval -3 year) as date)
;

