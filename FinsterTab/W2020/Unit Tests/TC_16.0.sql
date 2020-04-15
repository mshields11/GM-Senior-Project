-- Script ID : TC_16.0 --

use gmfsp_db;

SELECT i.`date`,
       count(distinct i.signal)  signals
       
FROM dbo_actionsignals as i 
group by i.date
order by i.date
;

