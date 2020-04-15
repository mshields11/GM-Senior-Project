-- Script ID : TC_18.0 --

use gmfsp_db;

SELECT i.`date`,
       count(distinct i.instrumentid) instid,
       count(distinct i.signal)       signals
       
FROM dbo_actionsignals as i 
group by i.`date`
having count(distinct i.instrumentid) <> 5
;

