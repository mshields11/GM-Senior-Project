-- Script ID : TC_17.0 --

use gmfsp_db;

SELECT i.`date`,
       count(distinct i.strategycode) strcd,
       count(distinct i.signal)       signals
       
FROM dbo_actionsignals as i 
group by i.date
order by i.date
;

