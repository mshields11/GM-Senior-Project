-- Script ID : TC_37.0 --

use gmfsp_db;

select `date`,
       instrumentid,
       sum(`signal`) as sgnl
       
FROM dbo_actionsignals

WHERE strategycode in ('frl' , 'cma', 'ema', 'macd')
group by `date`,
         instrumentid
having sum(`signal`) < -5 or
       sum(`signal`) > 5
;
