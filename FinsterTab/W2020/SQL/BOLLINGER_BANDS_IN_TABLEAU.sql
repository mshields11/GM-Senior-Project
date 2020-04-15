/*BOLLINGER_BANDS_IN_TABLEAU_MYSQL*/

use GMFSP_db;

SELECT 
       d.date,
       e.boll_v,
       e.boll_lb_v,
       e.boll_ub_v,
       m.InstrumentName,
	   i.`Adj Close`,
       i.`Close`,
       i.`Open`,
       i.Volume

FROM dbo_datedim as d
       left outer join dbo_instrumentstatistics as i on d.`date` = i.`date` 
       left outer join dbo_engineeredfeatures   as e on i.`date` = e.`date` and i.instrumentid = e.instrumentid
	   left outer join dbo_instrumentmaster     as m  on e.instrumentid = m.instrumentid

where d.weekend = 0 and 
      d.isholiday = 0 

order by d.date
;
