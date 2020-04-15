-- TRACKING TASK REQUESTED BY CLIENT --
use gmfsp_db;

select 
      a.instrumentid,
      a.date,
      m.instrumentname,
      a.close
      
from dbo_instrumentstatistics as a
        left outer join dbo_instrumentmaster as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim          as d on a.date = d.date
where 
      d.isholiday = 0 and
      d.weekend = 0 and
      a.`date` = current_date()

order by a.instrumentid
