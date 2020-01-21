-- TRACKING REQUESTED BY CLIENT --
use gmfsp_db;

-- NUMBERS FOR TRACKING --
select 
	   cast(current_date() as date) as RunDate,
	   a.forecastdate,
       a.instrumentid,
       m.instrumentname,
       a.forecastcloseprice,
       a.algorithmcode
       
from dbo_algorithmforecast as a
        left outer join dbo_instrumentmaster     as m on a.instrumentid = m.instrumentid
        left outer join dbo_datedim              as d on a.`forecastdate` = d.`date`
where 
      d.isholiday = 0 and 
      d.weekend = 0 and
      a.forecastdate > current_date()

order by a.forecastdate,
         a.instrumentid
;