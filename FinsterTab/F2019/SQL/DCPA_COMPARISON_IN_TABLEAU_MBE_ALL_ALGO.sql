/*DCPA_COMPARISON_IN_TABLEAU_MBE_ALL_ALGO*/

use GMFSP_db;

drop table if exists `main`;
drop table if exists `allalgo`;

create temporary table main
select distinct
   i.`date`
 , i.instrumentid
 , m.instrumentname
 , i.`close`

FROM dbo_instrumentstatistics as i 
        left outer join dbo_instrumentmaster as m on i.instrumentid = m.instrumentid
		left outer join dbo_datedim          as d on i.`date` = d.`date`
        
where 
   i.`date` > cast(date_add(now(), interval -3 year) as date) and 
   d.weekend = 0 and 
   d.isholiday = 0
;


create temporary table allalgo
select a.*

FROM dbo_algorithmforecast as a 
      left outer join dbo_datedim as d on a.forecastdate = d.`date`
       
where 
     d.`date` > cast(date_add(now(), interval -3 year) as date) and 
     a.forecastcloseprice is not null and
     a.prederror is not null and
     d.weekend = 0 and 
     d.isholiday = 0
;


select a.*, 
       m.`close`
       
from allalgo as a
       left outer join main as m on a.instrumentid = m.instrumentid and 
                                    a.forecastdate = m.`date`
       
order by a.forecastdate,
         a.instrumentid
;
