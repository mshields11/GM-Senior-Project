-- Script ID : TC_14.0 --

use gmfsp_db;

SELECT i.date,
       i.instrumentid,
       i.close,
	   a.algorithmcode,
       a.forecastcloseprice

FROM dbo_instrumentstatistics as i 
        left outer join dbo_algorithmforecast as a  on i.instrumentid = a.instrumentid and 
                                                       i.`date` = a.forecastdate and 
                                                       a.algorithmcode ='randomforest'

order by i.date
;

