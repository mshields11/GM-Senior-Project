-- Script ID      : TC_3.0                                                              --

SELECT 
      instrumentid, 
	  count(instrumentname) cnt

FROM dbo_instrumentmaster as m

group by
		instrumentid
having count(instrumentname) > 1
;
