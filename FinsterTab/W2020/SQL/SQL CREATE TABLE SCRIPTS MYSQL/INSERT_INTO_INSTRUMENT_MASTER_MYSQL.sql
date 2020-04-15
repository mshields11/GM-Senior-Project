
insert into dbo_instrumentmaster
values (1 , 'GM'   , 'Equity' , 'YAHOO'),
	   (2 , 'PFE'  , 'Equity' , 'YAHOO'),
	   (3 , 'SPY'  , 'Equity' , 'YAHOO'),
	   (4 , 'XPH'  , 'Equity' , 'YAHOO'),
	   (5 , 'CARZ' , 'Equity' , 'YAHOO')
;


/*duplication check*/
select instrumentname , count(distinct instrumentname) Cnt
from dbo_instrumentmaster
group by instrumentname
having count(distinct instrumentname) >1
;