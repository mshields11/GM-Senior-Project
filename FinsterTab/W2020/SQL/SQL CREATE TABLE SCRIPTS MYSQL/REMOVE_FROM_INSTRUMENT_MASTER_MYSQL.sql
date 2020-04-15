-- DATABASE SCRIPT TO REMOVE A STOCK SYMBOL --
select *
from dbo_instrumentmaster;

delete from dbo_instrumentmaster
where instrumentid = 1;


/*duplication check*/
select instrumentname , count(distinct instrumentname) Cnt
from dbo_instrumentmaster
group by instrumentname
having count(distinct instrumentname) >1
;