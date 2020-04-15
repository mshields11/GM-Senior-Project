-- Script ID : TC_34.0 --

use gmfsp_db;

drop table if exists `a`;
drop table if exists `b`;
drop table if exists `c`;

create temporary table a
SELECT 
 `signal`,
 count(`date`) cnt
FROM dbo_actionsignals
GROUP BY `signal`
;
create temporary table b
select 'SubTotal of Records Grouped by Signal Above' as `signal`,
       sum(cnt) as cnt
from a
;

select *
from a
UNION ALL
select *
from b
UNION ALL
select 
      'Total Records in ''actionsignals'' Table' as `signal`,
      count(*)
from dbo_actionsignals
;
