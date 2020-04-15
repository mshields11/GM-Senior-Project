
use GMFSP_db;

DROP TABLE IF EXISTS dbo_actionsignals;
create table dbo_actionsignals 
(          
date                  date null,
instrumentid          int   null,
strategycode          varchar(50) null,
`signal`              int null
)
;
