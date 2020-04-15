
use GMFSP_db;

drop table if exists dbo_instrumentmaster;  
create table dbo_instrumentmaster(
instrumentid            int null,
instrumentname          varchar(50) null,
`type`                  varchar(50) null,
exchangename            varchar(50) null
)
;
