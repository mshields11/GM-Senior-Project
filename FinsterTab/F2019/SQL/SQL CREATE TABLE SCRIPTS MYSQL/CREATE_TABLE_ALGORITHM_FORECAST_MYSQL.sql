
use GMFSP_db;

DROP TABLE IF EXISTS dbo_algorithmforecast;
create table dbo_algorithmforecast 
(          
forecastdate            date null,
instrumentid            int   null,
forecastcloseprice      float null,
algorithmcode           varchar(50) null,
prederror               float null
)
;