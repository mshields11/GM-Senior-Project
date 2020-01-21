
use GMFSP_db;

DROP TABLE IF EXISTS dbo_statisticalreturns;
create table dbo_statisticalreturns
(           
`date`                  date null,
instrumentid            int null,
strategycode            varchar(50) null,
positionsize            int null,
cashonhand              float null,
portfoliovalue          float null
)
;

