
use GMFSP_db;

DROP TABLE IF EXISTS dbo_instrumentstatistics;
create table dbo_instrumentstatistics(        
date                         date null,   
high                         float null,
low                          float null,
`open`                       float null,
`close`                      float null,
volume                       float null,
`adj close`                  float null,
instrumentid                 int null
)
;
