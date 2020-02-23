
/*SCRIPT TO GENERATE ALL THE BACK-END DATA TABLES FOR GM FINTECH APPLICATION */
/*TABLES ARE NOT DEPENDENT UPON EACH OTHER FOR THE BACK-END STRUCTURE        */
/*MISSING A TABLE COULD CAUSE ISSUES IN RUNNING THE PYTHON SCRIPTS AND       */
/*RUNNING TABLEAU MEASURES                                                   */
/*SCRIPT IS WRITTEN FOR SQL FORMAT ACCEPTED BY MYSQL 8.X                     */


use GMFSP_db;

DROP TABLE IF EXISTS dbo_strategymaster;
create table dbo_strategymaster 
(          
strategycode        varchar(20) null,
strategyname        varchar(50) null
)
;


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


DROP TABLE IF EXISTS dbo_actionsignals;
create table dbo_actionsignals 
(          
date                  date null,
instrumentid          int   null,
strategycode          varchar(50) null,
`signal`              int null
)
;


DROP TABLE IF EXISTS dbo_algorithmmaster;
create table dbo_algorithmmaster 
(          
algorithmcode         varchar(50) null,
algorithmname         varchar(50) null
)
;


drop table if exists dbo_instrumentmaster;  
create table dbo_instrumentmaster(
instrumentid            int null,
instrumentname          varchar(50) null,
`type`                  varchar(50) null,
exchangename            varchar(50) null
)
;


DROP TABLE IF EXISTS dbo_instrumentstatistics;
create table dbo_instrumentstatistics(        
date                    date null,   
high                    float null,
low                     float null,
`open`                  float null,
`close`                 float null,
volume                  float null,
`adj close`             float null,
instrumentid            int null
)
;


DROP TABLE IF EXISTS dbo_datedim;
create table dbo_datedim(
`date`                 date null,
`year`                 int null,
`month`                int null,
qtr                    int null,
weekend                int null,
isholiday              int null
)
;


DROP TABLE IF EXISTS dbo_statisticalreturns;
create table dbo_statisticalreturns
(           
`date`                 date null,
instrumentid           int null,
strategycode           varchar(50) null,
positionsize           int null,
cashonhand             float null,
portfoliovalue         float null
)
;


DROP TABLE IF EXISTS dbo_engineeredfeatures;
create table dbo_engineeredfeatures(        
`date`                       date null,   
instrumentid                 int null,
rsi_14                       float null,
macd_v                       float null,
macds_v                      float null,
boll_v                       float null,
boll_ub_v                    float null,
boll_lb_v                    float null,
open_2_sma                   float null,
wcma                         float null,
scma                         float null,
lcma                         float null,
ltrough                      float null,
lpeak                        float null,
highfrllinelong              float null,
medfrllinelong               float null,
lowfrllinelong               float null,
strough                      float null,
speak                        float null,
ktrough                      float null,
kpeak                        float null,
sema                         float null,
mema                         float null,
lema                         float null,
volume_delta                 float null
)
;



/* MUST INSERT VALUES INTO INSTRUMENTMASTER TABLE IN ORDER FOR DATABASE TO WORK */
/* USE INSERT_INTO_INSTRUMENT_MASTER_MYSQL.sql FILE TO ADD SYMBOLS              */
/* USE REMOVE_FROM_INSTRUMENT_MASTER_MYSQL.sql FILE TO REMOVE SYMBOLS           */

TRUNCATE TABLE dbo_instrumentmaster;
insert into dbo_instrumentmaster
values (1 , 'GM'   , 'Equity' , 'YAHOO'),
	   (2 , 'PFE'  , 'Equity' , 'YAHOO'),
	   (3 , 'SPY'  , 'Equity' , 'YAHOO'),
	   (4 , 'XPH'  , 'Equity' , 'YAHOO'),
	   (5 , 'CARZ' , 'Equity' , 'YAHOO'),
       (6 , '^TYX' , 'Equity' , 'YAHOO')
;

drop table if exists dbo_macroeconmaster;  
create table dbo_macroeconmaster(
macroID            int null,
instrumentname          varchar(50) null,
accesskey               varchar(50) null,
access_source			varchar(50) null,
);

DROP TABLE IF EXISTS dbo_macroeconstatistics;
CREATE TABLE dbo_macroeconstatistics (
	date			date,
	statistics		int,
    macroID			int null);


DROP TABLE IF EXISTS dbo_macroeconforecast;
CREATE TABLE dbo_macroeconforecast (
	macroID			INT,
    instrumentID	INT,
    date			date,
    forecastPrice	float);
    
insert into dbo_macroeconmaster
values (1 , 'GDP'   , 'FRBP/GDPPLUS', 'Quandl'),
	   (2 , 'Unemployment Rate'  , 'USMISERY/INDEX', 'Quandl'),
	   (3 , 'Inflation Rate'  , 'USMISERY/INDEX', 'Quandl'),
	   (4 , 'Misery Index'  , 'USMISERY/INDEX', 'Quandl')
;

