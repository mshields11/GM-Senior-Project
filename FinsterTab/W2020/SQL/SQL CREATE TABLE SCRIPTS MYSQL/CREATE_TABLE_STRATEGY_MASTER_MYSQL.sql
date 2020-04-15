
use GMFSP_db;

DROP TABLE IF EXISTS dbo_strategymaster;

create table dbo_strategymaster 
(          
strategycode      varchar(20) null,
strategyname      varchar(50) null
)
;
/*
insert into dbo_algorithmmaster(`algorithmcode`,`algorithmname`)
values('ARIMA', 'Auto Regressive Integrated Moving Average') ,
('RandomForest', ''),
('NA' , 'Not Applicable'),
('PricePred' , 'Improved prediction for previous group'),
('PricePredOld' , 'Previous groups prediction using todays close'),
('svm' , 'support vector machine')
;
*/