use GMFSP_db;

SELECT * FROM dbo_strategymaster;
SELECT * FROM dbo_algorithmforecast;
SELECT * FROM dbo_actionsignals;
SELECT * FROM dbo_algorithmmaster;
SELECT * FROM dbo_instrumentmaster;
SELECT * FROM dbo_instrumentstatistics order by date desc; 
SELECT * from dbo_datedim order by date desc;
SELECT * FROM dbo_statisticalreturns order by date desc;
SELECT * FROM dbo_engineeredfeatures;



select distinct strategycode from dbo_statisticalreturns;
select * from dbo_statisticalreturns where strategycode = 'algo'

SELECT * FROM dbo_algorithmforecast where algorithmcode = 'xgb';
SELECT * FROM dbo_strategymaster;

select min(date), strategycode from dbo_statisticalreturns  group by strategycode;

select * from dbo_actionsignals where strategycode = '';



select * from dbo_actionsignals
where date = (select max(date) from dbo_instrumentstatistics) and
strategycode = 'comb'



SELECT DISTINCT algorithmcode FROM dbo_algorithmforecast;




insert into dbo_datedim
values('2019-11-15','2019','11','4','0','0')

select min(date), max(date) from dbo_datedim
select count(*) from dbo_datedim



SELECT e.*
FROM dbo_engineeredfeatures as e
		left outer join dbo_instrumentstatistics as i on e.instrumentid = i.instrumentid and e.date=i.date
order by e.instrumentid asc, date asc;






