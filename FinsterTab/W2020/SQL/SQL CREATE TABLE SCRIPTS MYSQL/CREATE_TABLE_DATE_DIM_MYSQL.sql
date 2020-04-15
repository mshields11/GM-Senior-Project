use GMFSP_db;

DROP TABLE IF EXISTS dbo_datedim;
create table dbo_datedim(
`date`          date null,
`year`          int null,
`month`         int null,
qtr             int null,
weekend         int null,
isholiday       int null
)
;


/* BELOW CODE IS BACKUP TO GENRATE TABLE WITH VALUES, IF PYTHON LIBRARY IS NOT AVAILABLE
drop temporary table if exists InnoDB_dim;  

set @startdate = date_sub(now(), interval 3 year);
set @enddate = now();

DROP TABLE IF EXISTS numbers_small;
CREATE TABLE numbers_small (number INT);
INSERT INTO numbers_small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

DROP TABLE IF EXISTS numbers;
CREATE TABLE numbers (number BIGINT);
INSERT INTO numbers
SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
  FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones
LIMIT 1000000;

ALTER TABLE numbers CHARACTER SET UTF8; 

DROP TABLE IF EXISTS dbo_datedim;
CREATE TABLE dbo_datedim(
  date_id          BIGINT PRIMARY KEY, 
  date             DATE NOT NULL,
  weekend          CHAR(10) NULL DEFAULT "Weekday",
  day_of_week      CHAR(10) NULL,
  month            CHAR(10) NULL,
  month_day        INT NULL, 
  year             INT NULL,
  week_starting_monday CHAR(2) NULL,
  isholiday         INT NULL
);

ALTER TABLE dbo_datedim CHARACTER SET UTF8; 

INSERT INTO dbo_datedim (date_id, date)
SELECT number, DATE_ADD('2016-01-01', INTERVAL number DAY )
FROM numbers
WHERE DATE_ADD('2016-01-01', INTERVAL number DAY ) BETWEEN '2016-01-01' and '2022-12-31'
ORDER BY number;


UPDATE dbo_datedim SET
  day_of_week = DATE_FORMAT( date, "%W" ),
  weekend =     IF( DATE_FORMAT( date, "%W" ) IN ('Saturday','Sunday'), 'Weekend', 'Weekday'),
  month =       DATE_FORMAT( date, "%M"),
  year =        DATE_FORMAT( date, "%Y" ),
  month_day =   DATE_FORMAT( date, "%d" )
  ;

UPDATE dbo_datedim SET week_starting_monday = DATE_FORMAT(date,'%v');


update dbo_datedim SET isholiday = 1 
where date in (
'2016-01-01', 
'2016-12-25', 

'2017-01-01', 
'2017-12-25', 

'2018-01-01', 
'2018-01-15' ,
'2018-02-19',
'2018-03-30',
'2018-05-28',
'2018-07-04',
'2018-09-03',
'2018-11-22',
'2018-12-05',
'2018-12-25', 

'2019-01-01', 
'2019-01-21' ,
'2019-02-18',
'2019-04-19', 
'2019-05-27', 
'2019-07-04',
'2019-09-02',
'2019-11-28',
'2019-12-25'
)
*/
