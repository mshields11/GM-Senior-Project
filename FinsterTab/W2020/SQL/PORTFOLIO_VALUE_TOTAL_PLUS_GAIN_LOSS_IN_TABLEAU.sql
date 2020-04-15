/*PORTFOLIO_VALUE_TOTAL_PLUS_GAIN_LOSS_IN_TABLEAU*/

/*PORTFOLIO_VALUE_TOTAL*/
use GMFSP_db;

select r.* ,
       im.instrumentname       

from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster     as im on r.instrumentid = im.instrumentid

where r.date = (select max(date) from dbo_statisticalreturns)
;


/*GAIN_LOSS*/
select r.instrumentid,
       im.instrumentname,
       r.strategycode,
       r.date,
       sum(r.portfoliovalue) as portfoliovalue,
       sum(r.portfoliovalue) - sum(s.portfoliovalue) as gainloss

from dbo_statisticalreturns as r
        left outer join dbo_instrumentmaster as im on r.instrumentid = im.instrumentid
		left outer join (
                          select a.instrumentid,
                                 a.strategycode,
                                 a.date,
                                 max(portfoliovalue) as portfoliovalue
                          from dbo_statisticalreturns as a
						          inner join (
                                               select min(`date`) as `date`, 
                                                      strategycode
                                               from dbo_statisticalreturns
                                               group by strategycode
											  ) as s on a.strategycode = s.strategycode and a.date = s.date
                          group by
                                 a.instrumentid,
                                 a.strategycode,
                                 a.`date`
						) as s on  r.instrumentid = s.instrumentid and r.strategycode = s.strategycode 
where r.date = (select max(date) from dbo_statisticalreturns)
group by
       r.instrumentid,
       im.instrumentname,
       r.strategycode,
       r.`date`
;        
