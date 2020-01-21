-- Script ID : TC_33.0 --

use gmfsp_db;

SELECT *

FROM dbo_engineeredfeatures

WHERE boll_lb_v > boll_ub_v
;
