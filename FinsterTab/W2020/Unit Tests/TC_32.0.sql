-- Script ID : TC_32.0 --

use gmfsp_db;

SELECT *

FROM dbo_engineeredfeatures

WHERE lowfrllinelong > highfrllinelong
;
