
use GMFSP_db;

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
