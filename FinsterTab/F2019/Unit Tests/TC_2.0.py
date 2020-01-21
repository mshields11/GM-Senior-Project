# TC_2.0_Data Source

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch

conn_str = 'mysql+pymysql://root:password@localhost:3306/gmfsp_db'
engine = DBEngine().mysql_engine()
db_engine = DBEngine().mysql_engine()
instrument_master = 'dbo_instrumentmaster'
master_data = DataFetch(engine, instrument_master)

if master_data.datasource != 'yahoo':
    print("incorrect data source: %s" % master_data.datasource)

