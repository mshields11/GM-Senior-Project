
# TC_38.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')
tickers = fetch.get_datasources()

if len(tickers) == 5:
    print('incorrect number of symbols: %s' % str(len(tickers)))
