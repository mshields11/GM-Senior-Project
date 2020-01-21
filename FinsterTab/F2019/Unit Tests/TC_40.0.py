
# TC_40.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')
tickers = fetch.get_datasources()
data = fetch.get_data(tickers)

if data is not None:
    print('Incorrect return value while running get_data function: %s' % data)
