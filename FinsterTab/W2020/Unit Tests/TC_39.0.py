
# TC_39.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')
tickers = fetch.get_datasources()

symbols = ['GM', 'PFE', 'SPY', 'XPH', 'CARZ']

for i in range(len(symbols)):
    if symbols[i] != tickers['instrumentname'][i]:
        print('missing ticker: %s' % symbols[i])

