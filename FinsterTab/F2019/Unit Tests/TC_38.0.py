
# TC_38.0

from dbEngine import DBEngine
from DataFetch import DataFetch

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')
tickers = fetch.get_datasources()

print(tickers)

if len(tickers) == 5:
    print('incorrect number of symbols: %s' % str(len(tickers)))
