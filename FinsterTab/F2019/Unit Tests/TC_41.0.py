
# TC_41.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')
calendar = fetch.get_calendar()

if calendar is not None:
    print('Incorrect return value while running get_calendar function: %s' % calendar)
