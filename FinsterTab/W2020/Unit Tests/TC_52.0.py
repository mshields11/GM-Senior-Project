
# TC_52.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.BuySell import BuySell

engine = DBEngine().mysql_engine()
signal = BuySell(engine, 'dbo_instrumentmaster')
signal = signal.macd_signal()

if signal is not None:
    print('Incorrect return value while running macd_signal function: %s' % signal)
