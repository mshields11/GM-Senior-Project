
# TC_51.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.BuySell import BuySell

engine = DBEngine().mysql_engine()
signal = BuySell(engine, 'dbo_instrumentmaster')
signal = signal.ema_signal()

if signal is not None:
    print('Incorrect return value while running ema_signal function: %s' % signal)
