
# TC_56.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.TradingSimulator import TradingSimulator

engine = DBEngine().mysql_engine()
simulator = TradingSimulator(engine, 'dbo_instrumentmaster')
simulator = simulator.buy_hold_sim()

if simulator is not None:
    print('Incorrect return value while running buy_hold_sim function: %s' % simulator)
