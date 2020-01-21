
# TC_54.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.TradingSimulator import TradingSimulator

engine = DBEngine().mysql_engine()
simulator = TradingSimulator(engine, 'dbo_instrumentmaster')
simulator = simulator.trade_sim()

if simulator is not None:
    print('Incorrect return value while running trade_sim function: %s' % simulator)
