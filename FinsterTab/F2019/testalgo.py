from DataForecast import DataForecast
from datetime import datetime
from dbEngine import DBEngine

if __name__ == '__main__':
    engine = DBEngine().mysql_engine()
    inst = 'dbo_macroeconmaster'
    simulator = DataForecast(engine, inst)

    #simulator.MacroEconIndForecast()
    simulator.calculate_accuracy_comb()
