
# TC_43.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataForecast import DataForecast

engine = DBEngine().mysql_engine()
forecast = DataForecast(engine, 'dbo_instrumentmaster')
forecast = forecast.calculate_forecast()

if forecast is not None:
    print('Incorrect return value while running calculate_forecast function: %s' % forecast)
