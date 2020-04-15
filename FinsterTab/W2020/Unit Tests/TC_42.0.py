
# TC_42.0

from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.EngineeredFeatures import EngineeredFeatures

engine = DBEngine().mysql_engine()
eng_feat = EngineeredFeatures(engine, 'dbo_instrumentmaster')
eng_feat = eng_feat.calculate()

if eng_feat is not None:
    print('Incorrect return value while running calculate function: %s' % eng_feat)
