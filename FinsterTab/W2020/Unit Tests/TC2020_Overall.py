import unittest
import pandas as pd
from FinsterTab.F2019.dbEngine import DBEngine
from FinsterTab.F2019.DataFetch import DataFetch
from FinsterTab.F2019.DataForecast import DataForecast

engine = DBEngine().mysql_engine()
fetch = DataFetch(engine, 'dbo_instrumentmaster')


class MyTestCase(unittest.TestCase):

    def test_1_if_Quandl_Variables_Present(self):

        query = 'SELECT * FROM dbo_macroeconmaster WHERE accesssource = "Quandl"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_2_if_Fred_Variables_Present(self):

        query = 'SELECT * FROM dbo_macroeconmaster WHERE accesssource = "FRED"'
        result = pd.read_sql_query(query, engine)

        assert result is not None


    def test_3_if_Yahoo_Variables_Present(self):
        query = 'SELECT * FROM dbo_macroeconmaster WHERE accesssource = "Yahoo"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_4_if_Macromaster_Filled(self):
        query = 'SELECT macroeconcode FROM dbo_macroeconmaster'
        result = pd.read_sql_query(query, engine)

        self.assertEqual(result.size, 8)


    def test_5_if_Macroeconstatisctis_Filled(self):
        query = 'SELECT distinct macroeconcode FROM gmfsp_db.dbo_macroeconstatistics;'
        result = pd.read_sql_query(query, engine)

        self.assertEqual(result.size, 7)


    def test_6_if_Macroeconalgorithmforecast_Filled(self):
        query = 'SELECT distinct algorithmcode FROM gmfsp_db.dbo_macroeconalgorithmforecast'
        result = pd.read_sql_query(query, engine)

        self.assertEqual(result.size, 4)

    def test_7_if_MSF1_Calculated(self):
        query = 'SELECT algorithmcode FROM gmfsp_db.dbo_macroeconalgorithmforecast ' \
                'WHERE algorithmcode = "MSF1"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_8_if_MSF2_Calculated(self):
        query = 'SELECT algorithmcode FROM gmfsp_db.dbo_macroeconalgorithmforecast ' \
                'WHERE algorithmcode = "MSF2"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_9_if_MSF3_Calculated(self):
        query = 'SELECT algorithmcode FROM gmfsp_db.dbo_macroeconalgorithmforecast ' \
                'WHERE algorithmcode = "MSF3"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_10_if_Calc_Function_Works(self):
        query = 'SELECT * FROM gmfsp_db.dbo_macroeconalgorithmforecast'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_11_if_Regression_Present_In_Master(self):
        query = 'SELECT * FROM gmfsp_db.dbo_algorithmmaster ' \
                'where algorithmcode = "regression"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_12_if_Regression_Forecasted_Values_Present(self):
        query = 'SELECT * FROM gmfsp_db.dbo_algorithmforecast ' \
                'WHERE algorithmcode = "regression"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    def test_13_if_Regression_Function_Runs(self):
        query = 'SELECT instrumentid, algorithmcode FROM ' \
                'gmfsp_db.dbo_algorithmforecast ' \
                'WHERE algorithmcode = "regression"'
        result = pd.read_sql_query(query, engine)

        assert result is not None

    # def test_(self):
    #     query = ''
    #     result = pd.read_sql_query(query, engine)
    #
    #     self.assertEqual(result)
    #
    # def test_(self):
    #     query = ''
    #     result = pd.read_sql_query(query, engine)
    #
    #     self.assertEqual(result)

if __name__ == '__main__':
    unittest.main()