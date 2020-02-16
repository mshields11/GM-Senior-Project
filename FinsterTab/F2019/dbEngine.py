# import libraries to be used in this code module
import sqlalchemy as sal   # library to handle SQL database related operations
from pymysql import converters
import numpy as np


class DBEngine:

    def mysql_engine(self):
        """
        Create SQLAlchemy Engine for MySQL 8.0 server
        :return engine: creates connection to MySQL Server
        """

        '''converters.encoders[np.float64] = converters.escape_float
        converters.conversions = converters.encoders.copy()
        converters.conversions.update(converters.decoders)'''

        conn_str = 'mysql+pymysql://root:password@localhost:3306/gmfsp_db'
        engine = sal.create_engine(conn_str)
        return engine

# END CODE MODULE


