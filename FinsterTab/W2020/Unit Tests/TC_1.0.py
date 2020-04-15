# TC_1.0_Database Engine

import sqlalchemy as sal
from dbEngine import DBEngine

conn_str = 'mysql+pymysql://root:password@localhost:3306/gmfsp_db'
actual_engine = DBEngine().mysql_engine()
expected_engine = sal.create_engine(conn_str)

if actual_engine.url != expected_engine.url:
    print("incorrect engine")
else:
    print("correct engine")