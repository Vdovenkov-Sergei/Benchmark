from src import test_sqlite3 as T_sqlite3
from src import test_pandas as T_pandas
from src import test_psycopg2 as T_psycopg2
from src import test_duck_db as T_duckdb
from src import test_sqlalchemy as T_sqlalchemy
from src import functions as Func

tests = [T_sqlite3.test, T_pandas.test, T_psycopg2.test, T_duckdb.test, T_sqlalchemy.test]
Func.load_data()
Func.run_queries(tests)
