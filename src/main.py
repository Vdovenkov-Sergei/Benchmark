import test_sqlite3 as T_sqlite3
import test_pandas as T_pandas
import test_psycopg2 as T_psycopg2
import test_duck_db as T_duckdb
import test_sqlalchemy as T_sqlalchemy
import functions
from config import *

tests = [T_sqlite3.test, T_pandas.test, T_psycopg2.test, T_duckdb.test, T_sqlalchemy.test]
launches = [SQLITE3, PANDAS, PSYCOPG2, DUCKDB, SQLALCHEMY]
output = ["> SQLite3", "> Pandas", "> Psycopg2", "> Duckdb", "> SQLalchemy"]
functions.load_data()
print()
functions.run_queries(zip(tests, launches, output))
