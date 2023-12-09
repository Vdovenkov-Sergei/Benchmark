ATTEMPTS = 20

name_db = "DB_big_taxi.db"
name_tb = "B_trips"
data = "nyc_yellow_big.csv"
folder_data = "Data"
path_to_postgres = "postgresql://postgres:12345@localhost:5432/postgres"

SQLITE3 = True
PANDAS = True
PSYCOPG2 = True
DUCKDB = True
SQLALCHEMY = True

db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "12345",
    "host": "localhost",
    "port": "5432",
}
