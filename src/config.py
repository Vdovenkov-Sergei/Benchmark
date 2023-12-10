# Кол-во попыток запуска на статистических данных
ATTEMPTS = 20

# Название базы данных (файла .db)
name_db = ""
# Название таблицы в базе данных
name_tb = ""
# Название файла с данными
data = ""
# Название папки, где будут храниться файлы .csv, .db
folder_data = ""

# Данные для подключения к PostrgeSQL
username = ""
password = ""
hostname = "localhost"
port = "5432"
name_database = ""

# Выполняемые модули в программе (True - надо выполнить, False - не надо выполнить)
SQLITE3 = False
PANDAS = False
PSYCOPG2 = False
DUCKDB = False
SQLALCHEMY = False
