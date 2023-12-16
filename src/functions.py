from config import *
import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlite3 import connect

class Cl:
    BLUE = "\033[94m"; GREEN = "\033[92m"; YELLOW = "\033[93m"
    RED = "\033[91m"; PURPLE = "\033[95m"; BOLD = "\033[1m"
    UNDERLINE = "\033[4m"; ITALIC = "\033[3m"; END = "\033[0m"

def print_inf(where, is_already) -> None:
    if is_already:
        print(Cl.BLUE + "\n> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' already exists in " + Cl.END + Cl.GREEN + where + Cl.END)
    else:
        print(Cl.BLUE + "\n> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' has been created in " + Cl.END + Cl.GREEN + where + Cl.END)

def read_csv_file() -> pd.DataFrame:
    D_frame = pd.read_csv(f"{folder_data}\\{dataset}")
    D_frame = D_frame.drop(columns=["Airport_fee", "Unnamed: 21"])
    D_frame["tpep_pickup_datetime"] = pd.to_datetime(D_frame["tpep_pickup_datetime"])
    D_frame["tpep_dropoff_datetime"] = pd.to_datetime(D_frame["tpep_dropoff_datetime"])
    D_frame = D_frame.rename(columns={"Unnamed: 0" : "ID"})
    return D_frame

def load_data_to_PostgreSQL(path) -> None:
    engine = create_engine(path)
    if name_tb not in inspect(engine).get_table_names():
        D_frame = read_csv_file()
        D_frame.to_sql(name_tb, engine, index=False, chunksize=1000)
        print_inf("PostgreSQL", False)
    elif DROP:
        D_frame = read_csv_file()
        D_frame.to_sql(name_tb, engine, if_exists="replace", index=False, chunksize=1000)
        print_inf("PostgreSQL", False)       
    else:
        print_inf("PostgreSQL", True)
    engine.dispose()

def load_data_to_SQLite(path) -> None:
    con = connect(path)
    query_tables = "SELECT name FROM sqlite_master WHERE type='table';"
    tables = con.cursor().execute(query_tables).fetchall()
    if tuple([name_tb]) not in tables:
        D_frame = read_csv_file()
        D_frame.to_sql(name_tb, con, index=False, chunksize=1000)
        print_inf("SQLite", False)
    elif DROP:
        D_frame = read_csv_file()
        D_frame.to_sql(name_tb, con, if_exists="replace", index=False, chunksize=1000)
        print_inf("SQLite", False)       
    else:
        print_inf("SQLite", True)
    con.close()

def load_data() -> None:
    path_to_PostgreSQL = f"postgresql://{username}:{password}@{hostname}:{port}/{name_database}"
    load_data_to_PostgreSQL(path_to_PostgreSQL)
    load_data_to_SQLite(f"{folder_data}\\db.db")

def run_queries(tests) -> None:
    launches = [SQLITE3, PANDAS, PSYCOPG2, DUCKDB, SQLALCHEMY]
    modules = ["SQLite3", "Pandas", "Psycopg2", "Duckdb", "SQLalchemy"]
    data = {"Module" : [], "Query_1" : [], "Query_2" : [], "Query_3" : [], "Query_4" : []}
    MODULES = 5
    
    for i, test, launcn in zip(range(MODULES), tests, launches):
        if not launcn: continue
        result = test()
        data["Module"].append(modules[i])
        for j, key in enumerate(sorted(data)):
            if key != "Module": data[key].append(result[j - 1])

    dataframe = pd.DataFrame(data)
    dataframe.to_csv("Results/results.csv", sep=',', )
    print(Cl.BLUE + "\n> All results have been added to the file '" + Cl.END, end='')
    print(Cl.GREEN + "results.csv" + Cl.END + Cl.BLUE + "'\n" + Cl.END)
