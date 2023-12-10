from pandas import read_csv, to_datetime
from sqlalchemy import create_engine, inspect
from sqlite3 import connect
from config import *
import os

class Cl:
    BLUE = "\033[94m"; GREEN = "\033[92m"; YELLOW = "\033[93m"
    RED = "\033[91m"; PURPLE = '\033[95m'; BOLD = "\033[1m"
    UNDERLINE = "\033[4m"; ITALIC = "\033[3m"; END = "\033[0m"

def load_data():
    date_1, date_2 = "tpep_pickup_datetime", "tpep_dropoff_datetime"
    path = f"postgresql://{username}:{password}@{hostname}:{port}/{name_database}" 
    engine = create_engine(path)

    if name_tb not in inspect(engine).get_table_names():
        D_frame = read_csv(f"{folder_data}\\{data}")
        D_frame[date_1] = to_datetime(D_frame[date_1])
        D_frame[date_2] = to_datetime(D_frame[date_2])
        D_frame.to_sql(name_tb, engine, if_exists="replace", index=False, chunksize=1000)
        print(Cl.BLUE + "\n> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' was successfully created in" + Cl.END, end="")
        print(Cl.GREEN + " PostgreSQL" + Cl.END)
        engine.dispose()
    else:
        print(Cl.BLUE + "\n> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' has already been created in" + Cl.END, end="")
        print(Cl.GREEN + " PostgreSQL" + Cl.END)
        engine.dispose()

    if not os.path.exists(f"{folder_data}\\{name_db}"):
        db = connect(f"{folder_data}\\{name_db}")
        D_frame = read_csv(f"{folder_data}\\{data}")
        D_frame[date_1] = to_datetime(D_frame[date_1])
        D_frame[date_2] = to_datetime(D_frame[date_2])
        D_frame.to_sql(name_tb, db, if_exists="replace", index=False, chunksize=1000)
        print(Cl.BLUE + "> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' was successfully created in " + Cl.END, end="")
        print(Cl.GREEN + name_db + Cl.END)
        db.close()
    else:
        print(Cl.BLUE + "> Table '" + Cl.END + Cl.RED + name_tb + Cl.END, end="")
        print(Cl.BLUE + "' has already been created in " + Cl.END, end="")
        print(Cl.GREEN + name_db + Cl.END)

def run_queries(info):
    COUNT_QUERY = 4
    print(Cl.YELLOW + Cl.UNDERLINE + "{:^16}".format("Module/Query") + Cl.END + Cl.PURPLE, end='')
    for i in range(COUNT_QUERY): print("{:^8}".format("Q_" + str(i + 1)), end='')
    print(Cl.END)

    for test, launcn, output in info:
        if not launcn: continue
        result = test()
        print(Cl.ITALIC + Cl.BLUE + "{:^16}".format(output) + Cl.END + Cl.GREEN, end='')
        for time in result: print("{:^8}".format(time), end='')
        print(Cl.END)
