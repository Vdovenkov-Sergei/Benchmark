from time import perf_counter
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from config import *

queries = [
    f"""SELECT "VendorID", COUNT(*)
        FROM "{name_tb}" GROUP BY 1;""",
    f"""SELECT "passenger_count", AVG("total_amount")
       FROM "{name_tb}" GROUP BY 1;""",
    f"""SELECT "passenger_count", EXTRACT(year FROM "tpep_pickup_datetime"), COUNT(*)
       FROM "{name_tb}" GROUP BY 1, 2;""",
    f"""SELECT "passenger_count", EXTRACT(year FROM "tpep_pickup_datetime"), ROUND("trip_distance"), COUNT(*)
       FROM "{name_tb}" GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;""",
]
COUNT_QUERY = 4
path = f"postgresql://{username}:{password}@{hostname}:{port}/{name_database}"

def test():
    measured_time = [0] * COUNT_QUERY
    engine = create_engine(path)
    session = sessionmaker(bind=engine)()
    for i in range(COUNT_QUERY):
        for _ in range(ATTEMPTS):
            start = perf_counter()
            session.execute(text(queries[i]))
            finish = perf_counter()
            measured_time[i] += finish - start
        measured_time[i] = round(measured_time[i] / ATTEMPTS, 4)
    session.close()
    engine.dispose()
    return measured_time
    