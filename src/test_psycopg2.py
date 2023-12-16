from time import perf_counter
from config import *
import psycopg2

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
db_params = {"dbname": name_database, "user": username, "password": password,
             "host": hostname, "port": port}

def test():
    measured_time = [0] * COUNT_QUERY
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for i in range(COUNT_QUERY):
        for _ in range(ATTEMPTS):
            start = perf_counter()
            cursor.execute(queries[i])
            finish = perf_counter()
            measured_time[i] += finish - start
        measured_time[i] = round(measured_time[i] / ATTEMPTS, 4)
    cursor.close()
    connection.close()
    return measured_time
