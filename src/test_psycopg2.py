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

def test():
    results = [0] * COUNT_QUERY
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    for i in range(COUNT_QUERY):
        for _ in range(ATTEMPTS):
            start = perf_counter()
            cursor.execute(queries[i])
            finish = perf_counter()
            results[i] += finish - start
        results[i] = round(results[i] / ATTEMPTS, 3)
    cursor.close()
    connection.close()
    return results
