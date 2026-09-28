import time
import statistics
import duckdb
import pyarrow.parquet as pq

from engine.engine import sql as engine_sql
from engine.sql import Catalog
from engine.scan import Scan
from engine.join import HashJoinDirectIndex

USERS = "data/users.parquet"
ORDERS = "data/orders.parquet"
RUNS = 3

catalog = Catalog({"users": USERS, "orders": ORDERS})
con = duckdb.connect()
con.execute(f"CREATE VIEW users AS SELECT * FROM '{USERS}'")
con.execute(f"CREATE VIEW orders AS SELECT * FROM '{ORDERS}'")


def timed(fn):
    fn()
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


def run_engine(query):
    def go():
        return sum(b.num_rows for b in engine_sql(query, catalog))
    return timed(go)


def run_duckdb(query):
    def go():
        return con.execute(query).fetch_arrow_table().num_rows
    return timed(go)


def compare(label, query, duck_query=None):
    duck_query = duck_query or query
    t_mine, rows_mine = run_engine(query)
    t_duck, rows_duck = run_duckdb(duck_query)

    ok = "ok" if rows_mine == rows_duck else "MISMATCH"
    print(f"{label:<34}{t_mine:>8.3f}{t_duck:>9.3f}"
          f"{t_mine / t_duck:>8.1f}x   {rows_mine:>10,}  {ok}")


print(f"{'query':<34}{'mine':>8}{'duckdb':>9}{'ratio':>8}"
      f"{'rows':>13}")
print("-" * 76)

compare("filter, sorted column",
        "SELECT id, name FROM users WHERE id > 4900000")

compare("filter, random column",
        "SELECT name FROM users WHERE age > 70")

compare("select star, filtered",
        "SELECT * FROM users WHERE age > 79")

compare("group by, 10 groups",
        "SELECT city, count(*) FROM users GROUP BY city")


# join is not exposed through SQL yet, so compare the operator directly
def join_mine():
    op = HashJoinDirectIndex(
        build_child=Scan(USERS, columns=["id", "name"]),
        probe_child=Scan(ORDERS, columns=["user_id", "amount"]),
        build_key="id", probe_key="user_id")
    return sum(b.num_rows for b in op.execute())


JOIN_SQL = """
SELECT o.user_id, o.amount, u.name
FROM orders o JOIN users u ON o.user_id = u.id
"""

t_mine, rows_mine = timed(join_mine)
t_duck, rows_duck = run_duckdb(JOIN_SQL)
ok = "ok" if rows_mine == rows_duck else "MISMATCH"
print(f"{'hash join, 5M x 20M':<34}{t_mine:>8.3f}{t_duck:>9.3f}"
      f"{t_mine / t_duck:>8.1f}x   {rows_mine:>10,}  {ok}")