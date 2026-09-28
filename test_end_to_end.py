import time
from engine.engine import sql
from engine.sql import Catalog

catalog = Catalog({
    "users": "data/users.parquet",
    "orders": "data/orders.parquet",
})

queries = [
    "SELECT name FROM users WHERE age > 70",
    "SELECT id, name, city FROM users WHERE id > 4900000",
    "SELECT city, count(*) FROM users GROUP BY city",
]

for query in queries:
    print("=" * 60)
    print(query)

    t0 = time.perf_counter()
    rows = 0
    first = None
    for batch in sql(query, catalog):
        if first is None and batch.num_rows:
            first = batch.slice(0, 2).to_pydict()
        rows += batch.num_rows
    elapsed = time.perf_counter() - t0

    print(f"  rows:   {rows:,}")
    print(f"  time:   {elapsed:.3f} s")
    print(f"  sample: {first}")
    print()