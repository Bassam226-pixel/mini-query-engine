from engine.sql import Catalog, to_logical_plan
from engine.engine import run

catalog = Catalog({
    "users": "data/users.parquet",
    "orders": "data/orders.parquet",
})

queries = [
    "SELECT name FROM users WHERE age > 30",
    "SELECT * FROM users WHERE age > 70",
    "SELECT city, count(*) FROM users GROUP BY city",
]

for sql in queries:
    print("=" * 60)
    print(sql)
    plan = to_logical_plan(sql, catalog)
    print(plan)
    print()