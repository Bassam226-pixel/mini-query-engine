import sqlglot

queries = [
    "SELECT name FROM users WHERE age > 30",
    "SELECT city, count(*) FROM users GROUP BY city",
    "SELECT * FROM users",
]

for sql in queries:
    print("=" * 60)
    print(sql)
    print(repr(sqlglot.parse_one(sql)))
    print()