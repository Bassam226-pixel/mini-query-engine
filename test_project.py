from engine.scan import Scan
from engine.filter import FilterVectorized
from engine.project import Project

PATH = "data/users.parquet"

plan = Project(
    FilterVectorized(
        Scan(PATH, columns=["age", "name"]),
        "age", 30
    ),
    ["name"]
)

total = 0
for i, batch in enumerate(plan.execute()):
    total += batch.num_rows
    if i == 0:
        print(batch.schema)
        print(batch.slice(0, 3).to_pylist())

print(f"\ntotal rows: {total:,}")