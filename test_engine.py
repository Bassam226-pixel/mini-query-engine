from engine.logical import ScanNode, FilterNode, ProjectNode
from engine.engine import run

plan = ProjectNode(
    child=FilterNode(
        child=ScanNode(path="data/users.parquet"),
        column="age",
        threshold=30,
    ),
    columns=["name"],
)

total = 0
for i, batch in enumerate(run(plan)):
    total += batch.num_rows
    if i == 0:
        print("schema:", batch.schema)
        print("sample:", batch.slice(0, 3).to_pylist())

print(f"total rows: {total:,}")