from engine.logical import ScanNode, FilterNode, AggregateNode
from engine.engine import run

plan = AggregateNode(
    child=FilterNode(
        child=ScanNode(path="data/users.parquet"),
        column="age",
        threshold=30,
    ),
    column="age",
)

for batch in run(plan):
    print(batch.to_pydict())