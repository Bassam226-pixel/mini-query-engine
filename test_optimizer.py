from engine.logical import ScanNode, FilterNode, ProjectNode
from engine.optimizer import push_down_projection

plan = ProjectNode(
    child=FilterNode(
        child=ScanNode(path="data/users.parquet"),
        column="age",
        threshold=30,
    ),
    columns=["name"],
)

print("before:", plan.child.child.columns)
push_down_projection(plan)
print("after: ", plan.child.child.columns)