from engine.logical import ScanNode, FilterNode, ProjectNode
from engine.optimizer import push_down_projection, push_down_predicate
from engine.planner import build


def run_and_report(label, column, threshold):
    plan = ProjectNode(
        child=FilterNode(
            child=ScanNode(path="data/users.parquet"),
            column=column,
            threshold=threshold,
        ),
        columns=["name"],
    )
    push_down_projection(plan)
    push_down_predicate(plan)
    op = build(plan)

    # walk down to the scan so we can read its counters
    scan = op
    while hasattr(scan, "child"):
        scan = scan.child

    total = sum(b.num_rows for b in op.execute())
    print(f"{label:<22} rows={total:>9,}  "
          f"read={scan.groups_read}  skipped={scan.groups_skipped}")


run_and_report("id > 4900000", "id", 4_900_000)
run_and_report("age > 79", "age", 79)