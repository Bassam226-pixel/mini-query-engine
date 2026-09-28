import time
from engine.scan import Scan
from engine.join import HashJoinManual, HashJoinDirectIndex

USERS = "data/users.parquet"
ORDERS = "data/orders.parquet"


def make(cls):
    return cls(
        build_child=Scan(USERS, columns=["id", "name"]),
        probe_child=Scan(ORDERS, columns=["user_id", "amount"]),
        build_key="id",
        probe_key="user_id",
    )


def run(label, cls):
    op = make(cls)
    t0 = time.perf_counter()
    first = None
    for batch in op.execute():
        if first is None and batch.num_rows:
            first = batch.slice(0, 2).to_pydict()
    elapsed = time.perf_counter() - t0

    print(f"{label}")
    print(f"  build rows:  {op.build_rows:,}")
    print(f"  probe rows:  {op.probe_rows:,}")
    print(f"  output rows: {op.output_rows:,}")
    if getattr(op, "lookup_size", 0):
        print(f"  lookup size: {op.lookup_size / 1024 / 1024:.1f} MB")
    print(f"  time:        {elapsed:.2f} s")
    print(f"  sample:      {first}")
    print()
    return elapsed


t_manual = run("Manual hash join (python loop)", HashJoinManual)
t_direct = run("Direct-index join (numpy)", HashJoinDirectIndex)
print(f"speedup: {t_manual / t_direct:.1f}x")