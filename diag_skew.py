import sys
from engine.scan import Scan
from engine.spill_join import GraceHashJoin

USERS = "data/users.parquet"
N_PARTS = 16


def measure(orders_path, label):
    join = GraceHashJoin(
        build_child=Scan(USERS, columns=["id", "name"]),
        probe_child=Scan(orders_path, columns=["user_id", "amount"]),
        build_key="id",
        probe_key="user_id",
        n_partitions=N_PARTS,
    )

    for _ in join.execute():
        pass

    build = join.partition_counts["build"]
    probe = join.partition_counts["probe"]
    total = sum(probe)

    print(f"=== {label} ===")
    print(f"{'part':>5}{'build':>12}{'probe':>14}{'probe %':>10}")
    for p in range(N_PARTS):
        print(f"{p:>5}{build[p]:>12,}{probe[p]:>14,}"
              f"{probe[p] / total * 100:>9.2f}%")

    print(f"\nbuild  min={min(build):,}  max={max(build):,}  "
          f"ratio={max(build) / min(build):.2f}x")
    print(f"probe  min={min(probe):,}  max={max(probe):,}  "
          f"ratio={max(probe) / min(probe):.2f}x")
    print(f"output rows: {join.output_rows:,}\n")


measure("data/orders.parquet", "uniform-ish orders")
measure("data/orders_skewed.parquet", "one hot key (user 7 = 40%)")