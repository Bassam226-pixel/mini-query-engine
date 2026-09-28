import time
from engine.scan import Scan
from engine.spill_join import GraceHashJoin

USERS = "data/users.parquet"
ORDERS = "data/orders.parquet"

join = GraceHashJoin(
    build_child=Scan(USERS, columns=["id", "name"]),
    probe_child=Scan(ORDERS, columns=["user_id", "amount"]),
    build_key="id",
    probe_key="user_id",
    n_partitions=16,
)

t0 = time.perf_counter()
for batch in join.execute():
    pass
elapsed = time.perf_counter() - t0

print(f"build rows:      {join.build_rows:,}")
print(f"probe rows:      {join.probe_rows:,}")
print(f"output rows:     {join.output_rows:,}")
print(f"bytes spilled:   {join.bytes_spilled / 1024 / 1024:.1f} MB")
print(f"peak partition:  {join.peak_partition_rows:,} rows")
print(f"time:            {elapsed:.2f} s")