import time
import pyarrow as pa
from engine.scan import Scan

USERS = "data/users.parquet"
ORDERS = "data/orders.parquet"


def timed(label, fn):
    t0 = time.perf_counter()
    result = fn()
    print(f"{label:<34} {time.perf_counter()-t0:>7.2f} s")
    return result


def scan_users():
    return list(Scan(USERS, columns=["id", "name"]).execute())


def scan_orders():
    n = 0
    for b in Scan(ORDERS, columns=["user_id", "amount"]).execute():
        n += b.num_rows
    return n


build_batches = timed("scan users (build side)", scan_users)
build_table = timed("materialize build table",
                    lambda: pa.Table.from_batches(build_batches))
timed("scan orders (probe side)", scan_orders)

first_batch = next(Scan(ORDERS, columns=["user_id", "amount"],
                        batch_size=100_000).execute())
probe_table = pa.Table.from_batches([first_batch])

timed("join ONE probe batch (100k)",
      lambda: probe_table.join(build_table, keys="user_id",
                               right_keys="id", join_type="inner"))