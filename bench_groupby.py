import time
import statistics
from engine.scan import Scan
from engine.group_by import GroupBy, GroupByVectorized

PATH = "data/users.parquet"
RUNS = 3


def run(op):
    for batch in op.execute():
        rows = batch.num_rows
    return rows


def measure(label, make_op):
    run(make_op())
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        groups = run(make_op())
        times.append(time.perf_counter() - t0)
    median = statistics.median(times)
    print(f"{label:<34} {median:>8.3f} s   groups={groups}")
    return median


def scan(batch_size=100_000, dictionary=None):
    return Scan(PATH,
                columns=["city", "age"],
                batch_size=batch_size,
                dictionary_columns=dictionary)


print("--- baseline: batch=100k, plain strings ---")
t_slow = measure("row-at-a-time",
                 lambda: GroupBy(scan(), "city", "age"))
t_fast = measure("vectorized",
                 lambda: GroupByVectorized(scan(), "city", "age"))
print(f"speedup: {t_slow / t_fast:.1f}x\n")

print("--- effect of batch size (vectorized) ---")
for bs in [10_000, 100_000, 500_000, 1_000_000]:
    measure(f"batch_size={bs:,}",
            lambda bs=bs: GroupByVectorized(scan(batch_size=bs), "city", "age"))

print("\n--- effect of dictionary encoding ---")
t_plain = measure("string keys, batch=1M",
                  lambda: GroupByVectorized(scan(1_000_000), "city", "age"))
t_dict = measure("dictionary keys, batch=1M",
                 lambda: GroupByVectorized(scan(1_000_000, ["city"]),
                                           "city", "age"))
print(f"dictionary speedup: {t_plain / t_dict:.1f}x")