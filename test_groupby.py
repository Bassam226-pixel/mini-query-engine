import time
from engine.logical import ScanNode, GroupByNode
from engine.engine import run

plan = GroupByNode(
    child=ScanNode(path="data/users.parquet"),
    group_column="city",
    agg_column="age",
)

t0 = time.perf_counter()

for batch in run(plan):
    d = batch.to_pydict()
    print(f"{'city':<14}{'count':>10}{'sum':>14}{'avg':>8}")
    for i in range(batch.num_rows):
        print(f"{d['city'][i]:<14}"
              f"{d['count'][i]:>10,}"
              f"{d['sum'][i]:>14,}"
              f"{d['avg'][i]:>8.2f}")

elapsed = time.perf_counter() - t0
print(f"\n{elapsed:.3f} s")