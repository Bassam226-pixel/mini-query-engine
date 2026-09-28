import numpy as np
import pyarrow.parquet as pq

PATH = "data/orders.parquet"

t = pq.read_table(PATH, columns=["user_id"])
keys = t.column("user_id").combine_chunks().to_numpy()

vals, counts = np.unique(keys, return_counts=True)
counts_sorted = np.sort(counts)[::-1]
total = len(keys)

print(f"total orders:    {total:,}")
print(f"distinct users:  {len(vals):,}")
print()
print(f"orders per user: min={counts.min()}  max={counts.max()}  "
      f"mean={counts.mean():.1f}  median={int(np.median(counts))}")
print()

for pct in [0.001, 0.01, 0.1, 1, 5]:
    n = max(1, int(len(vals) * pct / 100))
    share = counts_sorted[:n].sum() / total * 100
    print(f"top {pct:>5}% of users ({n:>7,} users) "
          f"hold {share:>5.1f}% of orders")