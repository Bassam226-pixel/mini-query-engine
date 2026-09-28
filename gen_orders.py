import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

TOTAL_ROWS = 20_000_000
BATCH_SIZE = 1_000_000
SEED = 43
OUTPUT = "data/orders.parquet"

N_USERS = 5_000_000
HEAVY_FRACTION = 0.05        # 5% of users...
HEAVY_SHARE = 0.50           # ...receive 50% of orders
N_HEAVY = int(N_USERS * HEAVY_FRACTION)

STATUSES = ["paid", "pending", "cancelled", "refunded"]
STATUS_WEIGHTS = [0.70, 0.15, 0.10, 0.05]

schema = pa.schema([
    ("order_id", pa.int64()),
    ("user_id", pa.int64()),
    ("amount", pa.float64()),
    ("status", pa.string()),
])


def make_user_ids(rng, n):
    """Half the orders go to a small pool of heavy users."""
    n_from_heavy = int(n * HEAVY_SHARE)
    n_from_rest = n - n_from_heavy

    heavy = rng.integers(1, N_HEAVY + 1, size=n_from_heavy)
    rest = rng.integers(N_HEAVY + 1, N_USERS + 1, size=n_from_rest)

    ids = np.concatenate([heavy, rest])
    rng.shuffle(ids)
    return ids.astype(np.int64)


def make_batch(rng, start_id, n):
    order_ids = np.arange(start_id, start_id + n, dtype=np.int64)
    user_ids = make_user_ids(rng, n)
    amounts = np.round(rng.uniform(10.0, 5000.0, size=n), 2)
    status_idx = rng.choice(len(STATUSES), size=n, p=STATUS_WEIGHTS)
    statuses = [STATUSES[i] for i in status_idx]

    return pa.RecordBatch.from_arrays(
        [
            pa.array(order_ids),
            pa.array(user_ids),
            pa.array(amounts),
            pa.array(statuses, type=pa.string()),
        ],
        schema=schema,
    )


def main():
    rng = np.random.default_rng(SEED)
    written = 0
    with pq.ParquetWriter(OUTPUT, schema, compression="snappy") as writer:
        while written < TOTAL_ROWS:
            n = min(BATCH_SIZE, TOTAL_ROWS - written)
            writer.write_batch(make_batch(rng, written + 1, n))
            written += n
            print(f"{written:,} / {TOTAL_ROWS:,}")
    print("done")


if __name__ == "__main__":
    main()