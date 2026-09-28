import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

TOTAL_ROWS = 20_000_000
BATCH_SIZE = 1_000_000
SEED = 44
OUTPUT = "data/orders_skewed.parquet"

N_USERS = 5_000_000
HOT_USER = 7          # a single user id
HOT_SHARE = 0.40      # takes 40% of all orders

schema = pa.schema([
    ("order_id", pa.int64()),
    ("user_id", pa.int64()),
    ("amount", pa.float64()),
])


def make_batch(rng, start_id, n):
    n_hot = int(n * HOT_SHARE)
    n_rest = n - n_hot

    hot = np.full(n_hot, HOT_USER, dtype=np.int64)
    rest = rng.integers(1, N_USERS + 1, size=n_rest).astype(np.int64)

    user_ids = np.concatenate([hot, rest])
    rng.shuffle(user_ids)

    return pa.RecordBatch.from_arrays(
        [
            pa.array(np.arange(start_id, start_id + n, dtype=np.int64)),
            pa.array(user_ids),
            pa.array(np.round(rng.uniform(10.0, 5000.0, size=n), 2)),
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