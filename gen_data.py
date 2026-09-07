import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

TOTAL_ROWS = 5_000_000
BATCH_SIZE = 500_000
SEED = 42
OUTPUT = "data/users.parquet"

CITIES = ["Cairo", "Giza", "Alexandria", "Mansoura", "Tanta",
          "Aswan", "Luxor", "Suez", "Ismailia", "Port Said"]

schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("age", pa.int8()),
    ("city", pa.string()),
])


def make_batch(rng, start_id, n):
    ids = np.arange(start_id, start_id + n, dtype=np.int64)
    ages = rng.integers(18, 81, size=n).astype(np.int8)
    city_idx = rng.integers(0, len(CITIES), size=n)

    names = pa.array([f"user_{i}" for i in ids], type=pa.string())
    cities = pa.array([CITIES[i] for i in city_idx], type=pa.string())

    return pa.RecordBatch.from_arrays(
        [pa.array(ids), names, pa.array(ages, type=pa.int8()), cities],
        schema=schema
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