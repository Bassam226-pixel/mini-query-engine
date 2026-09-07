import time
import statistics
import pyarrow.parquet as pq

PATH = "data/users.parquet"
RUNS = 5


def measure(label, fn):
    fn()  # warm-up, not recorded
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        table = fn()
        times.append(time.perf_counter() - t0)

    median = statistics.median(times)
    print(f"{label:<28} "
          f"{median*1000:>8.1f} ms   "
          f"rows={table.num_rows:>9,}   "
          f"mem={table.nbytes/1024/1024:>7.1f} MB")
    return median


def main():
    pf = pq.ParquetFile(PATH)

    measure("full table", lambda: pq.read_table(PATH))
    measure("age only", lambda: pq.read_table(PATH, columns=["age"]))
    measure("id + age", lambda: pq.read_table(PATH, columns=["id", "age"]))
    measure("row group 0", lambda: pf.read_row_group(0))


if __name__ == "__main__":
    main()
