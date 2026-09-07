import time
import statistics
from engine.scan import Scan
from engine.filter import FilterRowAtATime, FilterVectorized

PATH = "data/users.parquet"
RUNS = 3


def run(op):
    total = 0
    for batch in op.execute():
        total += batch.num_rows
    return total


def measure(label, make_op):
    run(make_op())  # warm-up
    times = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        rows = run(make_op())
        times.append(time.perf_counter() - t0)
    median = statistics.median(times)
    print(f"{label:<20} {median:>8.3f} s   rows_out={rows:,}")
    return median


def main():
    def slow():
        return FilterRowAtATime(Scan(PATH, columns=["age"]), "age", 30)

    def fast():
        return FilterVectorized(Scan(PATH, columns=["age"]), "age", 30)

    t_slow = measure("row-at-a-time", slow)
    t_fast = measure("vectorized", fast)
    print(f"\nspeedup: {t_slow / t_fast:.1f}x")


if __name__ == "__main__":
    main()