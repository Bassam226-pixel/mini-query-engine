import time
from engine.scan import Scan

PATH = "data/users.parquet"


def time_it(label, fn):
    fn()
    t0 = time.perf_counter()
    fn()
    print(f"{label:<30} {time.perf_counter()-t0:>7.3f} s")


def scan_only():
    total = 0
    for b in Scan(PATH, columns=["city", "age"]).execute():
        total += b.num_rows
    return total


def scan_age_only():
    total = 0
    for b in Scan(PATH, columns=["age"]).execute():
        total += b.num_rows
    return total


time_it("scan city+age", scan_only)
time_it("scan age only", scan_age_only)