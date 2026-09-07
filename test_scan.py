from engine.scan import Scan

scan = Scan("data/users.parquet", columns=["age"])

total_rows = 0
batch_count = 0

for batch in scan.execute():
    batch_count += 1
    total_rows += batch.num_rows
    if batch_count <= 3:
        print(f"batch {batch_count}: {batch.num_rows:,} rows, "
              f"{batch.nbytes:,} bytes")

print(f"\ntotal: {batch_count} batches, {total_rows:,} rows")