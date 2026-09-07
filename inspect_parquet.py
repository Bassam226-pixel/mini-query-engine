import os
import pyarrow.parquet as pq

PATH = "data/users.parquet"


def inspect_file(path):
    pf = pq.ParquetFile(path)
    md = pf.metadata

    print("=" * 60)
    print(f"File: {path}")
    print(f"Size on disk: {os.path.getsize(path):,} bytes")
    print(f"Total rows: {md.num_rows:,}")
    print(f"Row groups: {md.num_row_groups}")
    print(f"Created by: {md.created_by}")
    print("\nSchema:")
    print(pf.schema_arrow)

    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        print("\n" + "-" * 60)
        print(f"Row group {i}: {rg.num_rows:,} rows, "
              f"{rg.total_byte_size:,} bytes")

        for j in range(rg.num_columns):
            col = rg.column(j)
            name = col.path_in_schema
            comp = col.total_compressed_size
            uncomp = col.total_uncompressed_size
            ratio = uncomp / comp if comp else 0

            print(f"  {name:<8} "
                  f"compressed={comp:>10,}  "
                  f"uncompressed={uncomp:>10,}  "
                  f"ratio={ratio:.1f}x  "
                  f"codec={col.compression}")
            print(f"           encodings={col.encodings}")

            st = col.statistics
            if st is not None:
                print(f"           min={st.min}  max={st.max}  "
                      f"nulls={st.null_count}  distinct={st.distinct_count}")
            else:
                print("           (no statistics)")


if __name__ == "__main__":
    inspect_file(PATH)