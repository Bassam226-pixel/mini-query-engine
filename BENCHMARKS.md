# Benchmarks

Machine: Intel Core i5-13420H (8 cores / 12 threads), 15.7 GB RAM, NVMe SSD (Micron 512 GB)
Runtime: Windows 11, Python 3.14.5, pyarrow 25.0.1
Data: users.parquet — 5,000,000 rows, 55 MB on disk, snappy, 10 row groups × 500,000 rows

Method: 5 runs per case, warm-up read discarded, median reported. Page cache warm.

## Day 1 — baseline Parquet reads

| Case | Median | Rows | In-memory |
|---|---|---|---|
| full table | 84.5 ms | 5,000,000 | 167.9 MB |
| age only | 8.1 ms | 5,000,000 | 4.8 MB |
| id + age | 26.6 ms | 5,000,000 | 43.5 MB |
| row group 0 | 19.5 ms | 500,000 | 16.3 MB |

### Disk vs memory

The file is 55 MB on disk and 167.9 MB once it lands in Arrow. That is **3.05x
expansion**, and it happens in two stages:

| Stage | Size |
|---|---|
| On disk (snappy-compressed, encoded) | 55.1 MB |
| Parquet pages decompressed (still dictionary-encoded) | 129.3 MB |
| Arrow in memory (fully materialized) | 167.9 MB |

Snappy accounts for 2.35x of it. Decoding accounts for the rest. Per column, on disk
versus measured in Arrow:

| Column | On disk | Snappy ratio | In Arrow | Disk → Arrow |
|---|---|---|---|---|
| id | 22.7 MB | 1.88x | 38.7 MB | 1.7x |
| name | 26.0 MB | 3.08x | 75.2 MB | 2.9x |
| age | 3.8 MB | 1.00x | 4.8 MB | 1.3x |
| city | 2.5 MB | 1.00x | 49.1 MB | **19.4x** |

`city` is the one to look at. It holds 10 distinct values, so on disk it is a dictionary
of 10 strings plus RLE-packed indices — 2.5 MB. In Arrow it becomes a plain string array:
every row carries its own full copy of "Alexandria" plus a 32-bit offset, so 49 MB. The
compression was never in snappy, it was in the encoding, and reading into a plain Arrow
array throws that encoding away.

That also explains why `age` and `city` show a snappy ratio of 1.00x. They are already
dictionary + RLE encoded on disk, so the codec has nothing left to find.

Practical consequence, measured rather than assumed. Reading `city` with
`read_dictionary=["city"]` keeps the encoding instead of expanding it:

| Case | Median | In-memory | Arrow type |
|---|---|---|---|
| city as string | 57.7 ms | 49.1 MB | `string` |
| city as dictionary | 10.1 ms | 19.1 MB | `dictionary<string, int32>` |
| full table, city as dictionary | 75.3 ms | 137.8 MB | — |

2.6x less memory and 5.7x faster, for one keyword argument. The remaining 19.1 MB is
almost entirely the index buffer: 5M rows × 4 bytes, because Arrow defaults to int32
indices even for a 10-value dictionary. An int8 index would fit here and cost 4.8 MB.

This is the first real design input for the scan operator: dictionary types are not a
micro-optimization on this data, they are the difference between holding a column and
not holding it.

### Why this justifies the project

A 20 GB Parquet file at this expansion ratio needs ~60 GB of RAM to read whole. On this
machine, with 15.7 GB, the ceiling is roughly a 5 GB file. So the engine needs two things:

1. **Column pruning** — reading `age` alone costs 8.1 ms and 4.8 MB, against 84.5 ms and
   167.9 MB for the full table. That is 10.4x faster and 35x smaller, and it is free.
   Week 5.
2. **Batched execution** — never hold the whole table. Week 2.

### Note on the row-group number

`read_row_group(0)` takes 19.5 ms for one tenth of the data, but the full table takes
84.5 ms, not 195 ms. `read_table` decodes row groups in parallel across cores; a single
`read_row_group` call does not. Useful to know before we build the scan operator: the
per-batch cost we should budget against is the serial 19.5 ms, and parallelism across
row groups is something we choose to add, not something we get for free.


## Day 2 — vectorized vs row-at-a-time filter

Query: age > 30 on 5M rows, batch_size=100,000

| Approach | Median | Rows out |
|---|---|---|
| row-at-a-time | 0.321 s | 3,968,128 |
| vectorized | 0.049 s | 3,968,128 |

Speedup: 6.5x


## Day 3 — row group skipping via Parquet statistics

Same file, same engine, two queries.

| Query | Rows out | Groups read | Groups skipped |
|---|---|---|---|
| id > 4900000 | 100,000 | 1 | 9 |
| age > 79 | 79,595 | 10 | 0 |

`id` is written sequentially, so each row group holds a disjoint range
(group 0 is 1–500000, group 9 is 4500001–5000000). The predicate excludes
9 groups from the min/max statistics alone, without opening them.

`age` is random, so every group reports min=18 max=80. Nothing can be
excluded — the engine reads all 5M rows to return 79,595 (1.6%).

The statistics are only as useful as the physical ordering of the data.
This is the mechanism behind sorting and partitioning on frequently
filtered columns.


## Day 4 — group by

Query: SELECT city, count(*), sum(age) FROM users GROUP BY city

| Variant | Median | Speedup |
|---|---|---|
| row-at-a-time (python dict) | 1.734 s | — |
| vectorized, string keys | 0.918 s | 1.9x |
| vectorized, dictionary keys | 0.277 s | 6.3x |

Batch size sweep (vectorized, string keys): 10k=1.460s, 100k=0.919s,
500k=0.921s, 1M=0.951s. Optimum sits in the middle — small batches pay
per-batch overhead 500 times, large batches lose cache locality.

Three wrong guesses before the measurement found it: the scan was only
17% of the time, batch size accounted for 11%, and the real cost was
hashing 5M string keys. Dictionary encoding turned the key into an
integer and removed most of it.

Known limitation: each row group carries its own dictionary, so partials
are decoded to strings before the second stage. Proper dictionary
unification would avoid this.


## Day 5 — hash join, 5M build × 20M probe

| Implementation | Time | vs best |
|---|---|---|
| Arrow Table.join per batch | 229.2 s | 43x |
| pc.index_in per batch | 206.1 s | 39x |
| Python dict, built once | 62.6 s | 12x |
| Direct-address numpy array | 5.28 s | — |

All four return 20,000,000 rows — every order matches exactly one user.

Two independent costs. Rebuilding the lookup per batch (200 times)
accounts for the gap between 229s and 62s. Per-row Python work on the
probe side accounts for the gap between 62s and 5.28s.

The first two implementations both rebuild per batch, which is why their
times are close despite one having no Python loop. Vectorizing the probe
while reintroducing the rebuild made things worse, not better — kept as
a documented negative result.

The final version exploits a property of this data: build keys are dense
integers 1..5,000,000, so the lookup is an array index rather than a
hash. Cost is 38.1 MB, or (max_key+1)*8 bytes. This does not generalize —
sparse or non-integer keys need a real hash table.


## Day 6 — grace hash join with spilling

| Implementation | Time | Peak build rows in RAM |
|---|---|---|
| Direct-index, in-memory | 5.28 s | 5,000,000 |
| Grace hash join, 16 partitions | 27.85 s | 312,500 |

Both return 20,000,000 rows. 312.8 MB written to and read back from disk.

This is not an optimization — it is 5.3x slower by design. It trades
throughput for the ability to join a build side that does not fit in
memory. Peak resident build rows drop 16x, which is the point.

Build-side partitions came out exactly even (5M / 16 = 312,500) because
`id` is a dense sequence and `id % 16` distributes it perfectly. That is
a property of this data, not of the algorithm.

Not yet handled: the operator always spills, even when the build side
would fit. A hybrid hash join keeps the first partitions resident and
only spills once a memory budget is exceeded.


## Day 7 — partition skew

Same join, same 16 partitions, two probe datasets.

| Probe data | Largest partition | Share | max/min |
|---|---|---|---|
| 4.4M distinct users, top 5% hold 46% of orders | 1,252,111 | 6.26% | 1.00x |
| one user id holding 40% of orders | 8,749,871 | 43.75% | 11.69x |

Both return 20,000,000 rows — correctness is unaffected.

The first dataset is genuinely skewed by distribution: 221,036 users
account for 45.7% of orders, and the busiest single user has 70 of them.
Partitioning absorbs it completely, because those 221,036 keys spread
evenly across 16 buckets.

The second is skewed on a single key. `7 % 16 = 7`, so all 8M of its rows
must land in partition 7. No hash function avoids this — a single key has
exactly one destination, and sending its rows elsewhere would break the
equal-keys-same-partition property the algorithm depends on.

Build side stays at ratio 1.00x in both cases: user 7 appears there
exactly once. Skew is a property of key repetition within a side, not of
table size.

On one machine this shows up as elapsed time, since partitions run
sequentially. On a 16-node cluster it would mean 15 nodes idle while one
does 44% of the work.

Key distribution in the uniform dataset (`diag_key_distribution.py`):

| Slice | Share of orders |
|---|---|
| top 0.01% of users (442) | 0.1% |
| top 0.1% (4,420) | 1.2% |
| top 1% (44,207) | 10.9% |
| top 5% (221,036) | 45.7% |

4,420,726 of 5,000,000 users appear at all; the remaining ~580,000 have
no orders. They sit in the build side and are never probed — invisible in
an inner join, but they would surface as nulls in a left join.


## Day 8 — comparison against DuckDB

Identical queries, same measurement method (warm-up + median of 3),
both consuming the full result. Row counts match DuckDB in every case.

| Query | mine | duckdb | ratio |
|---|---|---|---|
| filter, sorted column | 0.041 s | 0.034 s | 1.2x |
| select star, filtered | 0.342 s | 0.222 s | 1.5x |
| filter, random column | 0.402 s | 0.224 s | 1.8x |
| hash join, 5M × 20M | 4.083 s | 1.792 s | 2.3x |
| group by, 10 groups | 0.210 s | 0.023 s | 9.1x |

The first run of this comparison put group by at 126x. Two fixes closed
it to 9.1x, and finding them is what the comparison was for.

The planner was building `GroupBy` — the row-at-a-time baseline — not
`GroupByVectorized`. The vectorized version had existed and been measured
since day 4, but was never wired into the planner, so every end-to-end
query had been running the slow path for two weeks.

The second fix was a third optimizer rule: read string grouping keys as
dictionaries. Unlike projection and predicate pushdown, this rule needs
the file's schema rather than just the plan shape.

Component benchmarks could not have caught the first problem — they
measure operators in isolation, so they cannot reveal that the right
operator is never reached. Only an end-to-end comparison against an
external reference exposed it.

Closest result is the statistics-pruned filter at 1.2x, where the
optimization reduces work rather than speeding it up, so implementation
language matters less.