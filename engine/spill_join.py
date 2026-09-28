import os
import shutil
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


class GraceHashJoin:
    """Hash join that spills both sides to disk when the build side
    does not fit in memory.

    Partitions both inputs by hash(key) % n_partitions into temporary
    Parquet files, then joins one partition at a time. Peak memory is
    roughly one partition of the build side rather than all of it.

    Correctness rests on one property: equal keys hash to equal
    partitions, so a match can never span two partitions.

    This always spills, even when the build side would fit in memory.
    A hybrid hash join would keep early partitions resident and spill
    only past a memory budget.
    """

    def __init__(self, build_child, probe_child, build_key, probe_key,
                 n_partitions=16, spill_dir="spill"):
        self.build_child = build_child
        self.probe_child = probe_child
        self.build_key = build_key
        self.probe_key = probe_key
        self.n_partitions = n_partitions
        self.spill_dir = spill_dir

        self.build_rows = 0
        self.probe_rows = 0
        self.output_rows = 0
        self.bytes_spilled = 0
        self.peak_partition_rows = 0
        self.partition_counts = {}

    def _partition_path(self, side, index):
        return os.path.join(self.spill_dir, f"{side}_{index}.parquet")

    def _spill_side(self, child, key, side):
        writers = {}
        counts = [0] * self.n_partitions
        total = 0

        for batch in child.execute():
            total += batch.num_rows
            keys = batch.column(key).to_numpy(zero_copy_only=False)
            buckets = np.abs(keys) % self.n_partitions

            table = pa.Table.from_batches([batch])

            for p in range(self.n_partitions):
                mask = buckets == p
                n = int(mask.sum())
                if n == 0:
                    continue
                counts[p] += n

                part = table.filter(pa.array(mask))

                if p not in writers:
                    writers[p] = pq.ParquetWriter(
                        self._partition_path(side, p), part.schema)
                writers[p].write_table(part)

        for w in writers.values():
            w.close()

        self.partition_counts[side] = counts
        return total

    def _make_lookup(self, build_table):
        keys = build_table.column(self.build_key)
        if isinstance(keys, pa.ChunkedArray):
            keys = keys.combine_chunks()
            if isinstance(keys, pa.ChunkedArray):
                keys = keys.chunk(0)
        keys = keys.to_numpy(zero_copy_only=False)

        max_key = int(keys.max())
        lookup = np.full(max_key + 1, -1, dtype=np.int64)
        lookup[keys] = np.arange(len(keys), dtype=np.int64)
        return lookup

    def _probe(self, batch, build_table, lookup):
        limit = len(lookup)
        probe_keys = batch.column(self.probe_key).to_numpy(
            zero_copy_only=False)

        safe = np.clip(probe_keys, 0, limit - 1)
        positions = np.where(
            (probe_keys >= 0) & (probe_keys < limit),
            lookup[safe], -1)

        matched = positions != -1
        if not matched.any():
            return None

        probe_side = pa.Table.from_batches([batch]).filter(pa.array(matched))
        build_side = build_table.take(pa.array(positions[matched]))

        arrays = list(probe_side.columns)
        names = list(probe_side.schema.names)
        for name in build_side.schema.names:
            if name == self.build_key:
                continue
            arrays.append(build_side.column(name))
            names.append(name)

        out = pa.Table.from_arrays(arrays, names=names).combine_chunks()
        self.output_rows += out.num_rows
        return out.to_batches()[0]

    def execute(self):
        if os.path.exists(self.spill_dir):
            shutil.rmtree(self.spill_dir)
        os.makedirs(self.spill_dir)

        self.build_rows = self._spill_side(
            self.build_child, self.build_key, "build")
        self.probe_rows = self._spill_side(
            self.probe_child, self.probe_key, "probe")

        self.bytes_spilled = sum(
            os.path.getsize(os.path.join(self.spill_dir, f))
            for f in os.listdir(self.spill_dir)
        )

        self.output_rows = 0
        self.peak_partition_rows = 0

        for p in range(self.n_partitions):
            build_path = self._partition_path("build", p)
            probe_path = self._partition_path("probe", p)

            if not (os.path.exists(build_path) and os.path.exists(probe_path)):
                continue

            build_table = pq.read_table(build_path).combine_chunks()
            self.peak_partition_rows = max(self.peak_partition_rows,
                                           build_table.num_rows)

            lookup = self._make_lookup(build_table)

            for batch in pq.ParquetFile(probe_path).iter_batches(
                    batch_size=100_000):
                out = self._probe(batch, build_table, lookup)
                if out is not None:
                    yield out

        shutil.rmtree(self.spill_dir)