import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


class HashJoin:
    """Inner hash join using Arrow's built-in Table.join.

    Slowest baseline: Table.join rebuilds its internal hash table from
    the build side on every call, and we call it once per probe batch.
    Kept only for measurement.
    """

    def __init__(self, build_child, probe_child, build_key, probe_key):
        self.build_child = build_child
        self.probe_child = probe_child
        self.build_key = build_key
        self.probe_key = probe_key
        self.build_rows = 0
        self.probe_rows = 0
        self.output_rows = 0

    def execute(self):
        build_batches = list(self.build_child.execute())
        build_table = pa.Table.from_batches(build_batches)
        self.build_rows = build_table.num_rows

        self.probe_rows = 0
        self.output_rows = 0

        for batch in self.probe_child.execute():
            probe_table = pa.Table.from_batches([batch])
            self.probe_rows += probe_table.num_rows

            joined = probe_table.join(
                build_table,
                keys=self.probe_key,
                right_keys=self.build_key,
                join_type="inner",
            )
            self.output_rows += joined.num_rows

            for out in joined.to_batches():
                yield out


class HashJoinManual:
    """Hash join that builds a Python dict index exactly once.

    Fixes the rebuild problem, but still loops over every probe row in
    Python to do the lookups.

    Assumes build keys are unique. Duplicate build keys silently
    overwrite each other and rows are lost.
    """

    def __init__(self, build_child, probe_child, build_key, probe_key):
        self.build_child = build_child
        self.probe_child = probe_child
        self.build_key = build_key
        self.probe_key = probe_key
        self.build_rows = 0
        self.probe_rows = 0
        self.output_rows = 0

    def _build(self):
        batches = list(self.build_child.execute())
        table = pa.Table.from_batches(batches)
        self.build_rows = table.num_rows

        keys = table.column(self.build_key).to_pylist()

        index = {}
        for row_number, key in enumerate(keys):
            index[key] = row_number

        return table, index

    def _combine(self, probe_side, build_side):
        arrays = list(probe_side.columns)
        names = list(probe_side.schema.names)

        for name in build_side.schema.names:
            if name == self.build_key:
                continue
            arrays.append(build_side.column(name))
            names.append(name)

        combined = pa.Table.from_arrays(arrays, names=names)
        self.output_rows += combined.num_rows
        return combined.combine_chunks().to_batches()[0]

    def execute(self):
        build_table, index = self._build()

        self.probe_rows = 0
        self.output_rows = 0

        for batch in self.probe_child.execute():
            self.probe_rows += batch.num_rows

            probe_keys = batch.column(self.probe_key).to_pylist()

            probe_positions = []
            build_positions = []

            for i, key in enumerate(probe_keys):
                match = index.get(key)
                if match is not None:
                    probe_positions.append(i)
                    build_positions.append(match)

            if not probe_positions:
                continue

            probe_side = pa.Table.from_batches([batch]).take(probe_positions)
            build_side = build_table.take(build_positions)

            yield self._combine(probe_side, build_side)


class HashJoinVectorized:
    """Hash join using pc.index_in on the probe side.

    Removes the Python loop, but pc.index_in is a standalone kernel that
    builds its own lookup structure from value_set on every call — so
    this reintroduces the per-batch rebuild that HashJoinManual fixed.
    Measured slower than the Python loop. Kept as a documented negative
    result.
    """

    def __init__(self, build_child, probe_child, build_key, probe_key):
        self.build_child = build_child
        self.probe_child = probe_child
        self.build_key = build_key
        self.probe_key = probe_key
        self.build_rows = 0
        self.probe_rows = 0
        self.output_rows = 0

    def execute(self):
        build_batches = list(self.build_child.execute())
        build_table = pa.Table.from_batches(build_batches).combine_chunks()
        self.build_rows = build_table.num_rows

        build_keys = build_table.column(self.build_key)
        if isinstance(build_keys, pa.ChunkedArray):
            build_keys = build_keys.combine_chunks()
            if isinstance(build_keys, pa.ChunkedArray):
                build_keys = build_keys.chunk(0)

        self.probe_rows = 0
        self.output_rows = 0

        for batch in self.probe_child.execute():
            self.probe_rows += batch.num_rows

            positions = pc.index_in(batch.column(self.probe_key),
                                    value_set=build_keys)

            matched = pc.is_valid(positions)
            probe_side = pa.Table.from_batches([batch]).filter(matched)

            if probe_side.num_rows == 0:
                continue

            build_positions = pc.drop_null(positions)
            build_side = build_table.take(build_positions)

            arrays = list(probe_side.columns)
            names = list(probe_side.schema.names)
            for name in build_side.schema.names:
                if name == self.build_key:
                    continue
                arrays.append(build_side.column(name))
                names.append(name)

            out = pa.Table.from_arrays(arrays, names=names).combine_chunks()
            self.output_rows += out.num_rows
            yield out.to_batches()[0]


class HashJoinDirectIndex:
    """Join using a direct-address lookup array instead of a hash table.

    Valid only when build keys are dense non-negative integers within a
    known range. Then key lookup is an array index: the build index is
    constructed once with a single numpy scatter, and probing a whole
    batch is one numpy gather with no per-row work and no rebuild.

    Memory cost is (max_key + 1) * 8 bytes regardless of how many keys
    are actually present, so sparse key spaces make this a bad choice.
    lookup_size records the cost so it can be reported.

    Assumes build keys are unique.
    """

    SENTINEL = -1

    def __init__(self, build_child, probe_child, build_key, probe_key):
        self.build_child = build_child
        self.probe_child = probe_child
        self.build_key = build_key
        self.probe_key = probe_key
        self.build_rows = 0
        self.probe_rows = 0
        self.output_rows = 0
        self.lookup_size = 0

    def _build(self):
        batches = list(self.build_child.execute())
        table = pa.Table.from_batches(batches).combine_chunks()
        self.build_rows = table.num_rows

        keys = table.column(self.build_key)
        if isinstance(keys, pa.ChunkedArray):
            keys = keys.combine_chunks()
            if isinstance(keys, pa.ChunkedArray):
                keys = keys.chunk(0)
        keys = keys.to_numpy(zero_copy_only=False)

        max_key = int(keys.max())
        lookup = np.full(max_key + 1, self.SENTINEL, dtype=np.int64)
        lookup[keys] = np.arange(len(keys), dtype=np.int64)

        self.lookup_size = lookup.nbytes
        return table, lookup

    def execute(self):
        build_table, lookup = self._build()
        limit = len(lookup)

        self.probe_rows = 0
        self.output_rows = 0

        for batch in self.probe_child.execute():
            self.probe_rows += batch.num_rows

            probe_keys = batch.column(self.probe_key).to_numpy(
                zero_copy_only=False)

            safe_keys = np.clip(probe_keys, 0, limit - 1)
            build_positions = np.where(
                (probe_keys >= 0) & (probe_keys < limit),
                lookup[safe_keys],
                self.SENTINEL,
            )

            matched = build_positions != self.SENTINEL
            if not matched.any():
                continue

            probe_side = pa.Table.from_batches([batch]).filter(
                pa.array(matched))
            build_side = build_table.take(
                pa.array(build_positions[matched]))

            arrays = list(probe_side.columns)
            names = list(probe_side.schema.names)
            for name in build_side.schema.names:
                if name == self.build_key:
                    continue
                arrays.append(build_side.column(name))
                names.append(name)

            out = pa.Table.from_arrays(arrays, names=names).combine_chunks()
            self.output_rows += out.num_rows
            yield out.to_batches()[0]