import pyarrow as pa
import pyarrow.compute as pc


class GroupBy:
    """Row-at-a-time - the slow baseline we measure against."""

    def __init__(self, child, group_column, agg_column):
        self.child = child
        self.group_column = group_column
        self.agg_column = agg_column
        self.groups_seen = 0

    def execute(self):
        table = {}

        for batch in self.child.execute():
            keys = batch.column(self.group_column).to_pylist()
            values = batch.column(self.agg_column).to_pylist()

            for k, v in zip(keys, values):
                entry = table.get(k)
                if entry is None:
                    table[k] = [1, v]
                else:
                    entry[0] += 1
                    entry[1] += v

        self.groups_seen = len(table)

        group_keys = list(table.keys())
        counts = [table[k][0] for k in group_keys]
        sums = [table[k][1] for k in group_keys]
        avgs = [s / c for s, c in zip(sums, counts)]

        yield pa.RecordBatch.from_pydict({
            self.group_column: group_keys,
            "count": counts,
            "sum": sums,
            "avg": avgs,
        })


class GroupByVectorized:
    """Two-stage hash aggregation using Arrow kernels.

    Stage 1 aggregates each batch on its own, producing a tiny partial
    result. Stage 2 combines the partials. Dictionary-encoded group keys
    are decoded after stage 1, where the result is only a handful of rows,
    because each row group carries its own dictionary and Arrow cannot
    merge differing dictionaries.
    """

    def __init__(self, child, group_column, agg_column):
        self.child = child
        self.group_column = group_column
        self.agg_column = agg_column
        self.groups_seen = 0

    def _decode_keys(self, partial):
        idx = partial.schema.get_field_index(self.group_column)
        col = partial.column(idx)
        if not pa.types.is_dictionary(col.type):
            return partial
        return partial.set_column(idx,
                                  self.group_column,
                                  col.cast(pa.string()))

    def execute(self):
        partials = []

        for batch in self.child.execute():
            t = pa.Table.from_batches([batch])
            partial = t.group_by(self.group_column).aggregate([
                (self.agg_column, "count"),
                (self.agg_column, "sum"),
            ])
            partials.append(self._decode_keys(partial))

        combined = pa.concat_tables(partials)
        final = combined.group_by(self.group_column).aggregate([
            (f"{self.agg_column}_count", "sum"),
            (f"{self.agg_column}_sum", "sum"),
        ])

        counts = final.column(f"{self.agg_column}_count_sum").to_pylist()
        sums = final.column(f"{self.agg_column}_sum_sum").to_pylist()

        self.groups_seen = final.num_rows

        yield pa.RecordBatch.from_pydict({
            self.group_column: final.column(self.group_column).to_pylist(),
            "count": counts,
            "sum": sums,
            "avg": [s / c for s, c in zip(sums, counts)],
        })