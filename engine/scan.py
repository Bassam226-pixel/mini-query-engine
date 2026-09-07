import pyarrow.parquet as pq


class Scan:
    def __init__(self, path, columns=None, batch_size=100_000,
                 predicate=None, dictionary_columns=None):
        self.path = path
        self.columns = columns
        self.batch_size = batch_size
        self.predicate = predicate
        self.dictionary_columns = dictionary_columns
        self.groups_read = 0
        self.groups_skipped = 0

    def _can_skip(self, metadata, group_index):
        if self.predicate is None:
            return False

        column, threshold = self.predicate
        rg = metadata.row_group(group_index)

        for j in range(rg.num_columns):
            col = rg.column(j)
            if col.path_in_schema != column:
                continue
            stats = col.statistics
            if stats is None:
                return False
            return stats.max <= threshold

        return False

    def execute(self):
        pf = pq.ParquetFile(self.path,
                            read_dictionary=self.dictionary_columns)
        md = pf.metadata
        self.groups_read = 0
        self.groups_skipped = 0

        for i in range(md.num_row_groups):
            if self._can_skip(md, i):
                self.groups_skipped += 1
                continue

            self.groups_read += 1
            for batch in pf.iter_batches(
                batch_size=self.batch_size,
                columns=self.columns,
                row_groups=[i],
            ):
                yield batch