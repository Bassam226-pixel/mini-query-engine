import pyarrow as pa
import pyarrow.compute as pc


class Aggregate:
    """Computes count, sum, min, max over one column."""

    def __init__(self, child, column):
        self.child = child
        self.column = column

    def execute(self):
        count = 0
        total = 0
        minimum = None
        maximum = None

        for batch in self.child.execute():
            col = batch.column(self.column)
            if len(col) == 0:
                continue

            count += len(col)
            total += pc.sum(col).as_py()

            batch_min = pc.min(col).as_py()
            batch_max = pc.max(col).as_py()

            minimum = batch_min if minimum is None else min(minimum, batch_min)
            maximum = batch_max if maximum is None else max(maximum, batch_max)

        avg = total / count if count else None

        yield pa.RecordBatch.from_pydict({
            "count": [count],
            "sum": [total],
            "avg": [avg],
            "min": [minimum],
            "max": [maximum],
        })