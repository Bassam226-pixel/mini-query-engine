import pyarrow as pa
import pyarrow.compute as pc


class FilterRowAtATime:
    """The slow way — loop over every row in Python."""

    def __init__(self, child, column, threshold):
        self.child = child
        self.column = column
        self.threshold = threshold

    def execute(self):
        for batch in self.child.execute():
            values = batch.column(self.column).to_pylist()

            mask = []
            for v in values:
                mask.append(v > self.threshold)

            yield batch.filter(pa.array(mask))


class FilterVectorized:
    """The fast way — one operation on the whole array."""

    def __init__(self, child, column, threshold):
        self.child = child
        self.column = column
        self.threshold = threshold

    def execute(self):
        for batch in self.child.execute():
            mask = pc.greater(batch.column(self.column), self.threshold)
            yield batch.filter(mask)