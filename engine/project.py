class Project:
    def __init__(self, child, columns):
        self.child = child
        self.columns = columns

    def execute(self):
        for batch in self.child.execute():
            yield batch.select(self.columns)