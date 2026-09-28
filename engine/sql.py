import sqlglot
from sqlglot import expressions as exp
import pyarrow as pa
import pyarrow.parquet as pq

from engine.logical import (
    ScanNode, FilterNode, ProjectNode, AggregateNode, GroupByNode
)


class Catalog:
    """Maps logical table names to physical file paths.

    Also answers schema questions, which the planner needs to expand
    SELECT * before the optimizer runs.
    """

    def __init__(self, tables):
        self.tables = {k.lower(): v for k, v in tables.items()}
        self._schemas = {}

    def path(self, table_name):
        name = table_name.lower()
        if name not in self.tables:
            raise ValueError(
                f"unknown table '{table_name}'. "
                f"known tables: {sorted(self.tables)}")
        return self.tables[name]

    def schema(self, table_name):
        name = table_name.lower()
        if name not in self._schemas:
            self._schemas[name] = pq.ParquetFile(
                self.path(name)).schema_arrow
        return self._schemas[name]

    def columns(self, table_name):
        return list(self.schema(table_name).names)

    def first_numeric_column(self, table_name):
        """count(*) counts rows, but GroupByNode always computes
        sum and avg as well, so it needs some numeric column to point
        at. This picks one rather than letting a string column through.
        """
        for field in self.schema(table_name):
            if pa.types.is_integer(field.type) or \
                    pa.types.is_floating(field.type):
                return field.name
        raise ValueError(
            f"table '{table_name}' has no numeric column to aggregate")


COMPARISONS = {
    exp.GT: ">",
    exp.GTE: ">=",
    exp.LT: "<",
    exp.LTE: "<=",
    exp.EQ: "=",
}


def _table_name(select):
    from_clause = select.args.get("from") or select.args.get("from_")
    if from_clause is None:
        raise ValueError(
            f"query has no FROM clause (keys: {list(select.args.keys())})")
    table = from_clause.this
    if not isinstance(table, exp.Table):
        raise ValueError("only plain table names are supported")
    return table.name


def _select_columns(select, catalog, table):
    """Resolve the SELECT list into concrete column names.

    Star expansion happens here: SELECT * becomes the real column
    names, read from the file's schema, so nothing downstream ever
    sees a star.
    """
    columns = []
    for item in select.expressions:
        if isinstance(item, exp.Star):
            columns.extend(catalog.columns(table))
        elif isinstance(item, exp.Column):
            columns.append(item.name)
        elif isinstance(item, exp.Alias):
            columns.append(item.this.name)
        else:
            raise ValueError(
                f"unsupported select item: {type(item).__name__}")
    return columns


def _filter_from_where(select):
    """Extract a single comparison from WHERE.

    Only `column > literal` is supported. FilterNode carries no
    operator field, so anything else would silently produce wrong
    results — we reject it instead.
    """
    where = select.args.get("where")
    if where is None:
        return None

    condition = where.this
    op = COMPARISONS.get(type(condition))
    if op is None:
        raise ValueError(
            f"unsupported WHERE condition: {type(condition).__name__}. "
            "Only a single comparison is supported (no AND/OR).")

    left, right = condition.this, condition.expression

    if not isinstance(left, exp.Column):
        raise ValueError("left side of WHERE must be a column")
    if not isinstance(right, exp.Literal):
        raise ValueError("right side of WHERE must be a literal")

    if op != ">":
        raise ValueError(
            f"only '>' is implemented; got '{op}'. "
            "FilterNode carries no operator yet.")

    value = right.this
    if not right.is_string:
        value = float(value) if "." in str(value) else int(value)

    return left.name, value


def _aggregate_from_select(select):
    """Find an aggregate function in the SELECT list, if any.

    Returns (kind, column) or None. count(*) reports column as None,
    since it counts rows rather than values.
    """
    for item in select.expressions:
        target = item.this if isinstance(item, exp.Alias) else item

        if isinstance(target, exp.Count):
            inner = target.this
            column = None if isinstance(inner, exp.Star) else inner.name
            return "count", column

        if isinstance(target, exp.Sum):
            return "sum", target.this.name

        if isinstance(target, exp.Avg):
            return "avg", target.this.name

    return None


def to_logical_plan(sql, catalog):
    """Parse SQL and build one of our logical plan trees."""
    tree = sqlglot.parse_one(sql)

    if not isinstance(tree, exp.Select):
        raise ValueError("only SELECT statements are supported")

    table = _table_name(tree)
    plan = ScanNode(path=catalog.path(table))

    where = _filter_from_where(tree)
    if where is not None:
        column, threshold = where
        plan = FilterNode(child=plan, column=column, threshold=threshold)

    group = tree.args.get("group")
    aggregate = _aggregate_from_select(tree)

    if group is not None:
        group_columns = [c.name for c in group.expressions]
        if len(group_columns) != 1:
            raise ValueError("only single-column GROUP BY is supported")
        if aggregate is None:
            raise ValueError("GROUP BY requires an aggregate function")

        _, agg_column = aggregate
        if agg_column is None:
            agg_column = catalog.first_numeric_column(table)

        return GroupByNode(child=plan,
                           group_column=group_columns[0],
                           agg_column=agg_column)

    if aggregate is not None:
        _, agg_column = aggregate
        if agg_column is None:
            agg_column = catalog.first_numeric_column(table)
        return AggregateNode(child=plan, column=agg_column)

    columns = _select_columns(tree, catalog, table)
    return ProjectNode(child=plan, columns=columns)