import pyarrow as pa
import pyarrow.parquet as pq

from engine.logical import (
    ScanNode, FilterNode, ProjectNode, AggregateNode, GroupByNode
)


def collect_required_columns(node):
    if isinstance(node, ScanNode):
        return set()

    if isinstance(node, FilterNode):
        needed = collect_required_columns(node.child)
        needed.add(node.column)
        return needed

    if isinstance(node, ProjectNode):
        needed = collect_required_columns(node.child)
        needed.update(node.columns)
        return needed

    if isinstance(node, AggregateNode):
        needed = collect_required_columns(node.child)
        needed.add(node.column)
        return needed

    if isinstance(node, GroupByNode):
        needed = collect_required_columns(node.child)
        needed.add(node.group_column)
        needed.add(node.agg_column)
        return needed

    raise ValueError(f"unknown node: {type(node)}")


def _find_scan(node):
    while not isinstance(node, ScanNode):
        node = node.child
    return node


def push_down_projection(node):
    required = collect_required_columns(node)
    _set_columns(node, sorted(required))
    return node


def _set_columns(node, columns):
    if isinstance(node, ScanNode):
        node.columns = columns
        return
    _set_columns(node.child, columns)


def push_down_predicate(node):
    _set_predicate(node, None)
    return node


def _set_predicate(node, predicate):
    if isinstance(node, ScanNode):
        node.predicate = predicate
        return

    if isinstance(node, FilterNode):
        _set_predicate(node.child, (node.column, node.threshold))
        return

    _set_predicate(node.child, predicate)


def push_down_dictionary(node):
    """Read string group-by keys as dictionaries rather than plain strings.

    Hashing a dictionary index is an integer operation; hashing a string
    is not. Measured 3.3x on a 10-group string key.

    Applied only to grouping columns. Decoding costs something, and that
    cost is only repaid when the column is hashed millions of times —
    not when it is merely scanned or returned. That distinction is an
    assumption here, not a measurement.

    Unlike the other two rules, this one needs the file's schema, not
    just the plan shape.
    """
    group_columns = _collect_group_columns(node)
    if not group_columns:
        return node

    scan = _find_scan(node)
    schema = pq.ParquetFile(scan.path).schema_arrow

    string_keys = []
    for name in group_columns:
        try:
            field = schema.field(name)
        except KeyError:
            continue
        if pa.types.is_string(field.type):
            string_keys.append(name)

    if string_keys:
        scan.dictionary_columns = string_keys

    return node


def _collect_group_columns(node):
    if isinstance(node, ScanNode):
        return []
    if isinstance(node, GroupByNode):
        return [node.group_column] + _collect_group_columns(node.child)
    return _collect_group_columns(node.child)


def optimize(node):
    push_down_projection(node)
    push_down_predicate(node)
    push_down_dictionary(node)
    return node