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


def optimize(node):
    push_down_projection(node)
    push_down_predicate(node)
    return node