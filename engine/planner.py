from engine.logical import (
    ScanNode, FilterNode, ProjectNode, AggregateNode, GroupByNode
)
from engine.scan import Scan
from engine.filter import FilterVectorized
from engine.project import Project
from engine.aggregate import Aggregate
from engine.group_by import GroupBy


def build(node):
    if isinstance(node, ScanNode):
        return Scan(node.path,
                    columns=node.columns,
                    predicate=node.predicate)

    if isinstance(node, FilterNode):
        return FilterVectorized(build(node.child),
                                node.column,
                                node.threshold)

    if isinstance(node, ProjectNode):
        return Project(build(node.child), node.columns)

    if isinstance(node, AggregateNode):
        return Aggregate(build(node.child), node.column)

    if isinstance(node, GroupByNode):
        return GroupBy(build(node.child),
                       node.group_column,
                       node.agg_column)

    raise ValueError(f"unknown node: {type(node)}")


def find_scan(op):
    while hasattr(op, "child"):
        op = op.child
    return op