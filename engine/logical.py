from dataclasses import dataclass
from typing import Optional, List, Tuple


@dataclass
class ScanNode:
    path: str
    columns: Optional[List[str]] = None
    predicate: Optional[Tuple[str, object]] = None


@dataclass
class FilterNode:
    child: object
    column: str
    threshold: object


@dataclass
class ProjectNode:
    child: object
    columns: List[str]


@dataclass
class AggregateNode:
    child: object
    column: str


@dataclass
class GroupByNode:
    child: object
    group_column: str
    agg_column: str