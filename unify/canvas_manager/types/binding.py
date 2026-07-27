"""Read-only data bindings for canvases.

A binding declares one query a canvas needs at view time. Bindings are resolved
and dry-run validated when the canvas is authored, then re-executed server-side
under the owner's identity on every view, with the results delivered to the
canvas as props keyed by ``alias``.

Two properties make this safe enough to expose to assistant-authored UI:

- A binding names a **manager and a logical table**, not a raw context path. The
  path is resolved at author time against the tables that manager declares
  bindable, so a canvas cannot reach a context its manager never offered.
- The frame only ever sends an ``alias``. The stored binding is what executes,
  so a canvas cannot widen its own query after the fact.
"""

from __future__ import annotations

from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field

# Row ceiling for a single binding. Matches the dashboard bridge: enough for a
# chart or a table page, small enough that one canvas cannot pull a whole table
# into a browser.
CANVAS_MAX_ROWS = 1000

# Ceiling on bindings per canvas. A canvas needing more than this is almost
# always better served by one join than by a dozen round trips.
CANVAS_MAX_BINDINGS = 12

ALIAS_PATTERN = r"^[A-Za-z_$][A-Za-z0-9_$]*$"


class FilterArgs(BaseModel):
    """Row fetch from one table."""

    operation: Literal["filter"] = "filter"
    filter: Optional[str] = None
    columns: Optional[List[str]] = None
    exclude_columns: Optional[List[str]] = None
    order_by: Optional[str] = None
    descending: bool = False
    limit: int = Field(default=100, ge=1, le=CANVAS_MAX_ROWS)
    offset: Optional[int] = None
    group_by: Optional[List[str]] = None


class ReduceArgs(BaseModel):
    """Aggregate over one table, optionally grouped."""

    operation: Literal["reduce"] = "reduce"
    metric: str
    columns: Union[str, List[str]]
    filter: Optional[str] = None
    group_by: Optional[List[str]] = None


class JoinArgs(BaseModel):
    """Row fetch across two tables."""

    operation: Literal["join"] = "join"
    join_expr: str
    select: dict
    tables: List[str] = Field(min_length=2, max_length=2)
    mode: str = "inner"
    left_where: Optional[str] = None
    right_where: Optional[str] = None
    result_where: Optional[str] = None
    result_limit: int = Field(default=100, ge=1, le=CANVAS_MAX_ROWS)
    result_offset: Optional[int] = None


class JoinReduceArgs(BaseModel):
    """Aggregate across two joined tables."""

    operation: Literal["join_reduce"] = "join_reduce"
    join_expr: str
    select: dict
    tables: List[str] = Field(min_length=2, max_length=2)
    metric: str
    columns: Union[str, List[str]]
    mode: str = "inner"
    left_where: Optional[str] = None
    right_where: Optional[str] = None
    result_where: Optional[str] = None
    group_by: Optional[List[str]] = None


BindingArgs = Annotated[
    Union[FilterArgs, ReduceArgs, JoinArgs, JoinReduceArgs],
    Field(discriminator="operation"),
]


class PrimitiveBinding(BaseModel):
    """One named, read-only query a canvas performs at view time.

    Parameters
    ----------
    alias : str
        Name the canvas reads the result under, via ``canvas.data[alias]``.
        Must be a valid JavaScript identifier and unique within the canvas.
    manager : str
        Manager owning the data, e.g. ``"tasks"``, ``"contacts"``,
        ``"knowledge"``, ``"data"``. Must be a manager that declares bindable
        tables.
    table : str
        Logical table within that manager, e.g. ``"Tasks"``, ``"Contacts"``,
        ``"Data/Sales"``. Resolved to a fully qualified context at author time;
        the resolved path is what gets stored and executed.
    args : FilterArgs | ReduceArgs | JoinArgs | JoinReduceArgs
        The query itself, discriminated on ``operation``. Only these four
        read-only shapes exist — there is no binding that can mutate state.
    """

    kind: Literal["query"] = "query"
    alias: str = Field(pattern=ALIAS_PATTERN)
    manager: str
    table: str
    args: BindingArgs

    # Populated by binding resolution at author time, never by the author. Holds
    # the fully qualified context path the query runs against, so view-time
    # execution never has to re-resolve (and cannot resolve differently).
    resolved_context: Optional[str] = None
    resolved_tables: Optional[List[str]] = None
