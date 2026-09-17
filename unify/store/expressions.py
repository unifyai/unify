"""The row expression language: filters, sort keys and derived columns.

Expressions are Python expressions evaluated against one row at a time. A
bare name resolves to the row's field of that name (``None`` when absent), so
``status == "open" and len(tags) > 2`` reads the way it looks. Derived-column
equations may also spell a field as ``{field}`` or ``{lg:field}``; both forms
are rewritten to the bare name before compilation.

Missing data follows SQL rather than Python: an ordering comparison,
arithmetic, membership test or method call that meets ``None`` yields ``None``
(which is falsy) instead of raising. Equality keeps Python semantics so
``field == None`` tests for absence.

The evaluator walks the AST directly. Only the node types and callables listed
here are accepted; anything else is rejected at compile time with
:class:`InvalidExpression`, so a malformed filter fails before it touches a
single row.
"""

from __future__ import annotations

import ast
import math
import operator
import re
import statistics
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from .errors import InvalidExpression

Row = Mapping[str, Any]
Compiled = Callable[[Row], Any]

_PLACEHOLDER = re.compile(r"\{(?:[A-Za-z_]\w*:)?([A-Za-z_]\w*)\}")

# Names that are pseudo-fields when a row has no entry of that name.
ROW_ID_NAMES = ("row_id", "id")
TIMESTAMP_NAMES = ("timestamp", "ts")


def rewrite_placeholders(source: str) -> str:
    """Rewrite ``{field}`` / ``{alias:field}`` placeholders to bare names."""
    return _PLACEHOLDER.sub(lambda m: m.group(1), source)


# ---------------------------------------------------------------------------
# Value helpers with SQL-style NULL propagation
# ---------------------------------------------------------------------------


def _is_null(value: Any) -> bool:
    return value is None


def _numeric(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _order(a: Any, b: Any) -> int | None:
    """Three-way comparison that tolerates mixed types; ``None`` when undefined."""
    if _is_null(a) or _is_null(b):
        return None
    try:
        if a < b:
            return -1
        if a > b:
            return 1
        return 0
    except TypeError:
        pass
    na, nb = _numeric(a), _numeric(b)
    if na is not None and nb is not None:
        return (na > nb) - (na < nb)
    if isinstance(a, datetime) and isinstance(b, str):
        parsed = parse_datetime(b)
        return None if parsed is None else _order(a, parsed)
    if isinstance(b, datetime) and isinstance(a, str):
        parsed = parse_datetime(a)
        return None if parsed is None else _order(parsed, b)
    sa, sb = str(a), str(b)
    return (sa > sb) - (sa < sb)


def _eq(a: Any, b: Any) -> bool:
    if a == b:
        return True
    if _is_null(a) or _is_null(b):
        return False
    if isinstance(a, bool) or isinstance(b, bool):
        return False
    na, nb = _numeric(a), _numeric(b)
    if (
        na is not None
        and nb is not None
        and (isinstance(a, (int, float)) or isinstance(b, (int, float)))
    ):
        return na == nb
    return False


def _contains(container: Any, item: Any) -> bool | None:
    if _is_null(container):
        return None
    if isinstance(container, str):
        return None if _is_null(item) else str(item) in container
    if isinstance(container, Mapping):
        return item in container
    try:
        return any(_eq(elem, item) for elem in container)
    except TypeError:
        return None


def _compare(op: type, a: Any, b: Any) -> Any:
    if op is ast.Eq:
        return _eq(a, b)
    if op is ast.NotEq:
        return not _eq(a, b)
    if op is ast.Is:
        return a is b
    if op is ast.IsNot:
        return a is not b
    if op is ast.In:
        return _contains(b, a)
    if op is ast.NotIn:
        found = _contains(b, a)
        return None if found is None else not found
    ordered = _order(a, b)
    if ordered is None:
        return None
    if op is ast.Lt:
        return ordered < 0
    if op is ast.LtE:
        return ordered <= 0
    if op is ast.Gt:
        return ordered > 0
    if op is ast.GtE:
        return ordered >= 0
    raise InvalidExpression(f"Unsupported comparison {op.__name__}")


_BINARY = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}


def _binary(op: type, a: Any, b: Any) -> Any:
    if _is_null(a) or _is_null(b):
        return None
    fn = _BINARY.get(op)
    if fn is None:
        raise InvalidExpression(f"Unsupported operator {op.__name__}")
    try:
        return fn(a, b)
    except (TypeError, ZeroDivisionError):
        if op is ast.Add and (isinstance(a, str) or isinstance(b, str)):
            return str(a) + str(b)
        return None


# ---------------------------------------------------------------------------
# Built-in functions
# ---------------------------------------------------------------------------


def parse_datetime(value: Any) -> datetime | None:
    """Parse a datetime from ISO text, epoch seconds, a date or a datetime."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _fn_datetime(*args: Any, **kwargs: Any) -> datetime | None:
    if len(args) == 1 and not kwargs:
        return parse_datetime(args[0])
    return datetime(*args, **kwargs)


def _fn_date(*args: Any) -> date | None:
    if len(args) == 1:
        parsed = parse_datetime(args[0])
        return None if parsed is None else parsed.date()
    return date(*args)


def _fn_now() -> datetime:
    return datetime.now(timezone.utc)


def _vector(value: Any) -> list[float] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        try:
            return [float(x) for x in value]
        except (TypeError, ValueError):
            return None
    return None


def cosine_distance(a: Any, b: Any) -> float | None:
    """Cosine distance (1 - cosine similarity); ``None`` when undefined."""
    va, vb = _vector(a), _vector(b)
    if va is None or vb is None or len(va) != len(vb) or not va:
        return None
    dot = sum(x * y for x, y in zip(va, vb))
    na = math.sqrt(sum(x * x for x in va))
    nb = math.sqrt(sum(y * y for y in vb))
    if na == 0.0 or nb == 0.0:
        return None
    return 1.0 - dot / (na * nb)


def l2_distance(a: Any, b: Any) -> float | None:
    va, vb = _vector(a), _vector(b)
    if va is None or vb is None or len(va) != len(vb):
        return None
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(va, vb)))


def inner_product_distance(a: Any, b: Any) -> float | None:
    va, vb = _vector(a), _vector(b)
    if va is None or vb is None or len(va) != len(vb):
        return None
    return -sum(x * y for x, y in zip(va, vb))


def _fn_embed(
    text: Any,
    model: str | None = None,
    **_ignored: Any,
) -> list[float] | None:
    if text is None:
        return None
    from . import embeddings

    return embeddings.embed(str(text), model=model)


def _fn_exists(value: Any) -> bool:
    return value is not None


def _null_tolerant(fn: Callable[..., Any]) -> Callable[..., Any]:
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if any(_is_null(a) for a in args):
            return None
        try:
            return fn(*args, **kwargs)
        except (TypeError, ValueError):
            return None

    return wrapped


def _fn_json(value: Any) -> Any:
    import json

    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return json.dumps(value, default=str)


FUNCTIONS: dict[str, Callable[..., Any]] = {
    "len": _null_tolerant(len),
    "str": lambda v: None if v is None else str(v),
    "int": _null_tolerant(int),
    "float": _null_tolerant(float),
    "bool": lambda v: bool(v),
    "abs": _null_tolerant(abs),
    "round": _null_tolerant(round),
    "min": _null_tolerant(min),
    "max": _null_tolerant(max),
    "sum": _null_tolerant(sum),
    "any": _null_tolerant(any),
    "all": _null_tolerant(all),
    "sorted": _null_tolerant(sorted),
    "list": _null_tolerant(list),
    "dict": _null_tolerant(dict),
    "set": _null_tolerant(set),
    "tuple": _null_tolerant(tuple),
    "range": range,
    "zip": zip,
    "enumerate": enumerate,
    "mean": _null_tolerant(statistics.fmean),
    "exists": _fn_exists,
    "embed": _fn_embed,
    "cosine": cosine_distance,
    "l2": l2_distance,
    "ip": inner_product_distance,
    "now": _fn_now,
    "datetime": _fn_datetime,
    "date": _fn_date,
    "timedelta": timedelta,
    "json": _fn_json,
    "lower": _null_tolerant(lambda s: str(s).lower()),
    "upper": _null_tolerant(lambda s: str(s).upper()),
    "contains": _contains,
}

_CONSTANTS = {"True": True, "False": False, "None": None}


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------


class _Scope:
    """A row plus comprehension-local bindings."""

    __slots__ = ("row", "locals", "aliases")

    def __init__(
        self,
        row: Row,
        aliases: Mapping[str, Row] | None = None,
        local_vars: dict[str, Any] | None = None,
    ) -> None:
        self.row = row
        self.aliases = aliases or {}
        self.locals = local_vars or {}

    def child(self, bindings: dict[str, Any]) -> "_Scope":
        merged = dict(self.locals)
        merged.update(bindings)
        return _Scope(self.row, self.aliases, merged)


def _lookup_field(row: Row, name: str) -> Any:
    if name in row:
        return row[name]
    if name in ROW_ID_NAMES:
        for alt in ROW_ID_NAMES:
            if alt in row:
                return row[alt]
    if name in TIMESTAMP_NAMES:
        for alt in TIMESTAMP_NAMES:
            if alt in row:
                return row[alt]
    return None


class _Compiler(ast.NodeVisitor):
    """Turn an expression AST into a closure ``scope -> value``."""

    def __init__(self, source: str, aliases: tuple[str, ...]) -> None:
        self.source = source
        self.aliases = set(aliases)

    def fail(self, node: ast.AST, message: str) -> InvalidExpression:
        return InvalidExpression(f"{message} in expression {self.source!r}")

    def generic_visit(self, node: ast.AST) -> Any:
        raise self.fail(node, f"Unsupported syntax {type(node).__name__}")

    # Leaves -------------------------------------------------------------

    def visit_Constant(self, node: ast.Constant) -> Compiled:
        value = node.value
        return lambda scope: value

    def visit_Name(self, node: ast.Name) -> Compiled:
        name = node.id
        if name in _CONSTANTS:
            value = _CONSTANTS[name]
            return lambda scope: value
        if name in self.aliases:
            return lambda scope: scope.aliases.get(name)

        def load(scope: _Scope) -> Any:
            if name in scope.locals:
                return scope.locals[name]
            return _lookup_field(scope.row, name)

        return load

    # Containers ---------------------------------------------------------

    def visit_List(self, node: ast.List) -> Compiled:
        items = [self.visit(e) for e in node.elts]
        return lambda scope: [f(scope) for f in items]

    def visit_Tuple(self, node: ast.Tuple) -> Compiled:
        items = [self.visit(e) for e in node.elts]
        return lambda scope: tuple(f(scope) for f in items)

    def visit_Set(self, node: ast.Set) -> Compiled:
        items = [self.visit(e) for e in node.elts]
        return lambda scope: {f(scope) for f in items}

    def visit_Dict(self, node: ast.Dict) -> Compiled:
        pairs = []
        for key, value in zip(node.keys, node.values):
            if key is None:
                raise self.fail(node, "Dict unpacking is not supported")
            pairs.append((self.visit(key), self.visit(value)))
        return lambda scope: {k(scope): v(scope) for k, v in pairs}

    # Operators ----------------------------------------------------------

    def visit_BoolOp(self, node: ast.BoolOp) -> Compiled:
        parts = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):

            def run_and(scope: _Scope) -> Any:
                result: Any = True
                for part in parts:
                    result = part(scope)
                    if not result:
                        return result
                return result

            return run_and

        def run_or(scope: _Scope) -> Any:
            result: Any = False
            for part in parts:
                result = part(scope)
                if result:
                    return result
            return result

        return run_or

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Compiled:
        operand = self.visit(node.operand)
        if isinstance(node.op, ast.Not):
            return lambda scope: not operand(scope)
        if isinstance(node.op, ast.USub):
            return lambda scope: _binary(ast.Sub, 0, operand(scope))
        if isinstance(node.op, ast.UAdd):
            return operand
        raise self.fail(node, f"Unsupported unary operator {type(node.op).__name__}")

    def visit_BinOp(self, node: ast.BinOp) -> Compiled:
        left, right = self.visit(node.left), self.visit(node.right)
        op = type(node.op)
        if op not in _BINARY:
            raise self.fail(node, f"Unsupported operator {op.__name__}")
        return lambda scope: _binary(op, left(scope), right(scope))

    def visit_Compare(self, node: ast.Compare) -> Compiled:
        left = self.visit(node.left)
        ops = [type(op) for op in node.ops]
        rights = [self.visit(c) for c in node.comparators]

        def run(scope: _Scope) -> Any:
            current = left(scope)
            for op, right in zip(ops, rights):
                other = right(scope)
                verdict = _compare(op, current, other)
                if not verdict:
                    return verdict
                current = other
            return True

        return run

    def visit_IfExp(self, node: ast.IfExp) -> Compiled:
        test, body, orelse = (
            self.visit(node.test),
            self.visit(node.body),
            self.visit(node.orelse),
        )
        return lambda scope: body(scope) if test(scope) else orelse(scope)

    # Access -------------------------------------------------------------

    def visit_Subscript(self, node: ast.Subscript) -> Compiled:
        value = self.visit(node.value)
        if isinstance(node.slice, ast.Slice):
            lower = self.visit(node.slice.lower) if node.slice.lower else None
            upper = self.visit(node.slice.upper) if node.slice.upper else None
            step = self.visit(node.slice.step) if node.slice.step else None

            def run_slice(scope: _Scope) -> Any:
                target = value(scope)
                if target is None:
                    return None
                try:
                    return target[
                        slice(
                            lower(scope) if lower else None,
                            upper(scope) if upper else None,
                            step(scope) if step else None,
                        )
                    ]
                except (TypeError, KeyError, IndexError):
                    return None

            return run_slice
        index = self.visit(node.slice)

        def run(scope: _Scope) -> Any:
            target = value(scope)
            key = index(scope)
            if target is None or key is None:
                return None
            try:
                return target[key]
            except (TypeError, KeyError, IndexError):
                return None

        return run

    def visit_Attribute(self, node: ast.Attribute) -> Compiled:
        value = self.visit(node.value)
        attr = node.attr
        if attr.startswith("_"):
            raise self.fail(node, f"Attribute {attr!r} is not allowed")

        def run(scope: _Scope) -> Any:
            target = value(scope)
            if target is None:
                return None
            if isinstance(target, Mapping):
                if attr in target:
                    return target[attr]
                bound = getattr(target, attr, None)
                return bound if callable(bound) else None
            return getattr(target, attr, None)

        return run

    def visit_Call(self, node: ast.Call) -> Compiled:
        args = [self.visit(a) for a in node.args]
        kwargs = {}
        for kw in node.keywords:
            if kw.arg is None:
                raise self.fail(node, "Keyword unpacking is not supported")
            kwargs[kw.arg] = self.visit(kw.value)
        for arg in node.args:
            if isinstance(arg, ast.Starred):
                raise self.fail(node, "Star arguments are not supported")

        if isinstance(node.func, ast.Name) and node.func.id in FUNCTIONS:
            fn = FUNCTIONS[node.func.id]

            def call_builtin(scope: _Scope) -> Any:
                return fn(
                    *[a(scope) for a in args],
                    **{k: v(scope) for k, v in kwargs.items()},
                )

            return call_builtin

        if isinstance(node.func, ast.Attribute):
            method = self.visit(node.func)

            def call_method(scope: _Scope) -> Any:
                bound = method(scope)
                if bound is None or not callable(bound):
                    return None
                call_args = [a(scope) for a in args]
                call_kwargs = {k: v(scope) for k, v in kwargs.items()}
                try:
                    return bound(*call_args, **call_kwargs)
                except (TypeError, ValueError, AttributeError):
                    return None

            return call_method

        if isinstance(node.func, ast.Name):
            raise self.fail(node, f"Unknown function {node.func.id!r}")
        raise self.fail(node, "Unsupported call target")

    # Comprehensions -----------------------------------------------------

    def _comprehension(
        self,
        node: ast.ListComp | ast.SetComp | ast.GeneratorExp | ast.DictComp,
    ) -> Callable[[_Scope], list[Any]]:
        if isinstance(node, ast.DictComp):
            key_fn, value_fn = self.visit(node.key), self.visit(node.value)
            element = lambda scope: (key_fn(scope), value_fn(scope))
        else:
            element = self.visit(node.elt)
        generators = []
        for gen in node.generators:
            if gen.is_async:
                raise self.fail(node, "Async comprehensions are not supported")
            generators.append(
                (
                    self._target_names(gen.target),
                    self.visit(gen.iter),
                    [self.visit(cond) for cond in gen.ifs],
                ),
            )

        def run(scope: _Scope) -> list[Any]:
            results: list[Any] = []

            def recurse(depth: int, current: _Scope) -> None:
                if depth == len(generators):
                    results.append(element(current))
                    return
                names, iterable, conditions = generators[depth]
                source = iterable(current)
                if source is None:
                    return
                for item in source:
                    bindings = _bind(names, item)
                    inner = current.child(bindings)
                    if all(cond(inner) for cond in conditions):
                        recurse(depth + 1, inner)

            recurse(0, scope)
            return results

        return run

    def _target_names(self, target: ast.expr) -> Any:
        if isinstance(target, ast.Name):
            return target.id
        if isinstance(target, (ast.Tuple, ast.List)):
            return tuple(self._target_names(e) for e in target.elts)
        raise self.fail(target, "Unsupported comprehension target")

    def visit_ListComp(self, node: ast.ListComp) -> Compiled:
        run = self._comprehension(node)
        return run

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> Compiled:
        run = self._comprehension(node)
        return run

    def visit_SetComp(self, node: ast.SetComp) -> Compiled:
        run = self._comprehension(node)
        return lambda scope: set(run(scope))

    def visit_DictComp(self, node: ast.DictComp) -> Compiled:
        run = self._comprehension(node)
        return lambda scope: dict(run(scope))


def _bind(names: Any, item: Any) -> dict[str, Any]:
    if isinstance(names, str):
        return {names: item}
    bindings: dict[str, Any] = {}
    for name, value in zip(names, item):
        bindings.update(_bind(name, value))
    return bindings


class Expression:
    """A compiled expression that can be evaluated against rows."""

    __slots__ = ("source", "_run", "_aliases")

    def __init__(self, source: str, aliases: tuple[str, ...] = ()) -> None:
        self.source = source
        self._aliases = aliases
        rewritten = rewrite_placeholders(source)
        try:
            tree = ast.parse(rewritten.strip(), mode="eval")
        except SyntaxError as exc:
            raise InvalidExpression(
                f"Could not parse expression {source!r}: {exc.msg}",
            ) from exc
        self._run = _Compiler(source, aliases).visit(tree.body)

    def evaluate(self, row: Row, aliases: Mapping[str, Row] | None = None) -> Any:
        return self._run(_Scope(row, aliases))

    def matches(self, row: Row, aliases: Mapping[str, Row] | None = None) -> bool:
        return bool(self.evaluate(row, aliases))

    def __repr__(self) -> str:
        return f"Expression({self.source!r})"


_CACHE: dict[tuple[str, tuple[str, ...]], Expression] = {}


def compile_expression(source: str, aliases: tuple[str, ...] = ()) -> Expression:
    """Compile (and memoise) an expression string."""
    key = (source, aliases)
    cached = _CACHE.get(key)
    if cached is None:
        if len(_CACHE) > 4096:
            _CACHE.clear()
        cached = _CACHE[key] = Expression(source, aliases)
    return cached
