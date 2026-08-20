"""Tier-0 contracts: JSON schemas from type hints and authored postconditions.

The contract is the cheapest verifier: microseconds, no model. Input and
output schemas come from the stored function's annotations via pydantic
``TypeAdapter``; parameters and returns without a resolvable annotation
contribute nothing. Postconditions are boolean expressions over ``result``
and ``kwargs`` that a librarian authors; each is compiled at store time and
may reference nothing beyond those two names, an allowlist of builtins and
the standard comparison/boolean operators.
"""

from __future__ import annotations

import ast
import inspect
import typing
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional

import jsonschema
from pydantic import TypeAdapter

from ..types.verification import FunctionContract

_UNTYPED = (inspect.Parameter.empty, Any, typing.Any)

POSTCONDITION_ALLOWED_BUILTINS: Dict[str, Any] = {
    "len": len,
    "all": all,
    "any": any,
    "isinstance": isinstance,
    "min": min,
    "max": max,
    "sum": sum,
    "abs": abs,
    "round": round,
    "sorted": sorted,
    "set": set,
    "list": list,
    "dict": dict,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
}
POSTCONDITION_ALLOWED_NAMES = frozenset(
    {"result", "kwargs", *POSTCONDITION_ALLOWED_BUILTINS},
)

_ALLOWED_NODES = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.USub,
    ast.UAdd,
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.Is,
    ast.IsNot,
    ast.In,
    ast.NotIn,
    ast.IfExp,
    ast.Call,
    ast.Name,
    ast.Load,
    ast.Store,
    ast.Constant,
    ast.Subscript,
    ast.Slice,
    ast.Tuple,
    ast.List,
    ast.Set,
    ast.Dict,
    ast.ListComp,
    ast.SetComp,
    ast.GeneratorExp,
    ast.comprehension,
    ast.Attribute,
    ast.keyword,
    ast.Starred,
)

# ``result.items()``-style calls on the values under test are legitimate;
# attribute access is otherwise the road to ``__class__``/``__globals__``.
_ALLOWED_ATTRIBUTES = frozenset(
    {
        "items",
        "keys",
        "values",
        "get",
        "lower",
        "upper",
        "strip",
        "startswith",
        "endswith",
        "isdigit",
        "isalpha",
        "count",
        "index",
        "split",
    },
)


class PostconditionError(ValueError):
    """A postcondition expression is not admissible."""


def compile_postcondition(expression: str) -> str:
    """Validate one postcondition and return it normalised.

    Raises ``PostconditionError`` when the expression is not a pure boolean
    expression over ``result`` / ``kwargs`` and the allowlisted builtins.
    """
    text = str(expression).strip()
    if not text:
        raise PostconditionError("Empty postcondition.")
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError as exc:
        raise PostconditionError(
            f"Postcondition is not a valid expression: {exc.msg}",
        ) from exc
    bound_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.comprehension):
            for target in ast.walk(node.target):
                if isinstance(target, ast.Name):
                    bound_names.add(target.id)
    for node in ast.walk(tree):
        if not isinstance(node, _ALLOWED_NODES):
            raise PostconditionError(
                f"Postcondition uses disallowed syntax {type(node).__name__!r}: {text}",
            )
        if isinstance(node, ast.Name):
            if (
                node.id not in POSTCONDITION_ALLOWED_NAMES
                and node.id not in bound_names
            ):
                raise PostconditionError(
                    f"Postcondition references {node.id!r}; only result, kwargs and "
                    f"{sorted(POSTCONDITION_ALLOWED_BUILTINS)} are allowed: {text}",
                )
        if isinstance(node, ast.Attribute):
            if node.attr.startswith("_") or node.attr not in _ALLOWED_ATTRIBUTES:
                raise PostconditionError(
                    f"Postcondition accesses attribute {node.attr!r}, which is not allowed: {text}",
                )
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name):
                if func.id not in POSTCONDITION_ALLOWED_BUILTINS:
                    raise PostconditionError(
                        f"Postcondition calls {func.id!r}, which is not an allowed builtin: {text}",
                    )
            elif not isinstance(func, ast.Attribute):
                raise PostconditionError(
                    f"Postcondition calls a non-name callable: {text}",
                )
    return text


def compile_postconditions(expressions: Iterable[str]) -> List[str]:
    return [compile_postcondition(expr) for expr in expressions]


def evaluate_postcondition(
    expression: str,
    *,
    result: Any,
    kwargs: Mapping[str, Any],
) -> bool:
    """Evaluate a compiled postcondition in a namespace holding only the allowed names."""
    code = compile(ast.parse(expression, mode="eval"), "<postcondition>", "eval")
    namespace = {
        "__builtins__": {},
        **POSTCONDITION_ALLOWED_BUILTINS,
        "result": result,
        "kwargs": dict(kwargs),
    }
    return bool(eval(code, namespace, {}))


def _schema_for(annotation: Any) -> Optional[Dict[str, Any]]:
    if annotation in _UNTYPED:
        return None
    try:
        return TypeAdapter(annotation).json_schema()
    except Exception:
        return None


def contract_from_callable(fn: Callable[..., Any]) -> FunctionContract:
    """Build the type-hint contract of ``fn``; ``source='none'`` when hints yield nothing."""
    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        hints = dict(getattr(fn, "__annotations__", {}) or {})
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return FunctionContract()

    properties: Dict[str, Any] = {}
    required: list[str] = []
    for name, param in signature.parameters.items():
        if param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        schema = _schema_for(hints.get(name, param.annotation))
        if schema is None:
            continue
        properties[name] = schema
        if param.default is inspect.Parameter.empty:
            required.append(name)

    input_schema: Optional[Dict[str, Any]] = None
    if properties:
        input_schema = {
            "type": "object",
            "properties": properties,
            "required": required,
        }
    output_schema = _schema_for(hints.get("return", signature.return_annotation))
    if input_schema is None and output_schema is None:
        return FunctionContract()
    return FunctionContract(
        input_schema=input_schema,
        output_schema=output_schema,
        source="type_hints",
    )


def merge_contract(
    hinted: FunctionContract,
    authored: Optional[FunctionContract | Mapping[str, Any]],
) -> FunctionContract:
    """Combine the type-hint contract with a librarian-authored one.

    Schemas come from hints; the librarian contributes postconditions (each
    compiled and validated here) and may supply schemas only where hints
    yielded none.
    """
    if authored is None:
        return hinted
    if not isinstance(authored, FunctionContract):
        authored = FunctionContract.model_validate(dict(authored))
    postconditions = compile_postconditions(authored.postconditions)
    input_schema = (
        hinted.input_schema
        if hinted.input_schema is not None
        else authored.input_schema
    )
    output_schema = (
        hinted.output_schema
        if hinted.output_schema is not None
        else authored.output_schema
    )
    if postconditions:
        source = "librarian"
    elif input_schema is not None or output_schema is not None:
        source = "type_hints" if hinted.source == "type_hints" else "librarian"
    else:
        source = "none"
    return FunctionContract(
        input_schema=input_schema,
        output_schema=output_schema,
        postconditions=postconditions,
        source=source,
    )


def _coerce_contract(
    contract: FunctionContract | Mapping[str, Any] | None,
) -> FunctionContract:
    if contract is None:
        return FunctionContract()
    if isinstance(contract, FunctionContract):
        return contract
    return FunctionContract.model_validate(dict(contract))


def _jsonable(value: Any) -> Any:
    """Best-effort conversion to the JSON model jsonschema validates."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "model_dump"):
        try:
            return _jsonable(value.model_dump(mode="json"))
        except Exception:
            return str(value)
    return str(value)


def _first_schema_error(schema: Dict[str, Any], value: Any) -> Optional[str]:
    validator = jsonschema.Draft202012Validator(schema)
    error = jsonschema.exceptions.best_match(validator.iter_errors(_jsonable(value)))
    if error is None:
        return None
    path = "/".join(str(p) for p in error.absolute_path)
    where = f" at {path}" if path else ""
    return f"{error.message}{where}"


def check_input(
    contract: FunctionContract | Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> Optional[str]:
    """Return a reason when ``kwargs`` violate the input schema, else None."""
    resolved = _coerce_contract(contract)
    if resolved.input_schema is None:
        return None
    error = _first_schema_error(resolved.input_schema, dict(kwargs))
    return f"input contract violated: {error}" if error else None


def check_output(
    contract: FunctionContract | Mapping[str, Any] | None,
    *,
    result: Any,
    kwargs: Mapping[str, Any],
) -> Optional[str]:
    """Return a reason when ``result`` violates the output schema or a postcondition."""
    resolved = _coerce_contract(contract)
    if resolved.output_schema is not None:
        error = _first_schema_error(resolved.output_schema, result)
        if error:
            return f"output contract violated: {error}"
    for expression in resolved.postconditions:
        try:
            holds = evaluate_postcondition(expression, result=result, kwargs=kwargs)
        except Exception as exc:
            return f"postcondition {expression!r} raised {type(exc).__name__}: {exc}"
        if not holds:
            return f"postcondition {expression!r} is false"
    return None
