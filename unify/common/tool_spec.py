from dataclasses import dataclass
from typing import Any, Callable, Optional, Dict, Union

# Attribute used by `llm_soft_required` to stash per-argument runtime defaults
# on a tool callable. The async tool loop reads it when dispatching base tools.
LLM_SOFT_REQUIRED_DEFAULTS_ATTR = "_llm_soft_required_defaults"

# ─────────────────────────────────────────────────────────────────────────────
# 0.  metadata wrapper - lets us attach `max_concurrent` to a tool
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class ToolSpec:
    """Wrap the callable with optional metadata (e.g., max_concurrent)."""

    fn: Callable
    max_concurrent: Optional[int] = None  # «None» ⇒ unlimited
    # Hidden per-loop quota: when set, the tool will only be callable
    # `max_total_calls` times within a single async tool-use loop. Once
    # exhausted, the tool is silently hidden from the exposed schema and
    # any additional invocations are minimally acknowledged without
    # revealing quota details to the LLM.
    max_total_calls: Optional[int] = None
    read_only: Optional[bool] = None
    manager_tool: bool = False
    display_label: Optional[Union[str, Callable[[Dict[str, Any]], str]]] = None

    # Let a ToolSpec be invoked like the underlying callable (nice for tests)
    def __call__(self, *a, **kw):  # pragma: no cover
        return self.fn(*a, **kw)


def normalise_tools(
    raw: Dict[str, Union[Callable, "ToolSpec"]],
) -> Dict[str, "ToolSpec"]:
    """Return a uniform ``dict[name → ToolSpec]`` from callables or ToolSpec values."""
    out: Dict[str, ToolSpec] = {}
    for n, v in raw.items():
        if isinstance(v, ToolSpec):
            if v.read_only is None or not v.manager_tool:
                fn_ro = getattr(v.fn, "_tool_spec_read_only", None)
                fn_mt = getattr(v.fn, "_tool_spec_manager_tool", False)
                if (v.read_only is None and fn_ro is not None) or (
                    not v.manager_tool and fn_mt
                ):
                    v = ToolSpec(
                        fn=v.fn,
                        max_concurrent=v.max_concurrent,
                        max_total_calls=v.max_total_calls,
                        read_only=v.read_only if v.read_only is not None else fn_ro,
                        manager_tool=v.manager_tool or fn_mt,
                        display_label=v.display_label,
                    )
            out[n] = v
        else:
            out[n] = ToolSpec(
                fn=v,
                read_only=getattr(v, "_tool_spec_read_only", None),
                manager_tool=getattr(v, "_tool_spec_manager_tool", None),
            )
    return out


def read_only(fn: Callable) -> Callable:
    """Mark a tool as read-only; eligible for semantic-cache re-execution."""
    setattr(fn, "_tool_spec_read_only", True)
    return fn


def manager_tool(fn: Callable) -> Callable:
    """Mark a tool as a manager tool; used to call other tools."""
    setattr(fn, "_tool_spec_manager_tool", True)
    return fn


def llm_soft_required(**defaults: Any) -> Callable[[Callable], Callable]:
    """Advertise parameters as required in the tool schema while keeping them
    optional at runtime.

    A parameter declared here stays in the schema's ``required`` array (so the
    model is strongly nudged to supply it on every call), but if the model
    omits it the tool loop backfills the configured default instead of letting
    ``fn(**kwargs)`` raise ``TypeError``.

    Use only for advisory, non-functional arguments — for example
    ``execute_code``'s ``thought``, a user-facing rationale string that is
    logged but never read programmatically. Functional required parameters must
    not be listed here: a genuine omission should surface as an error the model
    can self-correct against.
    """

    def _decorator(fn: Callable) -> Callable:
        setattr(fn, LLM_SOFT_REQUIRED_DEFAULTS_ATTR, dict(defaults))
        return fn

    return _decorator
