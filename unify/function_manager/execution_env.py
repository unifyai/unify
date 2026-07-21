"""
Utilities for creating function execution environments.

Provides controlled global namespaces for executing user-defined and
compositional functions with:
- Safe built-in functions (excluding dangerous ones like eval, exec, open)
- Common standard library modules (asyncio, re, json, datetime, collections)
- Typing module and common type hints
- Pydantic support (if available)
- Access to primitives (state managers, computer use)
- Steerable handle infrastructure (for functions that return steerable handles)
"""

import asyncio
import collections
import datetime
import functools
import json
import re
import typing
from typing import Any, Dict

import unillm

try:
    import pydantic
    from pydantic import BaseModel, Field

    HAS_PYDANTIC = True
except ImportError:
    pydantic = None
    BaseModel = None
    Field = None
    HAS_PYDANTIC = False


ENVIRONMENT_MODULES: frozenset[str] = frozenset(
    {
        "primitives",
        "pydantic",
        "unillm",
        "unify",
    },
)
"""Non-stdlib module names that the function execution environment provides.

Used by ``detect_third_party_imports`` to distinguish between packages that
need a venv and packages that the runtime already supplies.
"""


def create_base_globals() -> Dict[str, Any]:
    """
    Creates a dictionary of safe global functions for code execution.

    This provides a controlled environment with:
    - Safe built-in functions (excluding dangerous ones like eval, exec)
    - Common standard library modules (asyncio, re, json, datetime, collections)
    - Typing module and common type hints
    - Pydantic support (if available)

    Returns:
        A dictionary of globals allowed within the execution environment.
    """
    # Safe builtins - excluding dangerous functions
    safe_builtins = {
        k: __builtins__.get(k)
        for k in [
            "repr",
            "print",
            "len",
            "str",
            "int",
            "float",
            "bool",
            "list",
            "dict",
            "set",
            "tuple",
            "range",
            "type",
            "object",
            "bytes",
            "frozenset",
            "isinstance",
            "hasattr",
            "getattr",
            "setattr",
            "callable",
            "issubclass",
            "dir",
            "vars",
            "iter",
            "next",
            "filter",
            "map",
            "reversed",
            "enumerate",
            "zip",
            "any",
            "all",
            "sum",
            "min",
            "max",
            "abs",
            "round",
            "pow",
            "divmod",
            "sorted",
            "format",
            "chr",
            "ord",
            "id",
            "hash",
            "hex",
            "oct",
            "bin",
            "bytearray",
            "memoryview",
            "complex",
            "slice",
            # Exception hierarchy
            "Exception",
            "BaseException",
            "ArithmeticError",
            "LookupError",
            "NotImplementedError",
            "ValueError",
            "TypeError",
            "KeyError",
            "IndexError",
            "AttributeError",
            "RuntimeError",
            "StopIteration",
            "StopAsyncIteration",
            "AssertionError",
            "NameError",
            "ImportError",
            "ModuleNotFoundError",
            "OSError",
            "IOError",
            "FileNotFoundError",
            "FileExistsError",
            "PermissionError",
            "IsADirectoryError",
            "NotADirectoryError",
            "EOFError",
            "UnicodeError",
            "UnicodeDecodeError",
            "UnicodeEncodeError",
            "ZeroDivisionError",
            "OverflowError",
            "TimeoutError",
            "ConnectionError",
            "BrokenPipeError",
            # File I/O
            "open",
            # Class-related
            "super",
            "property",
            "classmethod",
            "staticmethod",
            "__build_class__",
            "__name__",
            "__import__",
        ]
        if __builtins__.get(k) is not None
    }

    globals_dict = {
        "__builtins__": safe_builtins,
        # Standard library modules
        "asyncio": asyncio,
        "re": re,
        "json": json,
        "datetime": datetime,
        "collections": collections,
        # Additional useful modules
        "statistics": __import__("statistics"),
        # Typing module and common types
        "typing": typing,
        "Any": typing.Any,
        "Callable": typing.Callable,
        "Dict": typing.Dict,
        "List": typing.List,
        "Optional": typing.Optional,
        "Tuple": typing.Tuple,
        "Set": typing.Set,
        "Union": typing.Union,
        "Literal": typing.Literal,
        "functools": functools,
    }

    if HAS_PYDANTIC:
        globals_dict.update(
            {
                "pydantic": pydantic,
                "BaseModel": BaseModel,
                "Field": Field,
            },
        )

    return globals_dict


def create_execution_globals() -> Dict[str, Any]:
    """
    Creates execution globals for running stored functions.

    Extends create_base_globals() with:
    - The `primitives` object for lazy access to all primitive operations
      (state managers, computer use, etc.)
    - Steerable handle infrastructure for functions that return handles
      (SteerableToolHandle)
    - The `unillm` module for advanced direct LLM usage
    - The `query_llm` helper for focused one-shot LLM queries
    - The `run_coro_sync` helper for sync façades that must drive async work
      under an already-running event loop (offline Jobs / actor sandboxes)

    All primitive imports and instantiations are lazy - only the primitives
    actually used by a function are loaded. This means functions that don't
    need computer use won't import web/desktop infrastructure.

    Steerable Functions
    -------------------
    Functions can return a ``SteerableToolHandle`` subclass to indicate they
    are steerable. The execution layer will detect this at runtime via
    ``isinstance(result, SteerableToolHandle)`` and wire up steering operations.

    Returns:
        A dictionary of globals for function execution.
    """
    globals_dict = create_base_globals()

    # Import Primitives here to avoid circular imports at module load time
    from unify.function_manager.primitives import Primitives, default_runtime_scope

    # Inject the primitives instance - all access is lazy
    globals_dict["primitives"] = Primitives(primitive_scope=default_runtime_scope())

    # Steerable handle type - allows compositional functions to return handles
    # that the execution layer can detect and wire up for steering operations.
    from unify.common.async_tool_loop import SteerableToolHandle
    from unify.common.asyncio_compat import run_coro_sync
    from unify.common.reasoning import list_llms, query_llm
    from unify.common.runtime_oauth import get_oauth_access_token

    globals_dict["SteerableToolHandle"] = SteerableToolHandle
    globals_dict["query_llm"] = query_llm
    globals_dict["list_llms"] = list_llms
    globals_dict["get_oauth_access_token"] = get_oauth_access_token
    globals_dict["run_coro_sync"] = run_coro_sync
    globals_dict["unillm"] = unillm

    return globals_dict


# Backward compatibility aliases
create_sandbox_globals = create_base_globals
