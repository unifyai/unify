"""Filenames for compiled stored-function sources.

Every stored function is compiled under ``<function:NAME>`` so a Python stack
frame or traceback entry names the stored function it belongs to, and
``linecache`` can hand back the exact source that ran.
"""

from __future__ import annotations

import linecache
from types import CodeType

_PREFIX = "<function:"
_SUFFIX = ">"


def function_source_filename(name: str) -> str:
    return f"{_PREFIX}{name}{_SUFFIX}"


def compile_function_source(name: str, source: str) -> CodeType:
    """Compile ``source`` under the function's label and register it with ``linecache``.

    Registering the text lets ``traceback`` and the verification runtime read
    the exact executed lines back from a frame, including sources that were
    rewritten (decorators stripped, steering probes inserted) before compiling.
    """
    filename = function_source_filename(name)
    lines = source.splitlines(keepends=True)
    linecache.cache[filename] = (len(source), None, lines, filename)
    return compile(source, filename, "exec")
