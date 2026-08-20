"""Filenames for compiled stored-function sources.

Every stored function is compiled under ``<function:NAME>`` so a Python stack
frame or traceback entry names the stored function it belongs to. The
verification runtime reads call sites from these frames and repair maps an
exception back to the innermost stored function that raised it.
"""

from __future__ import annotations

import linecache
from types import CodeType
from typing import Optional

_PREFIX = "<function:"
_SUFFIX = ">"


def function_source_filename(name: str) -> str:
    return f"{_PREFIX}{name}{_SUFFIX}"


def function_name_from_filename(filename: Optional[str]) -> Optional[str]:
    """The stored function a compiled filename labels, or None for anything else."""
    if (
        not filename
        or not filename.startswith(_PREFIX)
        or not filename.endswith(_SUFFIX)
    ):
        return None
    return filename[len(_PREFIX) : -len(_SUFFIX)] or None


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


def executed_source_lines(name: str) -> Optional[list[str]]:
    """The lines last compiled for ``name``, or None if it was never compiled here."""
    entry = linecache.cache.get(function_source_filename(name))
    if entry is None:
        return None
    return list(entry[2])
