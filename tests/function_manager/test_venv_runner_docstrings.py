"""Venv RPC proxies must mirror their canonical sandbox-helper docstrings.

``venv_runner.py`` ships standalone into custom venvs and cannot import
``unify``, so its ``query_llm`` / ``list_llms`` proxies carry literal copies
of the canonical docstrings. Once the actor prompt stops inlining this
teaching, ``help(...)`` inside a venv session is the only in-sandbox source,
so a stub would silently regress venv sessions. These pins fail whenever the
canonical docstring changes without the mirror being re-copied.
"""

from __future__ import annotations

import inspect

from unify.common import reasoning
from unify.function_manager import venv_runner


def test_venv_query_llm_docstring_mirrors_reasoning_helper():
    assert inspect.getdoc(venv_runner.query_llm) == inspect.getdoc(
        reasoning.query_llm,
    )


def test_venv_list_llms_docstring_mirrors_reasoning_helper():
    assert inspect.getdoc(venv_runner.list_llms) == inspect.getdoc(
        reasoning.list_llms,
    )


def test_venv_query_llm_docstring_carries_model_selection_teaching():
    """The 'Choosing A Model' teaching folded into query_llm.__doc__ reaches
    venv sessions through the mirror."""
    doc = inspect.getdoc(venv_runner.query_llm) or ""
    assert "### Choosing A Model For `query_llm(...)`" in doc
    assert "Artificial Analysis (https://artificialanalysis.ai/)" in doc
    selection = reasoning.get_llm_model_selection_context()
    assert selection.startswith("### Choosing A Model")  # extraction non-empty
    assert selection in doc
