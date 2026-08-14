"""Venv RPC proxies must mirror their canonical sandbox-helper docstrings.

``venv_runner.py`` ships standalone into custom venvs and cannot import
``unify``, so its ``query_llm`` / ``list_llms`` / ``get_oauth_access_token``
proxies carry literal copies of the canonical docstrings. Once the actor
prompt stops inlining this teaching, ``help(...)`` inside a venv session is
the only in-sandbox source — a stub (or a wrong OAuth contract, as the old
stub had) would silently regress venv sessions. These pins fail whenever the
canonical docstring changes without the mirror being re-copied.
"""

from __future__ import annotations

import inspect

from unify.common import reasoning, runtime_oauth
from unify.function_manager import venv_runner


def test_venv_query_llm_docstring_mirrors_reasoning_helper():
    assert inspect.getdoc(venv_runner.query_llm) == inspect.getdoc(
        reasoning.query_llm,
    )


def test_venv_list_llms_docstring_mirrors_reasoning_helper():
    assert inspect.getdoc(venv_runner.list_llms) == inspect.getdoc(
        reasoning.list_llms,
    )


def test_venv_oauth_docstring_mirrors_proxy_nonce_contract():
    doc = inspect.getdoc(venv_runner.get_oauth_access_token)
    assert doc == inspect.getdoc(runtime_oauth.get_oauth_access_token)
    # The contract the old stub contradicted: the helper returns a workspace
    # proxy capability handle, never a raw provider token.
    assert "does NOT return a raw provider access token" in doc
    assert "capability handle" in doc
    assert "MICROSOFT_GRAPH_BASE" in doc


def test_venv_query_llm_docstring_carries_model_selection_teaching():
    """The 'Choosing A Model' teaching folded into query_llm.__doc__ reaches
    venv sessions through the mirror."""
    doc = inspect.getdoc(venv_runner.query_llm) or ""
    assert "### Choosing A Model For `query_llm(...)`" in doc
    assert "Artificial Analysis (https://artificialanalysis.ai/)" in doc
    selection = reasoning.get_llm_model_selection_context()
    assert selection.startswith("### Choosing A Model")  # extraction non-empty
    assert selection in doc
