"""A manager's base class is its contract, and the actor must read it.

The public docstrings on ``Base{Manager}`` are the LLM-facing API: the
primitives layer turns each manager method into a tool, taking its
description from the docstring and its arguments from the signature. That
only works if the concrete method carries
``@functools.wraps(Base.method, updated=())`` — which is what makes
``inspect.getdoc`` and ``inspect.signature`` resolve to the base.

Two failures this keeps dead, both of which were live when it was written:

- **A bespoke ``__doc__`` copy instead of wraps.** CanvasManager attached
  the base docstrings with a module-level loop. The prose reached the actor
  but the *signature* did not, so a publisher-internal keyword added to the
  concrete method leaked into the tool schema as something the model could
  pass.
- **An abstract base that is not abstract.** ``BaseCanvasManager`` was a
  plain class with ``@abstractmethod`` decorators, which do nothing without
  ``ABCMeta`` — nothing enforced that an implementation existed at all.
"""

from __future__ import annotations

import inspect
from typing import Dict, List

import pytest

# (alias, concrete module, concrete class, base module, base class). The
# managers the actor reaches, plus the two catalogue managers whose tools
# are typed JSON calls rather than primitives — the contract is the same.
MANAGERS = [
    ("guidance", "unify.guidance_manager.guidance_manager", "GuidanceManager"),
    ("knowledge", "unify.knowledge_manager.knowledge_manager", "KnowledgeManager"),
    ("tasks", "unify.task_scheduler.task_scheduler", "TaskScheduler"),
    ("contacts", "unify.contact_manager.contact_manager", "ContactManager"),
    ("data", "unify.data_manager.data_manager", "DataManager"),
    ("functions", "unify.function_manager.function_manager", "FunctionManager"),
    ("secrets", "unify.secret_manager.secret_manager", "SecretManager"),
    ("web", "unify.web_searcher.web_searcher", "WebSearcher"),
    ("transcripts", "unify.transcript_manager.transcript_manager", "TranscriptManager"),
    ("ingestion", "unify.ingestion_manager.ingestion_manager", "IngestionManager"),
    ("canvas", "unify.canvas_manager.canvas_manager", "CanvasManager"),
    ("workflows", "unify.workflow_manager.workflow_manager", "WorkflowManager"),
]

BASES = {
    "guidance": ("unify.guidance_manager.base", "BaseGuidanceManager"),
    "knowledge": ("unify.knowledge_manager.base", "BaseKnowledgeManager"),
    "tasks": ("unify.task_scheduler.base", "BaseTaskScheduler"),
    "contacts": ("unify.contact_manager.base", "BaseContactManager"),
    "data": ("unify.data_manager.base", "BaseDataManager"),
    "functions": ("unify.function_manager.base", "BaseFunctionManager"),
    "secrets": ("unify.secret_manager.base", "BaseSecretManager"),
    "web": ("unify.web_searcher.base", "BaseWebSearcher"),
    "transcripts": ("unify.transcript_manager.base", "BaseTranscriptManager"),
    "ingestion": ("unify.ingestion_manager.base", "BaseIngestionManager"),
    "canvas": ("unify.canvas_manager.base", "BaseCanvasManager"),
    "workflows": ("unify.workflow_manager.base", "BaseWorkflowManager"),
}

# Managers whose remaining drift is known, tracked, and needs its own pass
# because changing a base signature changes the actor's tool schema and
# reprices every cached prompt that carries it. Listed rather than skipped
# silently, so the debt is visible and shrinks rather than being forgotten.
KNOWN_UNWRAPPED: Dict[str, set[str]] = {
    "tasks": {"get_run_event", "get_run_event_children"},
    "contacts": {"filter_contacts", "update_contact"},
    "functions": {"add_functions", "delete_function", "execute_function"},
}


def _load(module: str, name: str):
    import importlib

    return getattr(importlib.import_module(module), name)


def _raw(cls, name):
    """The function object as written, bypassing ``functools.wraps``."""
    for klass in cls.__mro__:
        if name in klass.__dict__:
            func = klass.__dict__[name]
            if isinstance(func, (staticmethod, classmethod)):
                func = func.__func__
            return func
    return None


def _abstract_names(base) -> List[str]:
    return sorted(getattr(base, "__abstractmethods__", set()))


@pytest.mark.parametrize(
    ("alias", "module", "name"),
    MANAGERS,
    ids=[m[0] for m in MANAGERS],
)
def test_the_base_is_actually_abstract(alias: str, module: str, name: str):
    """``@abstractmethod`` on a plain class is inert: it enforces nothing
    and populates no ``__abstractmethods__``, so a missing implementation
    is a silent ``AttributeError`` at call time instead of a refusal at
    construction."""
    base = _load(*BASES[alias])
    assert hasattr(base, "__abstractmethods__"), (
        f"{base.__name__} declares @abstractmethod but is not an ABC; "
        "the decorators do nothing"
    )
    assert _abstract_names(base), f"{base.__name__} declares no abstract methods"


@pytest.mark.parametrize(
    ("alias", "module", "name"),
    MANAGERS,
    ids=[m[0] for m in MANAGERS],
)
def test_public_methods_carry_the_base_contract(alias: str, module: str, name: str):
    """Every abstract method's implementation wraps its base, so the actor
    reads one contract — the base's — for both the prose and the
    arguments."""
    concrete = _load(module, name)
    base = _load(*BASES[alias])
    allowed = KNOWN_UNWRAPPED.get(alias, set())

    unwrapped = []
    for method in _abstract_names(base):
        impl = _raw(concrete, method)
        assert impl is not None, f"{name}.{method} is not implemented"
        if getattr(impl, "__wrapped__", None) is not _raw(base, method):
            unwrapped.append(method)

    unexpected = sorted(set(unwrapped) - allowed)
    assert not unexpected, (
        f"{name}: {unexpected} do not wrap their base contract. Add "
        f"@functools.wraps(Base{name}.<method>, updated=()) so the actor "
        "reads the base docstring and the base signature."
    )
    # And the ledger stays honest in the other direction: a method that has
    # since been wrapped must leave the list rather than sit there implying
    # debt that is already paid.
    stale = sorted(allowed - set(unwrapped))
    assert (
        not stale
    ), f"{name}: {stale} now wrap their base; drop them from KNOWN_UNWRAPPED"


def test_a_publisher_only_keyword_stays_out_of_the_actor_s_schema():
    """The reason wraps beats copying ``__doc__``.

    ``provenance`` marks a canvas published from a bundle rather than
    authored in conversation. It is the publisher's business, not the
    actor's, so it must be callable but invisible — which is precisely
    what wrapping the base signature gives and what a ``__doc__`` copy
    does not.
    """
    from unify.canvas_manager.canvas_manager import CanvasManager

    exposed = inspect.signature(CanvasManager.create_view).parameters
    assert "provenance" not in exposed
    assert "tsx" in exposed and "title" in exposed

    real = inspect.signature(CanvasManager.create_view, follow_wrapped=False).parameters
    assert "provenance" in real, "still callable by the publisher"


def test_wrapped_methods_report_the_base_docstring():
    """What the primitives layer actually reads."""
    from unify.canvas_manager.base import BaseCanvasManager
    from unify.canvas_manager.canvas_manager import CanvasManager
    from unify.ingestion_manager.base import BaseIngestionManager
    from unify.ingestion_manager.ingestion_manager import IngestionManager

    for concrete, base, method in (
        (CanvasManager, BaseCanvasManager, "create_view"),
        (IngestionManager, BaseIngestionManager, "submit"),
    ):
        doc = inspect.getdoc(getattr(concrete, method))
        assert doc
        assert doc == inspect.getdoc(getattr(base, method))
