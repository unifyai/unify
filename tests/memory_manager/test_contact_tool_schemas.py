"""MemoryManager contact tools must expose closed LLM schemas.

Open ``**kwargs`` / ``additionalProperties: true`` schemas are unreliable on
OpenAI tool calling — undeclared keys are ignored. Contact wrappers pin a
closed signature so the model sees declared fields only.
"""

from __future__ import annotations

import functools
import inspect

import pytest

from tests.helpers import _handle_project

from unify.common.llm_helpers import method_to_schema
from unify.common.tool_spec import ToolSpec
from unify.contact_manager.contact_manager import ContactManager
from unify.contact_manager.simulated import SimulatedContactManager
from unify.memory_manager.memory_manager import (
    _llm_visible_contact_signature,
    _pin_contact_tool_schema,
)
from unify.memory_manager.simulated import SimulatedMemoryManager


def _schema_for_tool(tools: dict, name: str) -> dict:
    entry = tools[name]
    fn = entry.fn if isinstance(entry, ToolSpec) else entry
    return method_to_schema(fn, tool_name=name, include_class_name=False)


def _assert_closed_contact_schema(schema: dict, *, expected_props: set[str]) -> None:
    params = schema["function"]["parameters"]
    props = params.get("properties") or {}
    assert params.get("additionalProperties") is not True, (
        f"expected closed schema, got additionalProperties="
        f"{params.get('additionalProperties')!r}"
    )
    assert expected_props <= set(
        props,
    ), f"missing properties {expected_props - set(props)}; have {sorted(props)}"
    assert "custom_fields" not in props
    assert "kwargs" not in props


@_handle_project
def test_build_contact_tools_schemas_are_closed():
    cm = SimulatedContactManager(description="schema unit test")
    mm = SimulatedMemoryManager(contact_manager=cm)
    tools = mm._build_contact_tools()

    _assert_closed_contact_schema(
        _schema_for_tool(tools, "create_contact"),
        expected_props={"first_name", "surname", "email_address", "phone_number"},
    )
    _assert_closed_contact_schema(
        _schema_for_tool(tools, "update_contact"),
        expected_props={"contact_id", "first_name", "surname"},
    )
    _assert_closed_contact_schema(
        _schema_for_tool(tools, "merge_contacts"),
        expected_props={"contact_id_1", "contact_id_2"},
    )


def test_llm_visible_signature_strips_varkw_and_custom_fields():
    sig = _llm_visible_contact_signature(ContactManager._create_contact)
    names = set(sig.parameters)
    assert {"first_name", "destination"} <= names
    assert "kwargs" not in names
    assert "custom_fields" not in names
    assert not any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )


@_handle_project
def test_pinned_schema_survives_wraps_spy_monkeypatch(monkeypatch):
    """Unwrap+pin must recover declared fields through a wraps spy."""
    orig = SimulatedContactManager._create_contact

    @functools.wraps(orig)
    def spy_create(self, **kw):
        return orig(self, **kw)

    monkeypatch.setattr(SimulatedContactManager, "_create_contact", spy_create)

    cm = SimulatedContactManager(description="spy schema test")
    mm = SimulatedMemoryManager(contact_manager=cm)
    tools = mm._build_contact_tools()
    _assert_closed_contact_schema(
        _schema_for_tool(tools, "create_contact"),
        expected_props={"first_name", "phone_number"},
    )


@_handle_project
@pytest.mark.asyncio
async def test_create_contact_wrapper_rejects_custom_fields(monkeypatch):
    cm = SimulatedContactManager(description="custom_fields guard")
    mm = SimulatedMemoryManager(contact_manager=cm)
    tools = mm._build_contact_tools()
    fn = tools["create_contact"]
    fn = fn.fn if isinstance(fn, ToolSpec) else fn

    async def _to_thread(func, /, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(
        "unify.memory_manager.memory_manager.asyncio.to_thread",
        _to_thread,
    )

    with pytest.raises(ValueError, match="custom columns"):
        await fn(custom_fields={"nickname": "x"})


def test_pin_helper_sets_signature_without_varkw():
    async def wrapper(**kwargs):
        return kwargs

    _pin_contact_tool_schema(wrapper, ContactManager.update_contact)
    sig = inspect.signature(wrapper)
    assert "contact_id" in sig.parameters
    assert not any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    )
