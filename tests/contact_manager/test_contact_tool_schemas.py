"""ContactManager create/update tools must expose closed LLM schemas.

Open ``**kwargs`` / top-level ``additionalProperties: true`` schemas are
unreliable on OpenAI tool calling. The Contact schema is fixed, so tools
expose only the named built-in fields.
"""

from __future__ import annotations

import inspect

from tests.helpers import _handle_project

from unify.common.llm_helpers import method_to_schema
from unify.common.tool_spec import ToolSpec
from unify.contact_manager.contact_manager import ContactManager


def _schema_for_tool(tools: dict, name: str) -> dict:
    entry = tools[name]
    fn = entry.fn if isinstance(entry, ToolSpec) else entry
    return method_to_schema(fn, tool_name=name, include_class_name=False)


def _assert_closed(schema: dict, *, expected_props: set[str]) -> None:
    params = schema["function"]["parameters"]
    props = params.get("properties") or {}
    assert params.get("additionalProperties") is not True, (
        f"expected closed schema, got additionalProperties="
        f"{params.get('additionalProperties')!r}"
    )
    assert expected_props <= set(
        props,
    ), f"missing properties {expected_props - set(props)}; have {sorted(props)}"
    assert "kwargs" not in props


@_handle_project
def test_contact_manager_update_tools_schemas_are_closed():
    cm = ContactManager()
    tools = cm.get_tools("update")

    create = _schema_for_tool(tools, "create_contact")
    _assert_closed(
        create,
        expected_props={"first_name", "surname", "email_address"},
    )
    assert "_contact_id" not in (
        create["function"]["parameters"].get("properties") or {}
    )
    assert "custom_fields" not in (
        create["function"]["parameters"].get("properties") or {}
    )

    update = _schema_for_tool(tools, "update_contact")
    _assert_closed(
        update,
        expected_props={"contact_id", "first_name"},
    )
    assert "custom_fields" not in (
        update["function"]["parameters"].get("properties") or {}
    )


def test_create_and_update_signatures_have_no_varkw():
    for fn in (ContactManager._create_contact, ContactManager.update_contact):
        sig = inspect.signature(fn)
        assert not any(
            p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
        ), f"{fn.__name__} must not accept **kwargs"
        assert "custom_fields" not in sig.parameters
