from __future__ import annotations


import pytest

from unify import integration_status as IS


def _pkg(
    slug: str,
    label: str,
    required: list[str],
    function_names: list[str],
    guidance_titles: list[str] | None = None,
) -> dict:
    return {
        "slug": slug,
        "label": label,
        "required_secrets": required,
        "optional_secrets": [],
        "function_names": function_names,
        "guidance_titles": guidance_titles or [],
    }


class FakeFunctionManager:
    def filter(self, *, filter: str, limit: int):
        assert "disabled_package_func" in filter
        assert limit == 10000
        return [
            {"function_id": 101, "name": "disabled_package_func"},
            {"function_id": 102, "name": "another_disabled_func"},
        ]


class FakeGuidanceManager:
    def filter(self, *, filter: str, limit: int):
        assert "Disabled HubSpot Playbook" in filter
        assert limit == 10000
        return [
            {"guidance_id": 201, "title": "Disabled HubSpot Playbook"},
            {"guidance_id": 202, "title": "Disabled Salesforce SOP"},
        ]


def test_build_guidance_filter_scope_hides_disabled_package_guidance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unify.integration_status import discovery
    from unify.manager_registry import ManagerRegistry

    packages = [
        _pkg(
            "hubspot",
            "HubSpot",
            ["HUBSPOT_TOKEN"],
            ["enabled_func"],
            guidance_titles=["Enabled HubSpot Playbook"],
        ),
        _pkg(
            "salesforce",
            "Salesforce",
            ["SALESFORCE_TOKEN"],
            ["disabled_func"],
            guidance_titles=["Disabled HubSpot Playbook", "Disabled Salesforce SOP"],
        ),
    ]
    monkeypatch.setattr(discovery, "discover_available_packages", lambda: packages)
    monkeypatch.setattr(
        IS,
        "get_enabled_integrations",
        lambda: {"hubspot": packages[0]},
    )
    monkeypatch.setattr(
        ManagerRegistry,
        "get_guidance_manager",
        staticmethod(lambda: FakeGuidanceManager()),
    )

    assert IS.build_guidance_filter_scope() == "guidance_id not in (201, 202)"


def test_build_guidance_filter_scope_preserves_personal_rows_when_no_disabled_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unify.integration_status import discovery

    package = _pkg(
        "hubspot",
        "HubSpot",
        ["HUBSPOT_TOKEN"],
        ["enabled_func"],
        guidance_titles=["HubSpot Playbook"],
    )
    monkeypatch.setattr(discovery, "discover_available_packages", lambda: [package])
    monkeypatch.setattr(IS, "get_enabled_integrations", lambda: {"hubspot": package})

    assert IS.build_guidance_filter_scope() is None


def test_build_function_filter_scope_hides_disabled_package_functions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unify.integration_status import discovery
    from unify.manager_registry import ManagerRegistry

    packages = [
        _pkg("hubspot", "HubSpot", ["HUBSPOT_TOKEN"], ["enabled_package_func"]),
        _pkg(
            "salesforce",
            "Salesforce",
            ["SALESFORCE_TOKEN"],
            ["disabled_package_func", "another_disabled_func"],
        ),
    ]
    monkeypatch.setattr(discovery, "discover_available_packages", lambda: packages)
    monkeypatch.setattr(
        IS,
        "get_enabled_integrations",
        lambda: {"hubspot": packages[0]},
    )
    monkeypatch.setattr(
        ManagerRegistry,
        "get_function_manager",
        staticmethod(lambda: FakeFunctionManager()),
    )

    assert IS.build_function_filter_scope() == "function_id not in (101, 102)"


def test_build_function_filter_scope_preserves_normal_rows_when_no_disabled_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unify.integration_status import discovery

    package = _pkg("hubspot", "HubSpot", ["HUBSPOT_TOKEN"], ["enabled_package_func"])
    monkeypatch.setattr(discovery, "discover_available_packages", lambda: [package])
    monkeypatch.setattr(IS, "get_enabled_integrations", lambda: {"hubspot": package})

    assert IS.build_function_filter_scope() is None


@pytest.mark.anyio
async def test_code_act_actor_appends_enabled_integration_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unify.actor import code_act_actor as code_act_module
    from unify.actor.code_act_actor import CodeActActor

    captured: dict[str, str] = {}

    class FakeClient:
        def set_system_message(self, message: str) -> None:
            captured["system_prompt"] = message

    class FakeSandbox:
        def __init__(self, *args, **kwargs) -> None:
            self.global_state = {}

        async def close(self) -> None:
            return None

    class FakeHandle:
        def __init__(self) -> None:
            self._queue = None

        async def result(self) -> str:
            return "done"

        async def pause(self, **kwargs) -> None:
            return None

        async def resume(self, **kwargs) -> None:
            return None

    monkeypatch.setattr(
        IS,
        "enabled_summary_for_prompt",
        lambda: "### Integrations\nActive integrations:\n- HubSpot (fully_connected)",
    )
    monkeypatch.setattr(code_act_module, "PythonExecutionSession", FakeSandbox)
    monkeypatch.setattr(code_act_module, "new_llm_client", lambda _model: FakeClient())
    monkeypatch.setattr(
        code_act_module,
        "start_async_tool_loop",
        lambda *args, **kwargs: FakeHandle(),
    )
    monkeypatch.setattr(
        code_act_module,
        "build_code_act_prompt",
        lambda **kwargs: kwargs.get("guidelines") or "",
    )

    actor = CodeActActor(
        environments=[],
        function_manager=None,
        guidance_manager=None,
        knowledge_manager=None,
        can_store=False,
        tool_policy=None,
    )
    handle = await actor.act("hello", clarification_enabled=False, persist=False)
    assert await handle.result() == "done"

    assert "### Integrations" in captured["system_prompt"]
    assert "HubSpot (fully_connected)" in captured["system_prompt"]
