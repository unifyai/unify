from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from unify.utils.http import RequestError

from unity.conversation_manager.domains.coordinator_tools import (
    COORDINATOR_TOOL_METHOD_NAMES,
    CoordinatorPreseedWrite,
    CoordinatorTools,
    _preseed_write_payload,
)
from unity.session_details import SESSION_DETAILS

_SETUP_WRAPPER_METHOD_BY_TOOL = {
    "add_setup_checklist_item": "add_checklist_item",
    "update_setup_checklist_item": "update_checklist_item",
}


class TestCoordinatorTools:
    def setup_method(self):
        SESSION_DETAILS.reset()
        SESSION_DETAILS.unify_key = "owner-key"
        SESSION_DETAILS.org_id = 7

    @pytest.fixture(autouse=True)
    def _mock_default_org_role(self, monkeypatch):
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Owner"}],
        )

    def teardown_method(self):
        SESSION_DETAILS.reset()

    def test_as_tools_exposes_exact_workspace_surface(self):
        tools = CoordinatorTools(cm=object()).as_tools()

        assert set(tools) == {
            "create_assistant",
            "delete_assistant",
            "update_assistant_config",
            "list_assistants",
            "list_org_members",
            "invite_org_member",
            "pre_seed_colleague",
            "create_space",
            "delete_space",
            "update_space",
            "add_space_member",
            "remove_space_member",
            "list_spaces",
            "list_space_members",
            "list_spaces_for_assistant",
            "commission_colleague_into_workspace",
            "add_setup_checklist_item",
            "update_setup_checklist_item",
        }

    def test_coordinator_method_surface_includes_org_membership_operations(self):
        assert "list_org_members" in COORDINATOR_TOOL_METHOD_NAMES
        assert "invite_org_member" in COORDINATOR_TOOL_METHOD_NAMES

    def test_as_tools_keeps_org_membership_methods_without_org_context(self):
        SESSION_DETAILS.org_id = None

        tools = CoordinatorTools(cm=object()).as_tools()

        assert set(tools) == set(COORDINATOR_TOOL_METHOD_NAMES)
        assert "list_org_members" in tools
        assert "invite_org_member" in tools
        assert "add_setup_checklist_item" in tools

    def test_list_assistants_uses_owner_key_and_current_sdk_shape(self, monkeypatch):
        calls = []

        def fake_list_assistants(**kwargs):
            calls.append(kwargs)
            return [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}]

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            fake_list_assistants,
        )

        result = CoordinatorTools(cm=object()).list_assistants()

        assert result == [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}]
        assert calls == [
            {
                "phone": None,
                "email": None,
                "agent_id": None,
                "list_all_org": True,
                "api_key": "owner-key",
            },
        ]

    def test_list_assistants_uses_personal_scope_without_list_all_org(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.org_id = None
        calls = []

        def fake_list_assistants(**kwargs):
            calls.append(kwargs)
            return [{"agent_id": 41, "organization_id": 13}]

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            fake_list_assistants,
        )

        result = CoordinatorTools(cm=object()).list_assistants()

        assert result == [{"agent_id": 41, "organization_id": 13}]
        assert calls == [
            {
                "phone": None,
                "email": None,
                "agent_id": None,
                "list_all_org": False,
                "api_key": "owner-key",
            },
        ]

    def test_delete_requires_reachable_assistant(self, monkeypatch):
        delete_calls = []

        list_calls = []

        def fake_list_assistants(**kwargs):
            list_calls.append(kwargs)
            return [{"agent_id": 42}]

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            fake_list_assistants,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs)),
        )

        missing = CoordinatorTools(cm=object()).delete_assistant(agent_id=99)

        assert missing["error_kind"] == "not_found"
        assert list_calls[0]["agent_id"] == 99
        assert delete_calls == []

    def test_delete_assistant_checks_reachability_before_delete(
        self,
        monkeypatch,
    ):
        captured = {}
        delete_calls = []
        tools = CoordinatorTools(cm=object())

        monkeypatch.setattr(
            tools,
            "_assistant_is_reachable",
            lambda agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs))
            or {"status": "deleted"},
        )

        result = tools.delete_assistant(agent_id=42)

        assert result == {"status": "deleted"}
        assert captured == {"agent_id": 42}
        assert delete_calls == [((42,), {"api_key": "owner-key"})]

    def test_filtered_lists_do_not_poison_reachability_cache(self, monkeypatch):
        list_calls = []

        def fake_list_assistants(**kwargs):
            list_calls.append(kwargs)
            if kwargs["phone"] is not None:
                return [{"agent_id": 42, "organization_id": 7}]
            if kwargs["agent_id"] == 99:
                return [{"agent_id": 99, "organization_id": 7}]
            return []

        delete_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            fake_list_assistants,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs)) or {},
        )

        tools = CoordinatorTools(cm=object())
        assert tools.list_assistants(phone="+15555551234") == [
            {"agent_id": 42, "organization_id": 7},
        ]

        result = tools.delete_assistant(agent_id=99)

        assert result == {}
        assert list_calls[1]["agent_id"] == 99
        assert delete_calls == [((99,), {"api_key": "owner-key"})]

    def test_request_errors_return_tool_error(self, monkeypatch):
        def failing_create_assistant(**_):
            response = requests.Response()
            response.status_code = 403
            response._content = b"forbidden"
            raise RequestError("https://api.unify.ai", "POST", response)

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            failing_create_assistant,
        )

        result = CoordinatorTools(cm=object()).create_assistant(
            first_name="Ops",
            about="Operations colleague.",
        )

        assert result["error_kind"] == "permission_denied"
        assert result["details"]["status_code"] == 403

    def test_create_assistant_merges_defaults_without_overwriting_explicit_config(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorTools(cm=object()).create_assistant(
            first_name="Avery",
            surname="Parker",
            about="Handles escalation triage",
            config={
                "timezone": "Europe/Berlin",
            },
        )

        assert result == {"agent_id": 42}
        assert calls == [
            {
                "first_name": "Avery",
                "surname": "Parker",
                "config": {
                    "timezone": "Europe/Berlin",
                    "about": "Handles escalation triage",
                    "nationality": "United States",
                    "organization_id": 7,
                },
                "api_key": "owner-key",
            },
        ]

    def test_create_assistant_requires_explicit_about(self, monkeypatch):
        create_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorTools(cm=object()).create_assistant(
            first_name="Marcus",
            surname="Webb",
            about="   ",
        )

        assert result["error_kind"] == "invalid_argument"
        assert result["details"] == {"field": "about"}
        assert create_calls == []

    def test_create_assistant_personal_workspace_writes_without_org_id(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.org_id = None
        create_calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorTools(cm=object()).create_assistant(
            first_name="Ops",
            surname="Lead",
            about="Operations coordinator.",
        )

        assert result == {"agent_id": 42}
        assert "organization_id" not in create_calls[0]["config"]

    def test_create_assistant_explicit_profile_args_override_config_values(
        self,
        monkeypatch,
    ):
        calls = []
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        CoordinatorTools(cm=object()).create_assistant(
            first_name="Taylor",
            surname="Ops",
            about="Leads daily cash operations.",
            job_title="Revenue Operations Lead",
            timezone="Europe/London",
            nationality="United Kingdom",
            config={
                "job_title": "Old Title",
                "timezone": "America/Los_Angeles",
                "nationality": "Canada",
            },
        )

        assert calls[0]["config"] == {
            "job_title": "Revenue Operations Lead",
            "timezone": "Europe/London",
            "nationality": "United Kingdom",
            "about": "Leads daily cash operations.",
            "organization_id": 7,
        }

    def test_update_assistant_config_checks_reachability_before_update(
        self,
        monkeypatch,
    ):
        captured = {}
        update_calls = []
        tools = CoordinatorTools(cm=object())

        monkeypatch.setattr(
            tools,
            "_assistant_is_reachable",
            lambda agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.update_assistant_config",
            lambda *args, **kwargs: update_calls.append((args, kwargs))
            or {"agent_id": 42, "about": "updated"},
        )

        result = tools.update_assistant_config(
            agent_id=42,
            config={"about": "updated"},
        )

        assert result == {"agent_id": 42, "about": "updated"}
        assert captured == {"agent_id": 42}
        assert update_calls == [((42, {"about": "updated"}), {"api_key": "owner-key"})]

    def test_create_assistant_derives_job_title_from_surname_for_multiword_first_name(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        CoordinatorTools(cm=object()).create_assistant(
            first_name="Sarah Chen",
            surname="Recruiter",
            about="Senior recruiter for supply-chain hiring.",
        )

        assert calls[0]["config"] == {
            "timezone": "Asia/Karachi",
            "nationality": "United States",
            "job_title": "Recruiter",
            "about": "Senior recruiter for supply-chain hiring.",
            "organization_id": 7,
        }

    def test_create_assistant_conflict_parses_detail_payload_with_existing_id(
        self,
        monkeypatch,
    ):
        def failing_create_assistant(**_):
            response = requests.Response()
            response.status_code = 409
            response._content = (
                b'{"detail":{"error":"assistant_already_exists","message":"Assistant '
                b'with this name already exists in this scope.","existing_id":1939}}'
            )
            raise RequestError("https://api.unify.ai", "POST", response)

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            failing_create_assistant,
        )

        result = CoordinatorTools(cm=object()).create_assistant(
            first_name="Ops",
            about="Operations colleague.",
        )

        assert result["error_kind"] == "conflict"
        assert (
            result["message"]
            == "Assistant with this name already exists in this scope."
        )
        assert result["details"]["status_code"] == 409
        assert result["details"]["existing_id"] == 1939
        assert result["details"]["error"] == "assistant_already_exists"

    def test_add_setup_checklist_item_forwards_explicit_status_without_coercion(
        self,
        monkeypatch,
    ):
        calls = []

        def fake_add_checklist_item(*_, **kwargs):
            calls.append(kwargs)
            return {"outcome": "checklist item added", "details": {"item_id": 77}}

        monkeypatch.setattr(
            "unity.coordinator_manager.coordinator_manager."
            "CoordinatorOnboardingManager.add_checklist_item",
            fake_add_checklist_item,
        )

        result = CoordinatorTools(cm=object()).add_setup_checklist_item(
            title="Connect CRM",
            status="",
        )

        assert result == {"outcome": "checklist item added", "details": {"item_id": 77}}
        assert calls == [
            {
                "title": "Connect CRM",
                "initial_status": "",
                "description": None,
                "kind": None,
                "chat_prompt": None,
                "chat_prompt_label": None,
            },
        ]

    @pytest.mark.parametrize(
        ("tool_name", "tool_kwargs"),
        [
            ("add_setup_checklist_item", {"title": "Connect CRM", "status": "done"}),
            ("update_setup_checklist_item", {"item_id": 7, "status": "done"}),
        ],
    )
    def test_setup_wrappers_convert_request_errors_to_tool_errors(
        self,
        monkeypatch,
        tool_name,
        tool_kwargs,
    ):
        response = requests.Response()
        response.status_code = 400
        response._content = b"invalid payload"

        monkeypatch.setattr(
            "unity.coordinator_manager.coordinator_manager."
            f"CoordinatorOnboardingManager.{_SETUP_WRAPPER_METHOD_BY_TOOL[tool_name]}",
            lambda *_, **__: (_ for _ in ()).throw(
                RequestError("https://api.unify.ai", "POST", response),
            ),
        )

        result = getattr(CoordinatorTools(cm=object()), tool_name)(**tool_kwargs)

        assert result["error_kind"] == "invalid_argument"
        assert result["details"]["status_code"] == 400

    @pytest.mark.parametrize(
        ("tool_name", "tool_kwargs", "expected_message"),
        [
            (
                "add_setup_checklist_item",
                {"title": "Connect CRM", "status": "done"},
                "Failed to add setup checklist item.",
            ),
            (
                "update_setup_checklist_item",
                {"item_id": 7, "status": "done"},
                "Failed to update setup checklist item.",
            ),
        ],
    )
    def test_setup_wrappers_convert_unexpected_errors_to_internal_tool_errors(
        self,
        monkeypatch,
        tool_name,
        tool_kwargs,
        expected_message,
    ):
        monkeypatch.setattr(
            "unity.coordinator_manager.coordinator_manager."
            f"CoordinatorOnboardingManager.{_SETUP_WRAPPER_METHOD_BY_TOOL[tool_name]}",
            lambda *_, **__: (_ for _ in ()).throw(RuntimeError("boom")),
        )

        result = getattr(CoordinatorTools(cm=object()), tool_name)(**tool_kwargs)

        assert result["error_kind"] == "internal"
        assert result["message"] == expected_message
        assert result["details"]["error"] == "boom"

    def test_pre_seed_colleague_forwards_confirmed_writes_after_reachability(
        self,
        monkeypatch,
    ):
        calls = []
        writes = [
            {
                "context": "Tasks",
                "entries": [{"task_id": 77, "status": "scheduled"}],
            },
        ]

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.pre_seed_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs))
            or {"target_assistant_id": 42},
        )

        result = CoordinatorTools(cm=object()).pre_seed_colleague(
            target_assistant_id=42,
            writes=writes,
        )

        assert result == {"target_assistant_id": 42}
        assert calls == [((42, writes), {"api_key": "owner-key"})]

    def test_pre_seed_colleague_rejects_fabricated_assistant_without_sdk_call(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.pre_seed_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).pre_seed_colleague(
            target_assistant_id=99,
            writes=[{"context": "Knowledge", "entries": [{"content": "nope"}]}],
        )

        assert result["error_kind"] == "not_found"
        assert result["details"] == {"agent_id": 99}
        assert calls == []

    def test_preseed_write_payload_accepts_schema_and_mapping_inputs(self):
        writes = [
            CoordinatorPreseedWrite(
                context="Tasks",
                entries=[{"task_id": 77, "status": "scheduled"}],
            ),
            {"context": "Guidance", "entries": [{"content": "Check billing holds"}]},
        ]

        assert _preseed_write_payload(writes) == [
            {
                "context": "Tasks",
                "entries": [{"task_id": 77, "status": "scheduled"}],
            },
            {"context": "Guidance", "entries": [{"content": "Check billing holds"}]},
        ]

    def test_pre_seed_colleague_rejects_invalid_context(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.pre_seed_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).pre_seed_colleague(
            target_assistant_id=42,
            writes=[
                {
                    "context": "Spaces/Ops",
                    "entries": [{"content": "should fail before SDK call"}],
                },
            ],
        )

        assert result["error_kind"] == "invalid_argument"
        assert calls == []

    def test_pre_seed_colleague_checks_reachability_before_write(
        self,
        monkeypatch,
    ):
        captured = {}
        seed_calls = []
        tools = CoordinatorTools(cm=object())

        monkeypatch.setattr(
            tools,
            "_assistant_is_reachable",
            lambda agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.pre_seed_colleague",
            lambda *args, **kwargs: seed_calls.append((args, kwargs))
            or {"status": "seeded"},
        )

        result = tools.pre_seed_colleague(
            target_assistant_id=42,
            writes=[{"context": "Tasks", "entries": [{"title": "Audit inbox"}]}],
        )

        assert result == {"status": "seeded"}
        assert captured == {"agent_id": 42}
        assert seed_calls == [
            (
                (
                    42,
                    [{"context": "Tasks", "entries": [{"title": "Audit inbox"}]}],
                ),
                {"api_key": "owner-key"},
            ),
        ]

    def test_list_org_members_uses_active_workspace_organization(self, monkeypatch):
        list_members_calls = []

        def fake_list_org_members(*args, **kwargs):
            list_members_calls.append((args, kwargs))
            return [{"user_id": "member-1"}]

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_org_members",
            fake_list_org_members,
        )

        result = CoordinatorTools(cm=object()).list_org_members()

        assert result == [{"user_id": "member-1"}]
        assert list_members_calls == [((7,), {"api_key": "owner-key"})]

    def test_list_org_members_requires_org_workspace(self, monkeypatch):
        SESSION_DETAILS.org_id = None
        calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_org_members",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).list_org_members()

        assert result["error_kind"] == "invalid_argument"
        assert "requires an organization workspace Coordinator" in result["message"]
        assert calls == []

    def test_invite_org_member_happy_path(self, monkeypatch):
        calls = []

        def fake_invite_org_member(*args, **kwargs):
            calls.append((args, kwargs))
            return {
                "invitee_email": "sarah@example.com",
                "organization_name": "Acme",
                "role_name": "Admin",
            }

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.invite_org_member",
            fake_invite_org_member,
        )

        result = CoordinatorTools(cm=object()).invite_org_member(
            email="  SARAH@EXAMPLE.COM  ",
            role_name="  aDmiN  ",
        )

        assert result == {
            "invitee_email": "sarah@example.com",
            "organization_name": "Acme",
            "role_name": "Admin",
        }
        assert calls == [
            (
                (7, "sarah@example.com"),
                {"role_name": "Admin", "api_key": "owner-key"},
            ),
        ]

    def test_invite_org_member_no_org_context(self, monkeypatch):
        SESSION_DETAILS.org_id = None
        calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.invite_org_member",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).invite_org_member(
            email="sarah@example.com",
        )

        assert result["error_kind"] == "invalid_argument"
        assert "requires an organization workspace Coordinator" in result["message"]
        assert calls == []

    def test_invite_org_member_rejects_unknown_role_name(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.invite_org_member",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).invite_org_member(
            email="sarah@example.com",
            role_name="Operator",
        )

        assert result["error_kind"] == "invalid_argument"
        assert result["details"]["accepted_roles"] == ["Admin", "Member", "Viewer"]
        assert calls == []

    def test_invite_org_member_already_member(self, monkeypatch):
        def failing_invite_org_member(*_args, **_kwargs):
            response = requests.Response()
            response.status_code = 409
            response._content = (
                b'{"detail":"User is already a member of this organization"}'
            )
            raise RequestError("https://api.unify.ai", "POST", response)

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.invite_org_member",
            failing_invite_org_member,
        )

        result = CoordinatorTools(cm=object()).invite_org_member(
            email="sarah@example.com",
        )

        assert result["error_kind"] == "conflict"
        assert result["message"] == "User is already a member of this organization"

    def test_workspace_mutations_require_owner_or_admin_role(self, monkeypatch):
        mutation_calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Member"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.invite_org_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("invite_org_member", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_space",
            lambda **kwargs: mutation_calls.append(("create_space", kwargs))
            or {"space_id": 11},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.delete_space",
            lambda *args, **kwargs: mutation_calls.append(
                ("delete_space", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.update_space",
            lambda *args, **kwargs: mutation_calls.append(
                ("update_space", args, kwargs),
            )
            or {"space_id": 11},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("add_space_member", args, kwargs),
            )
            or {"membership_status": "active"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.remove_space_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("remove_space_member", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: mutation_calls.append(("create_assistant", kwargs))
            or {"agent_id": 42},
        )

        tools = CoordinatorTools(cm=object())
        results = {
            "invite_org_member": tools.invite_org_member(email="member-only@test.com"),
            "create_space": tools.create_space(
                name="Ops",
                description="Operations workspace.",
            ),
            "delete_space": tools.delete_space(space_id=11),
            "update_space": tools.update_space(space_id=11, patch={"name": "Ops v2"}),
            "add_space_member": tools.add_space_member(space_id=11, assistant_id=42),
            "remove_space_member": tools.remove_space_member(
                space_id=11,
                assistant_id=42,
            ),
            "commission_colleague_into_workspace": tools.commission_colleague_into_workspace(
                assistant_first_name="Avery",
                space_name="Ops",
                space_description="Operations workspace.",
                assistant_about="Runs operations workflows.",
            ),
        }

        for operation_name, result in results.items():
            assert result["error_kind"] == "permission_denied"
            assert (
                f"`{operation_name}` requires Owner or Admin role in the target organization."
                in result["message"]
            )
            assert result["details"]["role_name"] == "member"
            assert result["details"]["required_roles"] == ["Owner", "Admin"]

        assert mutation_calls == []

    def test_workspace_mutation_gate_allows_admin_role(self, monkeypatch):
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Admin"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_space",
            lambda **kwargs: calls.append(kwargs) or {"space_id": 11, "name": "Ops"},
        )

        result = CoordinatorTools(cm=object()).create_space(
            name="Ops",
            description="Operations workspace.",
        )

        assert result == {"space_id": 11, "name": "Ops"}
        assert calls == [
            {
                "name": "Ops",
                "description": "Operations workspace.",
                "organization_id": 7,
                "api_key": "owner-key",
            },
        ]

    def test_duplicate_commissioning_tool_suppression_short_circuits_writes(
        self,
        monkeypatch,
    ):
        create_calls = []
        cm = SimpleNamespace(
            suppress_duplicate_commissioning_tool=lambda **_: {
                "error_kind": "duplicate_suppressed",
                "message": "duplicate",
                "details": {"tool_name": "create_assistant"},
            },
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorTools(cm=cm).create_assistant(
            first_name="Ops",
            about="Operations colleague.",
        )

        assert result["error_kind"] == "duplicate_suppressed"
        assert create_calls == []

    def test_add_space_member_duplicate_suppression_captures_target_args(self):
        captured = {}
        cm = SimpleNamespace(
            suppress_duplicate_commissioning_tool=lambda **kwargs: captured.update(
                kwargs,
            )
            or {
                "error_kind": "duplicate_suppressed",
                "message": "duplicate",
                "details": kwargs["tool_args"],
            },
        )

        result = CoordinatorTools(cm=cm).add_space_member(
            space_id=11,
            assistant_id=42,
        )

        assert result["error_kind"] == "duplicate_suppressed"
        assert captured["tool_args"] == {
            "space_id": 11,
            "assistant_id": 42,
            "member_user_id": None,
        }

    def test_space_member_writes_require_reachable_space_and_assistant(
        self,
        monkeypatch,
    ):
        member_calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs))
            or {"membership_status": "active"},
        )

        result = CoordinatorTools(cm=object()).add_space_member(
            space_id=11,
            assistant_id=42,
        )

        assert result == {"membership_status": "active"}
        assert member_calls == [
            (
                (11,),
                {
                    "assistant_id": 42,
                    "member_user_id": None,
                    "api_key": "owner-key",
                },
            ),
        ]

    def test_space_member_writes_support_member_targeting(self, monkeypatch):
        member_calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_org_members",
            lambda *_, **__: [{"user_id": "member-1"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs))
            or {"membership_status": "active", "assistant_id": 91},
        )

        result = CoordinatorTools(cm=object()).add_space_member(
            space_id=11,
            member_user_id="member-1",
        )

        assert result == {"membership_status": "active", "assistant_id": 91}
        assert member_calls == [
            (
                (11,),
                {
                    "assistant_id": None,
                    "member_user_id": "member-1",
                    "api_key": "owner-key",
                },
            ),
        ]

    def test_space_member_writes_require_exactly_one_target_shape(self):
        tools = CoordinatorTools(cm=object())

        missing_target = tools.add_space_member(space_id=11)
        assert missing_target["error_kind"] == "invalid_argument"

        duplicate_target = tools.add_space_member(
            space_id=11,
            assistant_id=42,
            member_user_id="member-1",
        )
        assert duplicate_target["error_kind"] == "invalid_argument"

        blank_member_target = tools.add_space_member(
            space_id=11,
            member_user_id="   ",
        )
        assert blank_member_target["error_kind"] == "invalid_argument"

    def test_remaining_space_wrappers_forward_after_reachability(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11, "name": "Ops"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.update_space",
            lambda *args, **kwargs: calls.append(("update", args, kwargs))
            or {"space_id": 11, "name": "Ops Team"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.remove_space_member",
            lambda *args, **kwargs: calls.append(("remove", args, kwargs)) or {},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *args, **kwargs: calls.append(("members", args, kwargs))
            or [{"assistant_id": 42}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces_for_assistant",
            lambda *args, **kwargs: calls.append(("assistant_spaces", args, kwargs))
            or [{"space_id": 11}],
        )

        tools = CoordinatorTools(cm=object())

        assert tools.update_space(
            space_id=11,
            patch={"name": "Ops Team"},
        ) == {"space_id": 11, "name": "Ops Team"}
        assert tools.list_space_members(space_id=11) == [{"assistant_id": 42}]
        assert tools.list_spaces_for_assistant(assistant_id=42) == [{"space_id": 11}]
        assert tools.remove_space_member(space_id=11, assistant_id=42) == {}
        assert calls == [
            ("update", (11, {"name": "Ops Team"}), {"api_key": "owner-key"}),
            ("members", (11,), {"api_key": "owner-key"}),
            ("assistant_spaces", (42,), {"api_key": "owner-key"}),
            ("remove", (11, 42), {"api_key": "owner-key"}),
        ]

    def test_list_spaces_for_assistant_checks_reachability_before_lookup(
        self,
        monkeypatch,
    ):
        captured = {}
        list_calls = []
        tools = CoordinatorTools(cm=object())

        monkeypatch.setattr(
            tools,
            "_assistant_is_reachable",
            lambda agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces_for_assistant",
            lambda *args, **kwargs: list_calls.append((args, kwargs))
            or [{"space_id": 11}],
        )

        result = tools.list_spaces_for_assistant(assistant_id=42)

        assert result == [{"space_id": 11}]
        assert captured == {"agent_id": 42}
        assert list_calls == [((42,), {"api_key": "owner-key"})]

    def test_fabricated_space_id_returns_tool_error_without_sdk_write(
        self,
        monkeypatch,
    ):
        member_calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs)),
        )

        result = CoordinatorTools(cm=object()).add_space_member(
            space_id=99,
            assistant_id=42,
        )

        assert result["error_kind"] == "not_found"
        assert result["details"] == {"space_id": 99}
        assert member_calls == []

    def test_space_request_errors_return_tool_error(self, monkeypatch):
        def failing_delete_space(*_, **__):
            response = requests.Response()
            response.status_code = 409
            response._content = b"conflict"
            raise RequestError("https://api.unify.ai", "DELETE", response)

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.delete_space",
            failing_delete_space,
        )

        result = CoordinatorTools(cm=object()).delete_space(space_id=11)

        assert result["error_kind"] == "conflict"
        assert result["details"]["status_code"] == 409

    def test_commission_colleague_into_workspace_creates_missing_resources(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **kwargs: calls.append(("list_assistants", kwargs)) or [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: calls.append(("create_assistant", kwargs))
            or {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **kwargs: calls.append(("list_spaces", kwargs))
            or (
                [{"space_id": 11, "name": "Ops HQ"}]
                if any(name == "create_space" for name, *_ in calls)
                else []
            ),
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_space",
            lambda **kwargs: calls.append(("create_space", kwargs))
            or {"space_id": 11, "name": "Ops HQ"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *args, **kwargs: calls.append(("list_space_members", args, kwargs))
            or [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: calls.append(("add_space_member", args, kwargs))
            or {"membership_status": "active"},
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            space_name="Ops HQ",
            space_description="Operations workspace",
            assistant_about="Leads operations workflows.",
        )

        assert result == {
            "assistant": {
                "status": "created",
                "assistant_id": 42,
                "assistant": {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
            },
            "space": {
                "status": "created",
                "space_id": 11,
                "space": {"space_id": 11, "name": "Ops HQ"},
            },
            "membership": {
                "status": "added",
                "space_id": 11,
                "assistant_id": 42,
            },
        }
        assert calls == [
            (
                "list_assistants",
                {
                    "phone": None,
                    "email": None,
                    "agent_id": None,
                    "list_all_org": True,
                    "api_key": "owner-key",
                },
            ),
            (
                "create_assistant",
                {
                    "first_name": "Ops",
                    "surname": "Bot",
                    "config": {
                        "about": "Leads operations workflows.",
                        "organization_id": 7,
                    },
                    "api_key": "owner-key",
                },
            ),
            (
                "list_spaces",
                {"organization_id": 7, "api_key": "owner-key"},
            ),
            (
                "create_space",
                {
                    "name": "Ops HQ",
                    "description": "Operations workspace",
                    "organization_id": 7,
                    "api_key": "owner-key",
                },
            ),
            (
                "list_spaces",
                {"organization_id": 7, "api_key": "owner-key"},
            ),
            (
                "list_space_members",
                (11,),
                {"api_key": "owner-key"},
            ),
            (
                "add_space_member",
                (),
                {"space_id": 11, "assistant_id": 42, "api_key": "owner-key"},
            ),
        ]

    def test_commission_colleague_into_workspace_requires_about_when_creating(
        self,
        monkeypatch,
    ):
        create_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            space_name="Ops HQ",
            space_description="Operations workspace",
        )

        assert result["error_kind"] == "invalid_argument"
        assert result["details"] == {"field": "assistant_about"}
        assert create_calls == []

    def test_commission_colleague_explicit_profile_args_override_assistant_config(
        self,
        monkeypatch,
    ):
        create_calls = []
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs)
            or {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )

        CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            space_name="Ops HQ",
            space_description="Operations workspace",
            assistant_about="Runs the operations command center.",
            assistant_job_title="Operations Lead",
            assistant_timezone="Europe/London",
            assistant_nationality="United Kingdom",
            assistant_config={
                "job_title": "Old Title",
                "timezone": "America/New_York",
                "nationality": "Canada",
            },
        )

        assert create_calls[0]["config"] == {
            "job_title": "Operations Lead",
            "timezone": "Europe/London",
            "nationality": "United Kingdom",
            "about": "Runs the operations command center.",
            "organization_id": 7,
        }

    def test_commission_colleague_into_workspace_reuses_existing_membership(
        self,
        monkeypatch,
    ):
        add_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [
                {
                    "agent_id": 42,
                    "first_name": "Ops",
                    "surname": "Bot",
                    "organization_id": 7,
                },
            ],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: add_calls.append((args, kwargs)) or {},
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            space_name="Ops HQ",
            space_description="Operations workspace",
        )

        assert result["assistant"]["status"] == "reused"
        assert result["space"]["status"] == "reused"
        assert result["membership"]["status"] == "already_member"
        assert add_calls == []

    def test_commission_colleague_into_workspace_reuses_explicit_ids(
        self,
        monkeypatch,
    ):
        create_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **kwargs: (
                [
                    {
                        "agent_id": 42,
                        "first_name": "Ops",
                        "surname": "Bot",
                        "organization_id": 7,
                    },
                ]
                if kwargs.get("agent_id") == 42
                else []
            ),
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *_, **__: [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.add_space_member",
            lambda *args, **kwargs: {"membership_status": "active"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(("assistant", kwargs)),
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_space",
            lambda **kwargs: create_calls.append(("space", kwargs)),
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ignored",
            assistant_surname="Name",
            space_name="Ignored Space Name",
            space_description="ignored",
            assistant_id=42,
            space_id=11,
        )

        assert result["assistant"]["status"] == "reused"
        assert result["space"]["status"] == "reused"
        assert result["membership"]["status"] == "added"
        assert create_calls == []

    def test_commission_colleague_into_workspace_conflicts_on_ambiguous_assistant_name(
        self,
        monkeypatch,
    ):
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [
                {
                    "agent_id": 42,
                    "first_name": "Ops",
                    "surname": "Bot",
                    "organization_id": 7,
                },
                {
                    "agent_id": 43,
                    "first_name": "Ops",
                    "surname": "Bot",
                    "organization_id": 7,
                },
            ],
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            space_name="Ops HQ",
            space_description="Operations workspace",
        )

        assert result["error_kind"] == "conflict"
        assert result["details"]["matches"] == [42, 43]

    def test_commission_colleague_into_workspace_applies_same_assistant_defaults(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        create_calls = []
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs)
            or {"agent_id": 42, "first_name": "Sarah Chen", "surname": "Recruiter"},
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_spaces",
            lambda **_: [{"space_id": 11, "name": "Hiring Desk"}],
        )
        monkeypatch.setattr(
            "unity.conversation_manager.domains.coordinator_tools.unify.list_space_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )

        result = CoordinatorTools(cm=object()).commission_colleague_into_workspace(
            assistant_first_name="Sarah Chen",
            assistant_surname="Recruiter",
            space_name="Hiring Desk",
            space_description="Hiring workspace for sourcing and interview loops.",
            assistant_about="Leads recruiter scorecards and candidate routing.",
        )

        assert result["assistant"]["status"] == "created"
        assert result["space"]["status"] == "reused"
        assert result["membership"]["status"] == "already_member"
        assert create_calls[0]["config"] == {
            "timezone": "Asia/Karachi",
            "nationality": "United States",
            "job_title": "Recruiter",
            "about": "Leads recruiter scorecards and candidate routing.",
            "organization_id": 7,
        }
