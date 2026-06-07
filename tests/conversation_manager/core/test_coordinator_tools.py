from __future__ import annotations

import pytest
import requests
from unify.utils.http import RequestError

from unity.coordinator_manager.workspace_manager import (
    COORDINATOR_TOOL_METHOD_NAMES,
    CoordinatorWorkspaceManager,
    _CoordinatorWorkspaceSession,
)
from unity.session_details import SESSION_DETAILS

_SETUP_WRAPPER_METHOD_BY_TOOL = {
    "add_setup_checklist_item": "add_checklist_item",
    "update_setup_checklist_item": "update_checklist_item",
}


class TestCoordinatorWorkspaceManager:
    def setup_method(self):
        SESSION_DETAILS.reset()
        SESSION_DETAILS.is_coordinator = True
        SESSION_DETAILS.unify_key = "owner-key"
        SESSION_DETAILS.org_id = 7

    @pytest.fixture(autouse=True)
    def _mock_default_org_role(self, monkeypatch):
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Owner"}],
        )

    def teardown_method(self):
        SESSION_DETAILS.reset()

    def test_primitive_methods_expose_exact_workspace_surface(self):
        assert set(CoordinatorWorkspaceManager._PRIMITIVE_METHODS) == {
            "create_assistant",
            "delete_assistant",
            "update_assistant_config",
            "list_assistants",
            "list_org_members",
            "invite_org_member",
            "delegate_to_colleague",
            "create_team",
            "delete_team",
            "update_team",
            "add_team_member",
            "remove_team_member",
            "list_teams",
            "list_team_members",
            "list_teams_for_assistant",
            "commission_colleague_into_team",
            "add_setup_checklist_item",
            "update_setup_checklist_item",
        }

    def test_coordinator_method_surface_includes_org_membership_operations(self):
        assert "list_org_members" in COORDINATOR_TOOL_METHOD_NAMES
        assert "invite_org_member" in COORDINATOR_TOOL_METHOD_NAMES

    def test_org_membership_methods_remain_available_without_org_context(self):
        SESSION_DETAILS.org_id = None

        manager = CoordinatorWorkspaceManager()
        assert callable(manager.list_org_members)
        assert callable(manager.invite_org_member)
        assert callable(manager.add_setup_checklist_item)

    def test_list_assistants_uses_owner_key_and_current_sdk_shape(self, monkeypatch):
        calls = []

        def fake_list_assistants(**kwargs):
            calls.append(kwargs)
            return [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}]

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            fake_list_assistants,
        )

        result = CoordinatorWorkspaceManager().list_assistants()

        assert result == [{"agent_id": 42, "first_name": "Ops", "organization_id": 7}]
        assert calls == [
            {
                "phone": None,
                "email": None,
                "agent_id": None,
                "list_all_org": True,
                "api_key": "owner-key",  # pragma: allowlist secret
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
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            fake_list_assistants,
        )

        result = CoordinatorWorkspaceManager().list_assistants()

        assert result == [{"agent_id": 41, "organization_id": 13}]
        assert calls == [
            {
                "phone": None,
                "email": None,
                "agent_id": None,
                "list_all_org": False,
                "api_key": "owner-key",  # pragma: allowlist secret
            },
        ]

    def test_delete_requires_reachable_assistant(self, monkeypatch):
        delete_calls = []

        list_calls = []

        def fake_list_assistants(**kwargs):
            list_calls.append(kwargs)
            return [{"agent_id": 42}]

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            fake_list_assistants,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs)),
        )

        missing = CoordinatorWorkspaceManager().delete_assistant(agent_id=99)

        assert missing["error_kind"] == "not_found"
        assert list_calls[0]["agent_id"] == 99
        assert delete_calls == []

    def test_delete_assistant_checks_reachability_before_delete(
        self,
        monkeypatch,
    ):
        captured = {}
        delete_calls = []
        tools = CoordinatorWorkspaceManager()

        monkeypatch.setattr(
            _CoordinatorWorkspaceSession,
            "_assistant_is_reachable",
            lambda self, agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs))
            or {"status": "deleted"},
        )

        result = tools.delete_assistant(agent_id=42)

        assert result == {"status": "deleted"}
        assert captured == {"agent_id": 42}
        assert delete_calls == [
            ((42,), {"api_key": "owner-key"}),  # pragma: allowlist secret
        ]

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
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            fake_list_assistants,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delete_assistant",
            lambda *args, **kwargs: delete_calls.append((args, kwargs)) or {},
        )

        tools = CoordinatorWorkspaceManager()
        assert tools.list_assistants(phone="+15555551234") == [
            {"agent_id": 42, "organization_id": 7},
        ]

        result = tools.delete_assistant(agent_id=99)

        assert result == {}
        assert list_calls[1]["agent_id"] == 99
        assert delete_calls == [
            ((99,), {"api_key": "owner-key"}),  # pragma: allowlist secret
        ]

    def test_request_errors_return_tool_error(self, monkeypatch):
        def failing_create_assistant(**_):
            response = requests.Response()
            response.status_code = 403
            response._content = b"forbidden"
            raise RequestError("https://api.unify.ai", "POST", response)

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            failing_create_assistant,
        )

        result = CoordinatorWorkspaceManager().create_assistant(
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
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorWorkspaceManager().create_assistant(
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
                "api_key": "owner-key",  # pragma: allowlist secret
            },
        ]

    def test_create_assistant_requires_explicit_about(self, monkeypatch):
        create_calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorWorkspaceManager().create_assistant(
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
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorWorkspaceManager().create_assistant(
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
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        CoordinatorWorkspaceManager().create_assistant(
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
        tools = CoordinatorWorkspaceManager()

        monkeypatch.setattr(
            _CoordinatorWorkspaceSession,
            "_assistant_is_reachable",
            lambda self, agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.update_assistant_config",
            lambda *args, **kwargs: update_calls.append((args, kwargs))
            or {"agent_id": 42, "about": "updated"},
        )

        result = tools.update_assistant_config(
            agent_id=42,
            config={"about": "updated"},
        )

        assert result == {"agent_id": 42, "about": "updated"}
        assert captured == {"agent_id": 42}
        assert update_calls == [
            (
                (42, {"about": "updated"}),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),  # pragma: allowlist secret
        ]

    def test_create_assistant_derives_job_title_from_surname_for_multiword_first_name(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: calls.append(kwargs) or {"agent_id": 42},
        )

        CoordinatorWorkspaceManager().create_assistant(
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
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            failing_create_assistant,
        )

        result = CoordinatorWorkspaceManager().create_assistant(
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

        result = CoordinatorWorkspaceManager().add_setup_checklist_item(
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

        result = getattr(CoordinatorWorkspaceManager(), tool_name)(**tool_kwargs)

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

        result = getattr(CoordinatorWorkspaceManager(), tool_name)(**tool_kwargs)

        assert result["error_kind"] == "internal"
        assert result["message"] == expected_message
        assert result["details"]["error"] == "boom"

    def test_delegate_to_colleague_forwards_assignment_after_reachability(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delegate_to_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs))
            or {"target_assistant_id": 42},
        )

        result = CoordinatorWorkspaceManager().delegate_to_colleague(
            target_assistant_id=42,
            instruction="Schedule the renewal risk summary tomorrow morning.",
            intent="schedule_task",
            dedupe_key="renewal-risk-42",
        )

        assert result == {"target_assistant_id": 42}
        assert calls == [
            (
                (42,),
                {
                    "instruction": "Schedule the renewal risk summary tomorrow morning.",
                    "intent": "schedule_task",
                    "dedupe_key": "renewal-risk-42",
                    "related_context": None,
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_delegate_to_colleague_rejects_fabricated_assistant_without_sdk_call(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delegate_to_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().delegate_to_colleague(
            target_assistant_id=99,
            instruction="Remember that the renewal team checks blockers first.",
            intent="add_knowledge",
        )

        assert result["error_kind"] == "not_found"
        assert result["details"] == {"agent_id": 99}
        assert calls == []

    def test_delegate_to_colleague_rejects_blank_instruction(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delegate_to_colleague",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().delegate_to_colleague(
            target_assistant_id=42,
            instruction="  ",
        )

        assert result["error_kind"] == "invalid_argument"
        assert calls == []

    def test_delegate_to_colleague_checks_reachability_before_dispatch(
        self,
        monkeypatch,
    ):
        captured = {}
        delegate_calls = []
        tools = CoordinatorWorkspaceManager()

        monkeypatch.setattr(
            _CoordinatorWorkspaceSession,
            "_assistant_is_reachable",
            lambda self, agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delegate_to_colleague",
            lambda *args, **kwargs: delegate_calls.append((args, kwargs))
            or {"status": "attached_to_startup"},
        )

        result = tools.delegate_to_colleague(
            target_assistant_id=42,
            instruction="Audit the inbox tomorrow morning.",
            intent="schedule_task",
        )

        assert result == {"status": "attached_to_startup"}
        assert captured == {"agent_id": 42}
        assert delegate_calls == [
            (
                (42,),
                {
                    "instruction": "Audit the inbox tomorrow morning.",
                    "intent": "schedule_task",
                    "dedupe_key": None,
                    "related_context": None,
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_list_org_members_uses_active_workspace_organization(self, monkeypatch):
        list_members_calls = []

        def fake_list_org_members(*args, **kwargs):
            list_members_calls.append((args, kwargs))
            return [{"user_id": "member-1"}]

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_org_members",
            fake_list_org_members,
        )

        result = CoordinatorWorkspaceManager().list_org_members()

        assert result == [{"user_id": "member-1"}]
        assert list_members_calls == [
            ((7,), {"api_key": "owner-key"}),  # pragma: allowlist secret
        ]

    def test_list_org_members_requires_org_workspace(self, monkeypatch):
        SESSION_DETAILS.org_id = None
        calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_org_members",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().list_org_members()

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
            "unity.coordinator_manager.workspace_manager.unify.invite_org_member",
            fake_invite_org_member,
        )

        result = CoordinatorWorkspaceManager().invite_org_member(
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
                {
                    "role_name": "Admin",
                    "api_key": "owner-key",  # pragma: allowlist secret
                },  # pragma: allowlist secret
            ),
        ]

    def test_invite_org_member_no_org_context(self, monkeypatch):
        SESSION_DETAILS.org_id = None
        calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.invite_org_member",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().invite_org_member(
            email="sarah@example.com",
        )

        assert result["error_kind"] == "invalid_argument"
        assert "requires an organization workspace Coordinator" in result["message"]
        assert calls == []

    def test_invite_org_member_rejects_unknown_role_name(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.invite_org_member",
            lambda *args, **kwargs: calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().invite_org_member(
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
            "unity.coordinator_manager.workspace_manager.unify.invite_org_member",
            failing_invite_org_member,
        )

        result = CoordinatorWorkspaceManager().invite_org_member(
            email="sarah@example.com",
        )

        assert result["error_kind"] == "conflict"
        assert result["message"] == "User is already a member of this organization"

    def test_workspace_mutations_require_owner_or_admin_role(self, monkeypatch):
        mutation_calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Member"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.invite_org_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("invite_org_member", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_team",
            lambda *args, **kwargs: mutation_calls.append(("create_team", args, kwargs))
            or {"team_id": 11},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delete_team",
            lambda *args, **kwargs: mutation_calls.append(
                ("delete_team", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.update_team",
            lambda *args, **kwargs: mutation_calls.append(
                ("update_team", args, kwargs),
            )
            or {"team_id": 11},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("add_team_member", args, kwargs),
            )
            or {"membership_status": "active"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.remove_team_member",
            lambda *args, **kwargs: mutation_calls.append(
                ("remove_team_member", args, kwargs),
            )
            or {"ok": True},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: mutation_calls.append(("create_assistant", kwargs))
            or {"agent_id": 42},
        )

        tools = CoordinatorWorkspaceManager()
        results = {
            "invite_org_member": tools.invite_org_member(email="member-only@test.com"),
            "create_team": tools.create_team(
                name="Ops",
                description="Operations workspace.",
            ),
            "delete_team": tools.delete_team(team_id=11),
            "update_team": tools.update_team(team_id=11, patch={"name": "Ops v2"}),
            "add_team_member": tools.add_team_member(team_id=11, assistant_id=42),
            "remove_team_member": tools.remove_team_member(
                team_id=11,
                assistant_id=42,
            ),
            "commission_colleague_into_team": tools.commission_colleague_into_team(
                assistant_first_name="Avery",
                team_name="Ops",
                team_description="Operations workspace.",
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
            "unity.coordinator_manager.workspace_manager.unify.list_organizations",
            lambda **_: [{"id": 7, "name": "Acme", "role_name": "Admin"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_team",
            lambda *args, **kwargs: calls.append((args, kwargs))
            or {"team_id": 11, "name": "Ops"},
        )

        result = CoordinatorWorkspaceManager().create_team(
            name="Ops",
            description="Operations workspace.",
        )

        assert result == {"team_id": 11, "name": "Ops"}
        assert calls == [
            (
                (7,),
                {
                    "name": "Ops",
                    "description": "Operations workspace.",
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_team_member_writes_require_reachable_space_and_assistant(
        self,
        monkeypatch,
    ):
        member_calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs))
            or {"membership_status": "active"},
        )

        result = CoordinatorWorkspaceManager().add_team_member(
            team_id=11,
            assistant_id=42,
        )

        assert result == {"membership_status": "active"}
        assert member_calls == [
            (
                (7, 11),
                {
                    "assistant_id": 42,
                    "member_user_id": None,
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_team_member_writes_support_member_targeting(self, monkeypatch):
        member_calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_org_members",
            lambda *_, **__: [{"user_id": "member-1"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs))
            or {"membership_status": "active", "assistant_id": 91},
        )

        result = CoordinatorWorkspaceManager().add_team_member(
            team_id=11,
            member_user_id="member-1",
        )

        assert result == {"membership_status": "active", "assistant_id": 91}
        assert member_calls == [
            (
                (7, 11),
                {
                    "assistant_id": None,
                    "member_user_id": "member-1",
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_team_member_writes_require_exactly_one_target_shape(self):
        tools = CoordinatorWorkspaceManager()

        missing_target = tools.add_team_member(team_id=11)
        assert missing_target["error_kind"] == "invalid_argument"

        duplicate_target = tools.add_team_member(
            team_id=11,
            assistant_id=42,
            member_user_id="member-1",
        )
        assert duplicate_target["error_kind"] == "invalid_argument"

        blank_member_target = tools.add_team_member(
            team_id=11,
            member_user_id="   ",
        )
        assert blank_member_target["error_kind"] == "invalid_argument"

    def test_remaining_space_wrappers_forward_after_reachability(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11, "name": "Ops"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.update_team",
            lambda *args, **kwargs: calls.append(("update", args, kwargs))
            or {"team_id": 11, "name": "Ops Team"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.remove_team_member",
            lambda *args, **kwargs: calls.append(("remove", args, kwargs)) or {},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *args, **kwargs: calls.append(("members", args, kwargs))
            or [{"assistant_id": 42}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams_for_assistant",
            lambda *args, **kwargs: calls.append(("assistant_teams", args, kwargs))
            or [{"team_id": 11}],
        )

        tools = CoordinatorWorkspaceManager()

        assert tools.update_team(
            team_id=11,
            patch={"name": "Ops Team"},
        ) == {"team_id": 11, "name": "Ops Team"}
        assert tools.list_team_members(team_id=11) == [{"assistant_id": 42}]
        assert tools.list_teams_for_assistant(assistant_id=42) == [{"team_id": 11}]
        assert tools.remove_team_member(team_id=11, assistant_id=42) == {}
        assert calls == [
            (
                "update",
                (7, 11, {"name": "Ops Team"}),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),  # pragma: allowlist secret
            ("members", (7, 11), {"api_key": "owner-key"}),  # pragma: allowlist secret
            (
                "assistant_teams",
                (42,),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),  # pragma: allowlist secret
            (
                "remove",
                (7, 11, 42),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),
        ]

    def test_list_teams_for_assistant_checks_reachability_before_lookup(
        self,
        monkeypatch,
    ):
        captured = {}
        list_calls = []
        tools = CoordinatorWorkspaceManager()

        monkeypatch.setattr(
            _CoordinatorWorkspaceSession,
            "_assistant_is_reachable",
            lambda self, agent_id: captured.update({"agent_id": agent_id}) or True,
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams_for_assistant",
            lambda *args, **kwargs: list_calls.append((args, kwargs))
            or [{"team_id": 11}],
        )

        result = tools.list_teams_for_assistant(assistant_id=42)

        assert result == [{"team_id": 11}]
        assert captured == {"agent_id": 42}
        assert list_calls == [
            ((42,), {"api_key": "owner-key"}),  # pragma: allowlist secret
        ]

    def test_fabricated_team_id_returns_tool_error_without_sdk_write(
        self,
        monkeypatch,
    ):
        member_calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [{"agent_id": 42, "organization_id": 7}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: member_calls.append((args, kwargs)),
        )

        result = CoordinatorWorkspaceManager().add_team_member(
            team_id=99,
            assistant_id=42,
        )

        assert result["error_kind"] == "not_found"
        assert result["details"] == {"team_id": 99}
        assert member_calls == []

    def test_space_request_errors_return_tool_error(self, monkeypatch):
        def failing_delete_team(*_, **__):
            response = requests.Response()
            response.status_code = 409
            response._content = b"conflict"
            raise RequestError("https://api.unify.ai", "DELETE", response)

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.delete_team",
            failing_delete_team,
        )

        result = CoordinatorWorkspaceManager().delete_team(team_id=11)

        assert result["error_kind"] == "conflict"
        assert result["details"]["status_code"] == 409

    def test_commission_colleague_into_team_creates_missing_resources(
        self,
        monkeypatch,
    ):
        calls = []

        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **kwargs: calls.append(("list_assistants", kwargs)) or [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: calls.append(("create_assistant", kwargs))
            or {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *args, **kwargs: calls.append(("list_teams", args, kwargs))
            or (
                [{"team_id": 11, "name": "Ops HQ"}]
                if any(name == "create_team" for name, *_ in calls)
                else []
            ),
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_team",
            lambda *args, **kwargs: calls.append(("create_team", args, kwargs))
            or {"team_id": 11, "name": "Ops HQ"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *args, **kwargs: calls.append(("list_team_members", args, kwargs))
            or [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: calls.append(("add_team_member", args, kwargs))
            or {"membership_status": "active"},
        )

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            team_name="Ops HQ",
            team_description="Operations workspace",
            assistant_about="Leads operations workflows.",
        )

        assert result == {
            "assistant": {
                "status": "created",
                "assistant_id": 42,
                "assistant": {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
            },
            "team": {
                "status": "created",
                "team_id": 11,
                "team": {"team_id": 11, "name": "Ops HQ"},
            },
            "membership": {
                "status": "added",
                "team_id": 11,
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
                    "api_key": "owner-key",  # pragma: allowlist secret
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
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
            (
                "list_teams",
                (7,),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),
            (
                "create_team",
                (7,),
                {
                    "name": "Ops HQ",
                    "description": "Operations workspace",
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
            (
                "list_teams",
                (7,),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),
            (
                "list_team_members",
                (7, 11),
                {"api_key": "owner-key"},  # pragma: allowlist secret
            ),
            (
                "add_team_member",
                (7, 11),
                {
                    "assistant_id": 42,
                    "api_key": "owner-key",  # pragma: allowlist secret
                },
            ),
        ]

    def test_commission_colleague_into_team_requires_about_when_creating(
        self,
        monkeypatch,
    ):
        create_calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs) or {"agent_id": 42},
        )

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            team_name="Ops HQ",
            team_description="Operations workspace",
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
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs)
            or {"agent_id": 42, "first_name": "Ops", "surname": "Bot"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )

        CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            team_name="Ops HQ",
            team_description="Operations workspace",
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

    def test_commission_colleague_into_team_reuses_existing_membership(
        self,
        monkeypatch,
    ):
        add_calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
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
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: add_calls.append((args, kwargs)) or {},
        )

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            team_name="Ops HQ",
            team_description="Operations workspace",
        )

        assert result["assistant"]["status"] == "reused"
        assert result["team"]["status"] == "reused"
        assert result["membership"]["status"] == "already_member"
        assert add_calls == []

    def test_commission_colleague_into_team_reuses_explicit_ids(
        self,
        monkeypatch,
    ):
        create_calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
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
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11, "name": "Ops HQ"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *_, **__: [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.add_team_member",
            lambda *args, **kwargs: {"membership_status": "active"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(("assistant", kwargs)),
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_team",
            lambda **kwargs: create_calls.append(("team", kwargs)),
        )

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ignored",
            assistant_surname="Name",
            team_name="Ignored Space Name",
            team_description="ignored",
            assistant_id=42,
            team_id=11,
        )

        assert result["assistant"]["status"] == "reused"
        assert result["team"]["status"] == "reused"
        assert result["membership"]["status"] == "added"
        assert create_calls == []

    def test_commission_colleague_into_team_conflicts_on_ambiguous_assistant_name(
        self,
        monkeypatch,
    ):
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
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

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Ops",
            assistant_surname="Bot",
            team_name="Ops HQ",
            team_description="Operations workspace",
        )

        assert result["error_kind"] == "conflict"
        assert result["details"]["matches"] == [42, 43]

    def test_commission_colleague_into_team_applies_same_assistant_defaults(
        self,
        monkeypatch,
    ):
        SESSION_DETAILS.assistant.timezone = "Asia/Karachi"
        SESSION_DETAILS.assistant.nationality = "United States"
        create_calls = []
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_assistants",
            lambda **_: [],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.create_assistant",
            lambda **kwargs: create_calls.append(kwargs)
            or {"agent_id": 42, "first_name": "Sarah Chen", "surname": "Recruiter"},
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_teams",
            lambda *_, **__: [{"team_id": 11, "name": "Hiring Desk"}],
        )
        monkeypatch.setattr(
            "unity.coordinator_manager.workspace_manager.unify.list_team_members",
            lambda *_, **__: [{"assistant_id": 42}],
        )

        result = CoordinatorWorkspaceManager().commission_colleague_into_team(
            assistant_first_name="Sarah Chen",
            assistant_surname="Recruiter",
            team_name="Hiring Desk",
            team_description="Hiring workspace for sourcing and interview loops.",
            assistant_about="Leads recruiter scorecards and candidate routing.",
        )

        assert result["assistant"]["status"] == "created"
        assert result["team"]["status"] == "reused"
        assert result["membership"]["status"] == "already_member"
        assert create_calls[0]["config"] == {
            "timezone": "Asia/Karachi",
            "nationality": "United States",
            "job_title": "Recruiter",
            "about": "Leads recruiter scorecards and candidate routing.",
            "organization_id": 7,
        }
