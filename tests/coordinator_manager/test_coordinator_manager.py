from __future__ import annotations

from unittest.mock import patch

import pytest
import requests
from unify.utils.http import RequestError

from unity.coordinator_manager.coordinator_manager import CoordinatorOnboardingManager
from unity.manager_registry import ManagerRegistry
from unity.session_details import SESSION_DETAILS


@pytest.fixture(autouse=True)
def reset_coordinator_manager_state():
    ManagerRegistry.clear()
    SESSION_DETAILS.reset()
    yield
    ManagerRegistry.clear()
    SESSION_DETAILS.reset()


class TestCoordinatorOnboardingManager:
    def test_direct_instantiation_registers_singleton_instance(self):
        manager = CoordinatorOnboardingManager()
        assert ManagerRegistry.get_instance(CoordinatorOnboardingManager) is manager

    def test_personal_coordinator_has_empty_authorized_humans(self):
        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_org_members",
        ) as list_members:
            assert manager.get_org_members() == []

        list_members.assert_not_called()

    def test_org_members_are_cached(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_org_members",
            return_value=[{"email": "dana@acme.com"}],
        ) as list_members:
            assert manager.get_org_members() == [{"email": "dana@acme.com"}]
            assert manager.get_org_members() == [{"email": "dana@acme.com"}]

        list_members.assert_called_once_with(
            7,
            api_key="owner-key",  # pragma: allowlist secret
        )

    def test_org_member_failures_do_not_poison_cache(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        response = requests.Response()
        response.status_code = 500
        response._content = b"temporary"
        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_org_members",
            side_effect=[
                RequestError("https://api.unify.ai", "GET", response),
                [{"email": "dana@acme.com"}],
            ],
        ) as list_members:
            assert manager.get_org_members() == []
            assert manager.get_org_members() == [{"email": "dana@acme.com"}]

        assert list_members.call_count == 2

    def test_workspace_coordinator_name_uses_user_assistant_listing(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_assistants",
            return_value=[
                {
                    "first_name": "Patch",
                    "surname": "One",
                    "is_coordinator": False,
                },
                {
                    "first_name": "Avery",
                    "surname": "Coordinator",
                    "is_coordinator": True,
                },
            ],
        ) as list_assistants:
            assert manager.get_workspace_coordinator_name() == "Avery Coordinator"
            assert manager.get_workspace_coordinator_name() == "Avery Coordinator"

        list_assistants.assert_called_once_with(
            api_key="owner-key",  # pragma: allowlist secret
        )

    def test_workspace_coordinator_name_returns_none_when_no_coordinator_exists(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_assistants",
            return_value=[{"first_name": "Patch", "surname": "One"}],
        ):
            assert manager.get_workspace_coordinator_name() is None

    def test_workspace_coordinator_name_failures_do_not_poison_cache(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        response = requests.Response()
        response.status_code = 500
        response._content = b"temporary"
        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_assistants",
            side_effect=[
                RequestError("https://api.unify.ai", "GET", response),
                [
                    {
                        "first_name": "Avery",
                        "surname": "Coordinator",
                        "is_coordinator": True,
                    },
                ],
            ],
        ) as list_assistants:
            assert manager.get_workspace_coordinator_name() is None
            assert manager.get_workspace_coordinator_name() == "Avery Coordinator"

        assert list_assistants.call_count == 2

    def test_workspace_coordinator_name_cache_follows_api_key_identity(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_assistants",
            side_effect=[
                [
                    {
                        "first_name": "Avery",
                        "surname": "Coordinator",
                        "is_coordinator": True,
                    },
                ],
                [
                    {
                        "first_name": "Blair",
                        "surname": "Coordinator",
                        "is_coordinator": True,
                    },
                ],
            ],
        ) as list_assistants:
            assert manager.get_workspace_coordinator_name() == "Avery Coordinator"
            SESSION_DETAILS.org_id = 8
            assert manager.get_workspace_coordinator_name() == "Avery Coordinator"
            SESSION_DETAILS.unify_key = "admin-key"
            assert manager.get_workspace_coordinator_name() == "Blair Coordinator"

        assert list_assistants.call_count == 2

    def test_org_member_cache_follows_session_identity(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorOnboardingManager()

        with patch(
            "unity.coordinator_manager.coordinator_manager.unify.list_org_members",
            side_effect=[
                [{"email": "owner@acme.com"}],
                [{"email": "admin@beta.com"}],
            ],
        ) as list_members:
            assert manager.get_org_members() == [{"email": "owner@acme.com"}]
            SESSION_DETAILS.org_id = 8
            SESSION_DETAILS.unify_key = "admin-key"
            assert manager.get_org_members() == [{"email": "admin@beta.com"}]

        assert list_members.call_count == 2
