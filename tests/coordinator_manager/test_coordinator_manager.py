from __future__ import annotations

from unittest.mock import patch

import pytest
import requests
from unisdk.utils.http import RequestError

from unify.coordinator_manager.coordinator_manager import CoordinatorManager
from unify.manager_registry import ManagerRegistry
from unify.session_details import SESSION_DETAILS


@pytest.fixture(autouse=True)
def reset_coordinator_manager_state():
    ManagerRegistry.clear()
    SESSION_DETAILS.reset()
    yield
    ManagerRegistry.clear()
    SESSION_DETAILS.reset()


class TestCoordinatorManager:
    def test_direct_instantiation_registers_singleton_instance(self):
        manager = CoordinatorManager()
        assert ManagerRegistry.get_instance(CoordinatorManager) is manager

    def test_personal_coordinator_has_empty_authorized_humans(self):
        manager = CoordinatorManager()

        with patch(
            "unify.coordinator_manager.coordinator_manager.unisdk.list_org_members",
        ) as list_members:
            assert manager.get_org_members() == []

        list_members.assert_not_called()

    def test_org_members_are_cached(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorManager()

        with patch(
            "unify.coordinator_manager.coordinator_manager.unisdk.list_org_members",
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

        manager = CoordinatorManager()

        response = requests.Response()
        response.status_code = 500
        response._content = b"temporary"
        with patch(
            "unify.coordinator_manager.coordinator_manager.unisdk.list_org_members",
            side_effect=[
                RequestError("https://api.unify.ai", "GET", response),
                [{"email": "dana@acme.com"}],
            ],
        ) as list_members:
            assert manager.get_org_members() == []
            assert manager.get_org_members() == [{"email": "dana@acme.com"}]

        assert list_members.call_count == 2

    def test_org_member_cache_follows_session_identity(self):
        SESSION_DETAILS.org_id = 7
        SESSION_DETAILS.unify_key = "owner-key"

        manager = CoordinatorManager()

        with patch(
            "unify.coordinator_manager.coordinator_manager.unisdk.list_org_members",
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
