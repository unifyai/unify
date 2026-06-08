"""Coordinator workspace lookup helpers."""

from __future__ import annotations

from typing import Any

import unify
from unify.utils.http import RequestError

from unity.common.colleague_cache import (
    assistant_display_name as resolve_assistant_display_name,
)
from unity.manager_registry import SingletonABCMeta
from unity.session_details import SESSION_DETAILS

_CACHE_EMPTY = object()


class CoordinatorOnboardingManager(metaclass=SingletonABCMeta):
    """Resolve org membership and workspace coordinator metadata."""

    def __init__(self) -> None:
        self._org_members_cache_key: tuple[int | None, str] | object = _CACHE_EMPTY
        self._org_members_cache: list[dict[str, Any]] | object = _CACHE_EMPTY
        self._workspace_coordinator_name_cache_key: str | None | object = _CACHE_EMPTY
        self._workspace_coordinator_name_cache: str | None | object = _CACHE_EMPTY

    def get_org_members(self) -> list[dict[str, Any]]:
        """Return authorized humans in the Coordinator's organization."""

        cache_key = _org_members_cache_key()
        if (
            self._org_members_cache_key == cache_key
            and self._org_members_cache is not _CACHE_EMPTY
        ):
            return self._org_members_cache  # type: ignore[return-value]

        if SESSION_DETAILS.org_id is None:
            self._org_members_cache_key = cache_key
            self._org_members_cache = []
            return []

        try:
            members = unify.list_org_members(
                SESSION_DETAILS.org_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError:
            return []
        self._org_members_cache_key = cache_key
        self._org_members_cache = members
        return members

    def get_workspace_coordinator_name(self) -> str | None:
        """Return the display name for the active workspace Coordinator."""

        cache_key = _workspace_coordinator_cache_key()
        if (
            self._workspace_coordinator_name_cache_key == cache_key
            and self._workspace_coordinator_name_cache is not _CACHE_EMPTY
        ):
            return self._workspace_coordinator_name_cache  # type: ignore[return-value]

        try:
            assistants = unify.list_assistants(api_key=SESSION_DETAILS.unify_key)
        except RequestError:
            return None

        coordinator_name = next(
            (
                _assistant_display_name(assistant)
                for assistant in assistants
                if assistant.get("is_coordinator") is True
            ),
            None,
        )
        self._workspace_coordinator_name_cache_key = cache_key
        self._workspace_coordinator_name_cache = coordinator_name
        return coordinator_name


def _org_members_cache_key() -> tuple[int | None, str]:
    return SESSION_DETAILS.org_id, SESSION_DETAILS.unify_key


def _workspace_coordinator_cache_key() -> str | None:
    return SESSION_DETAILS.unify_key


def _assistant_display_name(assistant: dict[str, Any]) -> str | None:
    return resolve_assistant_display_name(assistant)
