"""Fixtures for coordinator local-stack integration tests."""

from __future__ import annotations

import os

import pytest

from tests.coordinator_manager.integration.local_stack_harness import (
    LocalStackUrls,
    ManagedLocalStack,
    apply_local_stack_credentials,
    local_stack_auto_manage_enabled,
    local_stack_is_ready,
    reset_and_start_local_stack,
    resolve_local_stack_credentials,
    resolve_local_stack_urls,
    stop_local_stack,
    wait_for_local_stack,
)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "local_stack: tests that require the local self-host stack (stack.sh up)",
    )


@pytest.fixture(scope="session")
def local_stack_urls() -> LocalStackUrls:
    return resolve_local_stack_urls()


@pytest.fixture(scope="session")
def managed_local_stack(local_stack_urls) -> ManagedLocalStack:
    """Ensure a fresh local self-host stack for ``local_stack`` tests."""

    if not local_stack_auto_manage_enabled():
        yield ManagedLocalStack(started_by_session=False, urls=local_stack_urls)
        return

    started_by_session = False
    try:
        reset_and_start_local_stack(local_stack_urls)
        started_by_session = True
        unify_key, admin_key = resolve_local_stack_credentials()
        if not unify_key:
            raise RuntimeError(
                "Fresh local stack has no UNIFY_KEY in env or bootstrap output",
            )
        apply_local_stack_credentials(unify_key=unify_key, admin_key=admin_key)
        wait_for_local_stack(
            local_stack_urls,
            unify_key=unify_key,
            admin_key=admin_key,
        )
        yield ManagedLocalStack(
            started_by_session=True,
            urls=local_stack_urls,
        )
    finally:
        if started_by_session and os.getenv(
            "LOCAL_STACK_LEAVE_RUNNING",
            "",
        ).strip().lower() not in {
            "1",
            "true",
            "yes",
        }:
            stop_local_stack()


@pytest.fixture(scope="session")
def require_local_stack(managed_local_stack, local_stack_urls) -> LocalStackUrls:
    """Require a reachable local self-host stack and resolved credentials."""

    unify_key, admin_key = resolve_local_stack_credentials()
    if not unify_key:
        pytest.skip(
            "UNIFY_KEY required for local stack integration tests "
            "(set explicitly or start stack to populate bootstrap credentials)",
        )
    if not admin_key:
        pytest.skip("ORCHESTRA_ADMIN_KEY required for local stack integration tests")
    apply_local_stack_credentials(unify_key=unify_key, admin_key=admin_key)

    if not local_stack_is_ready(
        local_stack_urls,
        unify_key=unify_key,
        admin_key=admin_key,
    ):
        if local_stack_auto_manage_enabled():
            pytest.fail(
                "Local stack is not reachable after auto-start: "
                f"Orchestra={local_stack_urls.orchestra_url}, "
                f"Adapters={local_stack_urls.adapters_url}, "
                f"Comms={local_stack_urls.comms_url}.",
            )
        pytest.skip(
            "Local stack is not running. Start it with "
            "`ORCHESTRA_REPO_PATH=... COMMUNICATION_REPO_PATH=... "
            "UNITY_REPO_PATH=... ./scripts/stack.sh up` "
            "or unset LOCAL_STACK_NO_AUTO to auto-manage.",
        )

    os.environ.setdefault(
        "PUBSUB_EMULATOR_HOST",
        local_stack_urls.pubsub_emulator_host,
    )
    os.environ["ORCHESTRA_URL"] = local_stack_urls.orchestra_url
    return local_stack_urls
