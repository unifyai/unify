"""Tests for CM eval assistant identity env overrides."""

from __future__ import annotations

import os

import pytest

from tests.conversation_manager.assistant_identity_env import (
    TEST_ASSISTANT_EMAIL,
    TEST_ASSISTANT_NUMBER,
    ensure_test_assistant_identity_env,
)

pytestmark = pytest.mark.no_unify_context


def test_ensure_overrides_blank_assistant_identity(monkeypatch):
    monkeypatch.setenv("ASSISTANT_EMAIL", "")
    monkeypatch.setenv("ASSISTANT_NUMBER", "   ")

    ensure_test_assistant_identity_env()

    assert os.environ["ASSISTANT_EMAIL"] == TEST_ASSISTANT_EMAIL
    assert os.environ["ASSISTANT_NUMBER"] == TEST_ASSISTANT_NUMBER


def test_ensure_preserves_explicit_assistant_identity(monkeypatch):
    monkeypatch.setenv("ASSISTANT_EMAIL", "custom@test.example.com")
    monkeypatch.setenv("ASSISTANT_NUMBER", "+15559998888")

    ensure_test_assistant_identity_env()

    assert os.environ["ASSISTANT_EMAIL"] == "custom@test.example.com"
    assert os.environ["ASSISTANT_NUMBER"] == "+15559998888"


def test_ensure_blanks_ambient_assistant_id(monkeypatch):
    """An assistant id pinned in the machine's .env must not leak into CM
    test boots: populate_from_env skips blank values, so agent_id stays None
    and the boot binds the default local identity."""
    monkeypatch.setenv("ASSISTANT_ID", "524")

    ensure_test_assistant_identity_env()

    assert os.environ["ASSISTANT_ID"] == ""
