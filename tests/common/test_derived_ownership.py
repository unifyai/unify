"""Unit tests for SESSION_DETAILS.bind_derived_ownership.

Team ownership is derived from the platform's assistant record at boot.
A launcher-delivered value is only a cross-check: absence self-heals from
the record, and disagreement stops the boot instead of picking a side —
the July failure mode was an omitted hop silently routing a team-owned
assistant's shared storage to the personal root.
"""

from __future__ import annotations

import os

import pytest

import unisdk
from unify.session_details import SessionDetails


def _session(agent_id: int | None, delivered: int | None) -> SessionDetails:
    details = SessionDetails()
    details.assistant.agent_id = agent_id
    details.assistant.owner_team_id = delivered
    return details


def _record(owner_team_id: int | None) -> list[dict]:
    return [{"agent_id": "42", "owner_team_id": owner_team_id}]


def test_derived_ownership_heals_a_missing_delivery(monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(11))
    details = _session(agent_id=42, delivered=None)

    details.bind_derived_ownership()

    assert details.assistant.owner_team_id == 11
    assert os.environ["OWNER_TEAM_ID"] == "11"


def test_derived_ownership_confirms_a_matching_delivery(monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(11))
    details = _session(agent_id=42, delivered=11)

    details.bind_derived_ownership()

    assert details.assistant.owner_team_id == 11


def test_derived_ownership_split_brain_stops_the_boot(monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(7))
    details = _session(agent_id=42, delivered=11)

    with pytest.raises(RuntimeError, match="split-brain"):
        details.bind_derived_ownership()


def test_derived_ownership_personal_assistant_stays_personal(monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: _record(None))
    details = _session(agent_id=42, delivered=None)

    details.bind_derived_ownership()

    assert details.assistant.owner_team_id is None
    assert os.environ["OWNER_TEAM_ID"] == ""


def test_derived_ownership_requires_a_platform_record(monkeypatch):
    monkeypatch.setattr(unisdk, "list_assistants", lambda agent_id: [])
    details = _session(agent_id=42, delivered=None)

    with pytest.raises(RuntimeError, match="no platform record"):
        details.bind_derived_ownership()


def test_derived_ownership_skips_when_unbound():
    details = _session(agent_id=None, delivered=None)

    # No assistant identity yet (e.g. a bare subprocess): nothing to derive,
    # nothing to fetch.
    details.bind_derived_ownership()

    assert details.assistant.owner_team_id is None
