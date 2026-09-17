"""
tests/test_session_details.py
=============================

Unit tests for SessionDetails: population, env round-trips, nullable
runtime strings, and the resolved self/boss contact identities.
"""

import os
from dataclasses import fields

from unify.session_details import (
    SESSION_DETAILS,
    AssistantDetails,
    SessionDetails,
    UserDetails,
    is_boss_contact,
    is_self_contact,
)


class TestPopulate:
    def test_defaults_are_uninitialised(self):
        sd = SessionDetails()

        assert sd.is_initialized is False
        assert sd.assistant.agent_id is None
        assert sd.assistant_context == "0"
        assert sd.user_context == "default"

    def test_populate_sets_identity_and_contexts(self):
        sd = SessionDetails()
        sd.populate(
            agent_id=42,
            assistant_first_name="Unity",
            assistant_surname="",
            user_id="user-1",
            user_first_name="Ada",
            user_surname="Lovelace",
        )

        assert sd.is_initialized is True
        assert sd.assistant.name == "Unity"
        assert sd.user.name == "Ada Lovelace"
        assert sd.user_id == "user-1"
        assert sd.assistant_context == "42"
        assert sd.user_context == "user-1"

    def test_reset_restores_defaults(self):
        sd = SessionDetails()
        sd.populate(agent_id=42, user_id="user-1", default_model="m@p")

        sd.reset()

        assert sd.is_initialized is False
        assert sd.assistant.agent_id is None
        assert sd.user.id == "default"
        assert sd.assistant.default_model == ""


class TestEnvRoundTrip:
    def test_export_and_populate_from_env_round_trips(self, monkeypatch):
        for key in (
            "ASSISTANT_ID",
            "ASSISTANT_FIRST_NAME",
            "ASSISTANT_TIMEZONE",
            "ASSISTANT_DEFAULT_MODEL",
            "ASSISTANT_SLOW_BRAIN_MODEL",
            "USER_ID",
            "USER_EMAIL",
        ):
            monkeypatch.delenv(key, raising=False)

        sd = SessionDetails()
        sd.populate(
            agent_id=7,
            assistant_first_name="Unity",
            assistant_timezone="Europe/London",
            default_model="model@provider",
            slow_brain_model="slow@provider",
            user_id="user-1",
            user_email="user@example.com",
        )
        sd.export_to_env()

        assert os.environ["ASSISTANT_ID"] == "7"
        assert os.environ["ASSISTANT_FIRST_NAME"] == "Unity"
        assert os.environ["ASSISTANT_TIMEZONE"] == "Europe/London"
        assert os.environ["ASSISTANT_DEFAULT_MODEL"] == "model@provider"
        assert os.environ["ASSISTANT_SLOW_BRAIN_MODEL"] == "slow@provider"
        assert os.environ["USER_ID"] == "user-1"
        assert os.environ["USER_EMAIL"] == "user@example.com"

        sd2 = SessionDetails()
        sd2.populate_from_env()

        assert sd2.is_initialized is True
        assert sd2.assistant.agent_id == 7
        assert sd2.assistant.first_name == "Unity"
        assert sd2.assistant.timezone == "Europe/London"
        assert sd2.assistant.default_model == "model@provider"
        assert sd2.assistant.slow_brain_model == "slow@provider"
        assert sd2.user.id == "user-1"
        assert sd2.user.email == "user@example.com"

    def test_get_subprocess_env_applies_overrides(self, monkeypatch):
        monkeypatch.delenv("ASSISTANT_FIRST_NAME", raising=False)
        sd = SessionDetails()
        sd.populate(agent_id=7, assistant_first_name="Unity")

        env = sd.get_subprocess_env(EXTRA_FLAG="1")

        assert env["ASSISTANT_FIRST_NAME"] == "Unity"
        assert env["EXTRA_FLAG"] == "1"


class TestNullableRuntimeStrings:
    def test_populate_and_export_coerce_none_strings(self, monkeypatch):
        for key in ("ASSISTANT_SURNAME", "USER_NUMBER"):
            monkeypatch.delenv(key, raising=False)

        sd = SessionDetails()
        sd.populate(
            assistant_first_name="T-W1N",
            assistant_surname=None,
            user_number=None,
        )

        assert sd.assistant.surname == ""
        assert sd.user.number == ""

        sd.export_to_env()

        assert os.environ["ASSISTANT_SURNAME"] == ""
        assert os.environ["USER_NUMBER"] == ""


class TestContactIds:
    def test_defaults_and_shortcuts_use_subcontainer_storage(self):
        sd = SessionDetails()

        assert sd.self_contact_id == 0
        assert sd.boss_contact_id == 1

        sd.self_contact_id = 5
        sd.boss_contact_id = 6

        assert sd.assistant.self_contact_id == 5
        assert sd.user.boss_contact_id == 6

    def test_populate_sets_resolved_contact_ids(self):
        sd = SessionDetails()

        sd.populate(assistant_self_contact_id=42, user_boss_contact_id=43)

        assert sd.self_contact_id == 42
        assert sd.assistant.self_contact_id == 42
        assert sd.boss_contact_id == 43
        assert sd.user.boss_contact_id == 43

    def test_subcontainers_do_not_expose_ambiguous_contact_id_fields(self):
        assistant_fields = {field.name for field in fields(AssistantDetails)}
        user_fields = {field.name for field in fields(UserDetails)}

        # AssistantDetails owns an explicit Contacts-table contact_id plus
        # self_contact_id; UserDetails must not expose a bare contact_id.
        assert "contact_id" in assistant_fields
        assert "self_contact_id" in assistant_fields
        assert "contact_id" not in user_fields

    def test_export_and_populate_from_env_round_trips(self, monkeypatch):
        monkeypatch.delenv("SELF_CONTACT_ID", raising=False)
        monkeypatch.delenv("BOSS_CONTACT_ID", raising=False)
        sd = SessionDetails()
        sd.populate(assistant_self_contact_id=42, user_boss_contact_id=43)
        sd.export_to_env()

        assert os.environ["SELF_CONTACT_ID"] == "42"
        assert os.environ["BOSS_CONTACT_ID"] == "43"

        sd2 = SessionDetails()
        sd2.populate_from_env()
        assert sd2.self_contact_id == 42
        assert sd2.boss_contact_id == 43

    def test_reset_restores_contact_id_defaults(self):
        sd = SessionDetails()
        sd.populate(assistant_self_contact_id=42, user_boss_contact_id=43)

        sd.reset()

        assert sd.self_contact_id == 0
        assert sd.boss_contact_id == 1

    def test_identity_predicates_use_resolved_session_ids(self):
        original_self = SESSION_DETAILS.self_contact_id
        original_boss = SESSION_DETAILS.boss_contact_id
        try:
            SESSION_DETAILS.self_contact_id = 42
            SESSION_DETAILS.boss_contact_id = 43

            assert is_self_contact(42)
            assert not is_self_contact(43)
            assert is_boss_contact(43)
            assert not is_boss_contact(42)
        finally:
            SESSION_DETAILS.self_contact_id = original_self
            SESSION_DETAILS.boss_contact_id = original_boss
