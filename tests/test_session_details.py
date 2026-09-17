"""
tests/test_session_details.py
=============================

Unit tests for SessionDetails: population, env round-trips and nullable
runtime strings.
"""

import os

from unify.session_details import SessionDetails


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
            assistant_first_name="Unify",
            assistant_surname="",
            user_id="user-1",
            user_first_name="Ada",
            user_surname="Lovelace",
        )

        assert sd.is_initialized is True
        assert sd.assistant.name == "Unify"
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
            assistant_first_name="Unify",
            assistant_timezone="Europe/London",
            default_model="model@provider",
            slow_brain_model="slow@provider",
            user_id="user-1",
            user_email="user@example.com",
        )
        sd.export_to_env()

        assert os.environ["ASSISTANT_ID"] == "7"
        assert os.environ["ASSISTANT_FIRST_NAME"] == "Unify"
        assert os.environ["ASSISTANT_TIMEZONE"] == "Europe/London"
        assert os.environ["ASSISTANT_DEFAULT_MODEL"] == "model@provider"
        assert os.environ["ASSISTANT_SLOW_BRAIN_MODEL"] == "slow@provider"
        assert os.environ["USER_ID"] == "user-1"
        assert os.environ["USER_EMAIL"] == "user@example.com"

        sd2 = SessionDetails()
        sd2.populate_from_env()

        assert sd2.is_initialized is True
        assert sd2.assistant.agent_id == 7
        assert sd2.assistant.first_name == "Unify"
        assert sd2.assistant.timezone == "Europe/London"
        assert sd2.assistant.default_model == "model@provider"
        assert sd2.assistant.slow_brain_model == "slow@provider"
        assert sd2.user.id == "user-1"
        assert sd2.user.email == "user@example.com"


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
