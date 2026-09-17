"""
unify/session_details.py
=========================

Runtime details for the active assistant session.

This differs from unify.settings (SETTINGS) in a key way:
  - SETTINGS: Static configuration from environment/.env, frozen at import time
  - SESSION_DETAILS: Dynamic runtime state, populated when a session starts

Usage:
    from unify.session_details import SESSION_DETAILS

    if SESSION_DETAILS.is_initialized:
        print(SESSION_DETAILS.assistant.first_name)

    # All fields have sensible defaults, so no `or "fallback"` is needed
    name = SESSION_DETAILS.assistant.name  # computed from first_name + surname
"""

import os
from dataclasses import dataclass, field

# ─────────────────────────────────────────────────────────────────────────────
# Unassigned Identity Sentinels
# ─────────────────────────────────────────────────────────────────────────────
UNASSIGNED_USER_ID = "default"

# ─────────────────────────────────────────────────────────────────────────────
# Placeholder Contact Details
# Used in tests and before a session has populated a real profile. The local
# single-assistant experience is intentionally fixed to "Unity".
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_SELF_CONTACT_ID = 0
DEFAULT_BOSS_CONTACT_ID = 1
PLACEHOLDER_ASSISTANT_FIRST_NAME = "Unity"
PLACEHOLDER_ASSISTANT_SURNAME = None  # Contact.surname is Optional[str] with a
# UNICODE_NAME_RE pattern; empty string fails the pattern and Pydantic coerces
# to None anyway, so the placeholder matches what contacts end up with.
PLACEHOLDER_ASSISTANT_EMAIL = "assistant@unify.ai"
PLACEHOLDER_ASSISTANT_PHONE = "+10000000000"
PLACEHOLDER_ASSISTANT_BIO = "Your local Unity assistant."
PLACEHOLDER_USER_FIRST_NAME = "Default"
PLACEHOLDER_USER_SURNAME = "User"
PLACEHOLDER_USER_EMAIL = "user@example.com"

# ─────────────────────────────────────────────────────────────────────────────
# Context Path Defaults (for the store's context hierarchy)
# Format: {user_id}/{agent_id}/... e.g., "default/0/Contacts"
# ─────────────────────────────────────────────────────────────────────────────
UNASSIGNED_USER_CONTEXT = UNASSIGNED_USER_ID
UNASSIGNED_ASSISTANT_CONTEXT = "0"


def _runtime_str(value: object) -> str:
    """Normalize nullable runtime values to env-safe strings."""
    return "" if value is None else str(value)


@dataclass
class AssistantDetails:
    """Details about the assistant."""

    agent_id: int | None = None
    first_name: str = ""
    surname: str = ""
    age: str = ""
    nationality: str = ""
    timezone: str = ""  # IANA timezone identifier (e.g., "America/New_York")
    about: str = ""
    job_title: str = ""
    number: str = ""
    email: str = ""
    # Default LLM as a unillm 'model@provider' endpoint plus a reasoning-effort
    # level. Empty = platform default (UNIFY_MODEL and per-call-site efforts).
    default_model: str = ""
    default_reasoning_effort: str = ""
    # ConversationManager slow-brain LLM. Empty = SLOW_BRAIN_MODEL setting
    # (independent of default_model / UNIFY_MODEL).
    slow_brain_model: str = ""
    slow_brain_reasoning_effort: str = ""
    contact_id: int = 0  # Contact ID in Contacts table
    self_contact_id: int = 0

    @property
    def name(self) -> str:
        return f"{self.first_name} {self.surname}".strip()


@dataclass
class UserDetails:
    """Details about the user and their assistant-scoped boss identity."""

    id: str = UNASSIGNED_USER_ID
    first_name: str = ""
    surname: str = ""
    number: str = ""
    email: str = ""
    boss_contact_id: int = 1

    @property
    def name(self) -> str:
        return f"{self.first_name} {self.surname}".strip()


@dataclass
class SessionDetails:
    """Runtime details populated on startup.

    All fields have sensible defaults so callers never need `or "fallback"` patterns.
    """

    assistant: AssistantDetails = field(default_factory=AssistantDetails)
    user: UserDetails = field(default_factory=UserDetails)

    _initialized: bool = field(default=False, repr=False)

    @property
    def assistant_context(self) -> str:
        """The assistant's agent_id as the context path component."""
        if self.assistant.agent_id is not None:
            return str(self.assistant.agent_id)
        return UNASSIGNED_ASSISTANT_CONTEXT

    @property
    def user_context(self) -> str:
        """The user's ID used as the context path component."""
        return self.user.id or UNASSIGNED_USER_CONTEXT

    @property
    def is_initialized(self) -> bool:
        """Returns True if populate() has been called."""
        return self._initialized

    @property
    def user_id(self) -> str:
        """Shortcut to user.id for convenient access."""
        return self.user.id

    @property
    def self_contact_id(self) -> int:
        """Shortcut to assistant.self_contact_id for convenient access."""
        return self.assistant.self_contact_id

    @self_contact_id.setter
    def self_contact_id(self, value: int) -> None:
        self.assistant.self_contact_id = value

    @property
    def boss_contact_id(self) -> int:
        """Shortcut to user.boss_contact_id for convenient access."""
        return self.user.boss_contact_id

    @boss_contact_id.setter
    def boss_contact_id(self, value: int) -> None:
        self.user.boss_contact_id = value

    def populate(
        self,
        *,
        agent_id: int | None = None,
        assistant_first_name: str = "",
        assistant_surname: str = "",
        assistant_age: str = "",
        assistant_nationality: str = "",
        assistant_timezone: str = "",
        assistant_about: str = "",
        assistant_job_title: str = "",
        assistant_number: str = "",
        assistant_email: str = "",
        assistant_contact_id: int = 0,
        assistant_self_contact_id: int = DEFAULT_SELF_CONTACT_ID,
        user_id: str = "",
        user_first_name: str = "",
        user_surname: str = "",
        user_number: str = "",
        user_email: str = "",
        user_boss_contact_id: int = DEFAULT_BOSS_CONTACT_ID,
        default_model: str = "",
        default_reasoning_effort: str = "",
        slow_brain_model: str = "",
        slow_brain_reasoning_effort: str = "",
    ) -> None:
        """Populate the session with runtime values."""
        self.assistant.agent_id = agent_id
        self.assistant.first_name = _runtime_str(assistant_first_name)
        self.assistant.surname = _runtime_str(assistant_surname)
        self.assistant.age = _runtime_str(assistant_age)
        self.assistant.nationality = _runtime_str(assistant_nationality)
        self.assistant.timezone = _runtime_str(assistant_timezone)
        self.assistant.about = _runtime_str(assistant_about)
        self.assistant.job_title = _runtime_str(assistant_job_title)
        self.assistant.number = _runtime_str(assistant_number)
        self.assistant.email = _runtime_str(assistant_email)
        self.assistant.contact_id = assistant_contact_id
        self.self_contact_id = assistant_self_contact_id
        self.user.id = _runtime_str(user_id)
        self.user.first_name = _runtime_str(user_first_name)
        self.user.surname = _runtime_str(user_surname)
        self.user.number = _runtime_str(user_number)
        self.user.email = _runtime_str(user_email)
        self.boss_contact_id = user_boss_contact_id
        self.assistant.default_model = _runtime_str(default_model)
        self.assistant.default_reasoning_effort = _runtime_str(
            default_reasoning_effort,
        )
        self.assistant.slow_brain_model = _runtime_str(slow_brain_model)
        self.assistant.slow_brain_reasoning_effort = _runtime_str(
            slow_brain_reasoning_effort,
        )
        self._initialized = True

    def reset(self) -> None:
        """Reset to default state (useful for tests)."""
        self.assistant = AssistantDetails()
        self.user = UserDetails()
        self._initialized = False

    def export_to_env(self) -> None:
        """Export current values to environment variables for subprocesses."""
        os.environ["ASSISTANT_ID"] = (
            str(self.assistant.agent_id) if self.assistant.agent_id is not None else ""
        )
        os.environ["ASSISTANT_FIRST_NAME"] = _runtime_str(self.assistant.first_name)
        os.environ["ASSISTANT_SURNAME"] = _runtime_str(self.assistant.surname)
        os.environ["ASSISTANT_NAME"] = _runtime_str(self.assistant.name)
        os.environ["ASSISTANT_AGE"] = _runtime_str(self.assistant.age)
        os.environ["ASSISTANT_NATIONALITY"] = _runtime_str(self.assistant.nationality)
        os.environ["ASSISTANT_TIMEZONE"] = _runtime_str(self.assistant.timezone)
        os.environ["ASSISTANT_ABOUT"] = _runtime_str(self.assistant.about)
        os.environ["ASSISTANT_JOB_TITLE"] = _runtime_str(self.assistant.job_title)
        os.environ["ASSISTANT_NUMBER"] = _runtime_str(self.assistant.number)
        os.environ["ASSISTANT_EMAIL"] = _runtime_str(self.assistant.email)
        os.environ["ASSISTANT_DEFAULT_MODEL"] = _runtime_str(
            self.assistant.default_model,
        )
        os.environ["ASSISTANT_DEFAULT_REASONING_EFFORT"] = _runtime_str(
            self.assistant.default_reasoning_effort,
        )
        os.environ["ASSISTANT_SLOW_BRAIN_MODEL"] = _runtime_str(
            self.assistant.slow_brain_model,
        )
        os.environ["ASSISTANT_SLOW_BRAIN_REASONING_EFFORT"] = _runtime_str(
            self.assistant.slow_brain_reasoning_effort,
        )
        os.environ["USER_ID"] = _runtime_str(self.user.id)
        os.environ["USER_FIRST_NAME"] = _runtime_str(self.user.first_name)
        os.environ["USER_SURNAME"] = _runtime_str(self.user.surname)
        os.environ["USER_NUMBER"] = _runtime_str(self.user.number)
        os.environ["USER_EMAIL"] = _runtime_str(self.user.email)
        self.export_contact_ids_to_env()

    def export_contact_ids_to_env(self) -> None:
        """Export resolved self and boss contact ids to the subprocess env shape."""
        os.environ["SELF_CONTACT_ID"] = str(self.assistant.self_contact_id)
        os.environ["BOSS_CONTACT_ID"] = str(self.user.boss_contact_id)

    def populate_from_env(self) -> None:
        """Populate from environment variables.

        Useful for subprocesses that inherit env vars from parent process.
        Only sets fields if the corresponding env var is non-empty.
        """
        if val := os.environ.get("ASSISTANT_ID"):
            try:
                self.assistant.agent_id = int(val)
            except (ValueError, TypeError):
                pass
        for env_name, attr in (
            ("ASSISTANT_FIRST_NAME", "first_name"),
            ("ASSISTANT_SURNAME", "surname"),
            ("ASSISTANT_AGE", "age"),
            ("ASSISTANT_NATIONALITY", "nationality"),
            ("ASSISTANT_TIMEZONE", "timezone"),
            ("ASSISTANT_ABOUT", "about"),
            ("ASSISTANT_JOB_TITLE", "job_title"),
            ("ASSISTANT_NUMBER", "number"),
            ("ASSISTANT_EMAIL", "email"),
            ("ASSISTANT_DEFAULT_MODEL", "default_model"),
            ("ASSISTANT_DEFAULT_REASONING_EFFORT", "default_reasoning_effort"),
            ("ASSISTANT_SLOW_BRAIN_MODEL", "slow_brain_model"),
            ("ASSISTANT_SLOW_BRAIN_REASONING_EFFORT", "slow_brain_reasoning_effort"),
        ):
            if val := os.environ.get(env_name):
                setattr(self.assistant, attr, val)
        for env_name, attr in (
            ("USER_ID", "id"),
            ("USER_FIRST_NAME", "first_name"),
            ("USER_SURNAME", "surname"),
            ("USER_NUMBER", "number"),
            ("USER_EMAIL", "email"),
        ):
            if val := os.environ.get(env_name):
                setattr(self.user, attr, val)
        if val := os.environ.get("SELF_CONTACT_ID"):
            try:
                self.self_contact_id = int(val)
            except (ValueError, TypeError):
                pass
        if val := os.environ.get("BOSS_CONTACT_ID"):
            try:
                self.boss_contact_id = int(val)
            except (ValueError, TypeError):
                pass
        if self.assistant.agent_id is not None:
            self._initialized = True

    def get_subprocess_env(self, **overrides: str) -> dict[str, str]:
        """Get a copy of the current environment for subprocess use.

        First exports SESSION_DETAILS values, then returns a copy with any
        overrides applied.
        """
        self.export_to_env()
        env = dict(os.environ)
        env.update(overrides)
        return env

    @staticmethod
    def get_impl_setting(name: str, default: str = "real") -> str:
        """Get implementation setting from environment for test-time override.

        SETTINGS is instantiated at import time, before test conftests can set
        environment variables. This helper reads directly from os.environ to
        allow conftests to set UNIFY_*_IMPL=simulated and have it take effect.
        """
        return os.environ.get(name, default)


# Global singleton instance
SESSION_DETAILS = SessionDetails()


def is_self_contact(contact_id: int | None) -> bool:
    """Return whether a contact id is the assistant's own contact identity."""

    return contact_id is not None and int(contact_id) == SESSION_DETAILS.self_contact_id


def is_boss_contact(contact_id: int | None) -> bool:
    """Return whether a contact id is the boss contact identity."""

    return contact_id is not None and int(contact_id) == SESSION_DETAILS.boss_contact_id
