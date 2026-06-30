"""
unify/session_details.py
=========================

Runtime details for the active assistant session.

This differs from unify.settings (SETTINGS) in a key way:
  - SETTINGS: Static configuration from environment/.env, frozen at import time
  - SESSION_DETAILS: Dynamic runtime state, populated when a session starts

Populated dynamically when the ConversationManager receives a StartupEvent.

Usage:
    from unify.session_details import SESSION_DETAILS

    # Check if initialized
    if SESSION_DETAILS.is_initialized:
        print(SESSION_DETAILS.assistant.first_name)

    # All fields have sensible defaults, so no `or "fallback"` is needed
    name = SESSION_DETAILS.assistant.name  # computed from first_name + surname
"""

import json
import os
from dataclasses import dataclass, field

# ─────────────────────────────────────────────────────────────────────────────
# Unassigned Identity Sentinels
# Used by idle containers that haven't been assigned a real assistant/user yet.
# ─────────────────────────────────────────────────────────────────────────────
UNASSIGNED_USER_ID = "default"

# ─────────────────────────────────────────────────────────────────────────────
# Placeholder Contact Details
# Used in tests/offline mode and the local install when no real assistant
# profile has been provisioned (the hosted product populates these via
# StartupEvent; the local install never does, and the local single-assistant
# experience is intentionally fixed to "Unity").
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_SELF_CONTACT_ID = 0
DEFAULT_BOSS_CONTACT_ID = 1
PLACEHOLDER_ASSISTANT_FIRST_NAME = "Unity"
PLACEHOLDER_ASSISTANT_SURNAME = None  # Contact.surname is Optional[str] with a
# UNICODE_NAME_RE pattern; empty string fails the pattern and Pydantic coerces
# to None anyway. Keep the placeholder honest about the actual value contacts
# end up with — anything else triggers stale-equality bugs in tests like
# tests/contact_manager/test_sync.py::test_dummy_assistant.
PLACEHOLDER_ASSISTANT_EMAIL = "assistant@unify.ai"
PLACEHOLDER_ASSISTANT_PHONE = "+10000000000"
PLACEHOLDER_ASSISTANT_BIO = "Your local Unity assistant."
PLACEHOLDER_USER_FIRST_NAME = "Default"
PLACEHOLDER_USER_SURNAME = "User"
PLACEHOLDER_USER_EMAIL = "user@example.com"

# ─────────────────────────────────────────────────────────────────────────────
# Context Path Defaults (for Unify context hierarchy)
# Format: {user_id}/{agent_id}/... e.g., "default/0/Contacts"
# ─────────────────────────────────────────────────────────────────────────────
UNASSIGNED_USER_CONTEXT = UNASSIGNED_USER_ID
UNASSIGNED_ASSISTANT_CONTEXT = "0"

# ─────────────────────────────────────────────────────────────────────────────
# Voice Defaults
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_VOICE_PROVIDER = "cartesia"
DEFAULT_VOICE_MODE = "tts"


def _encode_int_csv(values: list[int]) -> str:
    """Encode integer ids for env vars that can only carry plain strings."""
    return ",".join(str(value) for value in values) if values else ""


def _decode_int_csv(value: str) -> list[int]:
    """Decode an integer-id CSV env var."""
    return [int(item) for item in value.split(",") if item.strip()]


def _runtime_str(value: object) -> str:
    """Normalize nullable runtime wire values to env-safe strings."""
    return "" if value is None else str(value)


@dataclass
class TeamSummary:
    """Display and routing metadata for one shared team membership."""

    team_id: int
    name: str
    description: str


def normalize_team_summaries(value: list[TeamSummary | dict]) -> list[TeamSummary]:
    """Return shared-team summaries in runtime dataclass form."""

    summaries: list[TeamSummary] = []
    for item in value:
        if isinstance(item, TeamSummary):
            summaries.append(
                _normalize_team_summary(
                    team_id=item.team_id,
                    name=item.name,
                    description=item.description,
                ),
            )
            continue
        if not isinstance(item, dict):
            raise ValueError("team_summaries entries must be objects")
        if not {"team_id", "name"} <= set(item):
            raise ValueError("team_summaries entries require team_id and name")
        summaries.append(
            _normalize_team_summary(
                team_id=item["team_id"],
                name=item["name"],
                description=item.get("description") or "",
            ),
        )
    return summaries


def _normalize_team_summary(
    *,
    team_id: object,
    name: object,
    description: object,
) -> TeamSummary:
    if not isinstance(team_id, int) or isinstance(team_id, bool):
        raise ValueError("team_summaries team_id values must be integers")
    if not isinstance(name, str) or not name:
        raise ValueError("team_summaries name values must be non-empty strings")
    if description is None:
        description = ""
    if not isinstance(description, str):
        raise ValueError("team_summaries description values must be strings")
    return TeamSummary(team_id=team_id, name=name, description=description)


def _encode_team_summaries(value: list[TeamSummary]) -> str:
    """Encode shared-team summaries for env vars that can only carry strings."""

    if not value:
        return ""
    return json.dumps(
        [
            {
                "team_id": summary.team_id,
                "name": summary.name,
                "description": summary.description,
            }
            for summary in value
        ],
    )


def _decode_team_summaries(value: str) -> list[TeamSummary]:
    """Decode shared-team summaries from their environment representation."""

    if not value:
        return []
    decoded = json.loads(value)
    if not isinstance(decoded, list):
        raise ValueError("TEAM_SUMMARIES must be a JSON list")
    return normalize_team_summaries(decoded)


@dataclass
class UserDesktopLink:
    """One user's own machine linked to this assistant.

    A shared assistant can be linked to a different desktop per user, so links
    are keyed by ``owner_user_id`` and resolved at runtime against whoever is
    currently interacting with the assistant.
    """

    owner_user_id: str
    url: str
    os: str  # "ubuntu", "windows", or "macos"
    filesys_sync: bool = False
    # Coordinates for on-demand SFTP access to the user's home over the raw-TCP
    # tunnel. Populated only when the device has registered its SFTP tunnel; the
    # private key never rides this struct (it is fetched on demand from the admin
    # assistant read, keyed by owner_user_id).
    sftp_tunnel_host: str | None = None
    sftp_tunnel_port: int | None = None

    @property
    def filesys_available(self) -> bool:
        """True when on-demand home filesystem access is usable for this link."""
        return bool(
            self.filesys_sync and self.sftp_tunnel_host and self.sftp_tunnel_port,
        )


def normalize_user_desktops(
    value: "list[UserDesktopLink | dict] | dict[str, UserDesktopLink | dict]",
) -> dict[str, UserDesktopLink]:
    """Return per-user desktop links keyed by ``owner_user_id``.

    Accepts the list-of-dicts wire shape (from the bootstrap payload), an
    already-keyed map, or runtime dataclasses, and normalises to a map.
    """

    items: list = list(value.values()) if isinstance(value, dict) else list(value)
    desktops: dict[str, UserDesktopLink] = {}
    for item in items:
        if isinstance(item, UserDesktopLink):
            link = item
        else:
            if not isinstance(item, dict):
                raise ValueError("user_desktops entries must be objects")
            if not {"owner_user_id", "url", "os"} <= set(item):
                raise ValueError(
                    "user_desktops entries require owner_user_id, url and os",
                )
            port = item.get("sftp_tunnel_port")
            host = item.get("sftp_tunnel_host")
            link = UserDesktopLink(
                owner_user_id=str(item["owner_user_id"]),
                url=str(item["url"]),
                os=str(item["os"]),
                filesys_sync=bool(item.get("filesys_sync", False)),
                sftp_tunnel_host=str(host) if host else None,
                sftp_tunnel_port=int(port) if port else None,
            )
        desktops[link.owner_user_id] = link
    return desktops


def _encode_user_desktops(value: dict[str, UserDesktopLink]) -> str:
    """Encode per-user desktop links for env vars that can only carry strings."""

    if not value:
        return ""
    return json.dumps(
        [
            {
                "owner_user_id": link.owner_user_id,
                "url": link.url,
                "os": link.os,
                "filesys_sync": link.filesys_sync,
                "sftp_tunnel_host": link.sftp_tunnel_host,
                "sftp_tunnel_port": link.sftp_tunnel_port,
            }
            for link in value.values()
        ],
    )


def _decode_user_desktops(value: str) -> dict[str, UserDesktopLink]:
    """Decode per-user desktop links from their environment representation."""

    if not value:
        return {}
    decoded = json.loads(value)
    if not isinstance(decoded, list):
        raise ValueError("ASSISTANT_USER_DESKTOPS must be a JSON list")
    return normalize_user_desktops(decoded)


@dataclass
class AssistantDetails:
    """Details about the assistant and its runtime routing identity."""

    agent_id: int | None = None
    binding_id: str = ""
    first_name: str = ""
    surname: str = ""
    age: str = ""
    nationality: str = ""
    timezone: str = ""  # IANA timezone identifier (e.g., "America/New_York")
    about: str = ""
    job_title: str = ""
    number: str = ""
    email: str = ""
    email_provider: str = "google_workspace"
    whatsapp_number: str = ""
    discord_bot_id: str = ""
    slack_bot_user_id: str = ""
    is_coordinator: bool = False
    contact_id: int = 0  # Contact ID in Contacts table
    self_contact_id: int = 0
    desktop_mode: str = "ubuntu"  # "ubuntu" or "windows" - determines VM type
    desktop_url: str | None = None  # URL for managed VM desktop access
    # Per-user desktops keyed by owner_user_id. A shared assistant can be linked
    # to a different machine for each user who works with it.
    user_desktops: dict[str, UserDesktopLink] = field(default_factory=dict)
    team_ids: list[int] = field(default_factory=list)
    team_summaries: list[TeamSummary] = field(default_factory=list)

    @property
    def name(self) -> str:
        return f"{self.first_name} {self.surname}".strip()

    @property
    def has_managed_desktop(self) -> bool:
        """True when a managed VM desktop is assigned and sync gates apply."""
        return self.desktop_mode in ("ubuntu", "windows") and bool(self.desktop_url)

    def user_desktop_for(self, user_id: str | None) -> UserDesktopLink | None:
        """Return the desktop the given user has linked to this assistant."""
        if not user_id:
            return None
        return self.user_desktops.get(user_id)


@dataclass
class UserDetails:
    """Details about the user and their assistant-scoped boss identity."""

    id: str = UNASSIGNED_USER_ID
    first_name: str = ""
    surname: str = ""
    number: str = ""
    email: str = ""
    whatsapp_number: str = ""
    boss_contact_id: int = 1

    @property
    def name(self) -> str:
        return f"{self.first_name} {self.surname}".strip()


@dataclass
class OrgDetails:
    """Details about the organization (when in org context).

    None/empty values indicate personal (non-org) context.
    """

    id: int | None = None  # Organization ID, None for personal context
    name: str = ""  # Organization name


@dataclass
class VoiceConfig:
    """Voice configuration for the session."""

    provider: str = DEFAULT_VOICE_PROVIDER
    id: str = ""
    mode: str = DEFAULT_VOICE_MODE


@dataclass
class VoiceCallConfig:
    """Runtime configuration for an active voice call.

    Populated by configure_from_cli() in medium_scripts/common.py
    when a call agent subprocess is launched.
    """

    outbound: bool = False
    channel: str = ""  # "phone" or "meet"
    contact_json: str = "{}"  # JSON-serialized contact dict
    boss_json: str = "{}"  # JSON-serialized boss dict


@dataclass
class SessionDetails:
    """Runtime details populated by ConversationManager on startup.

    Hierarchical structure with sub-containers for assistant, user, and voice.
    All fields have sensible defaults so callers never need `or "fallback"` patterns.

    API keys (`unify_key`, `shared_unify_key`) are lazy properties that fall back
    to environment variables if not explicitly set. This mirrors how the `unify`
    package handles UNIFY_KEY and provides automatic access without requiring
    explicit `populate_from_env()` calls.
    """

    assistant: AssistantDetails = field(default_factory=AssistantDetails)
    user: UserDetails = field(default_factory=UserDetails)
    voice: VoiceConfig = field(default_factory=VoiceConfig)
    voice_call: VoiceCallConfig = field(default_factory=VoiceCallConfig)
    _unify_key: str = field(default="", repr=False)
    _shared_unify_key: str = field(default="", repr=False)

    # Organization context (None id for personal/non-org context)
    org: OrgDetails = field(default_factory=OrgDetails)

    _initialized: bool = field(default=False, repr=False)

    @property
    def assistant_context(self) -> str:
        """The assistant's agent_id as the context path component.

        Used for Unify context paths like '{user_id}/{agent_id}/...'.
        """
        if self.assistant.agent_id is not None:
            return str(self.assistant.agent_id)
        return UNASSIGNED_ASSISTANT_CONTEXT

    @property
    def user_context(self) -> str:
        """The user's ID used as the context path component.

        Used for Unify context paths like '{user_id}/{assistant_id}/...'.
        """
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
    def org_id(self) -> int | None:
        """Shortcut to org.id for convenient access."""
        return self.org.id

    @org_id.setter
    def org_id(self, value: int | None) -> None:
        self.org.id = value

    @property
    def org_name(self) -> str:
        """Shortcut to org.name for convenient access."""
        return self.org.name

    @org_name.setter
    def org_name(self, value: str) -> None:
        self.org.name = value

    @property
    def team_ids(self) -> list[int]:
        """Shortcut to assistant.team_ids for convenient access."""
        return self.assistant.team_ids

    @team_ids.setter
    def team_ids(self, value: list[int]) -> None:
        self.assistant.team_ids = value

    @property
    def team_summaries(self) -> list[TeamSummary]:
        """Shortcut to assistant.team_summaries for convenient access."""
        return self.assistant.team_summaries

    @team_summaries.setter
    def team_summaries(self, value: list[TeamSummary | dict]) -> None:
        self.assistant.team_summaries = normalize_team_summaries(value)

    @property
    def is_coordinator(self) -> bool:
        """Shortcut to assistant.is_coordinator for role-gated runtime behavior."""
        return self.assistant.is_coordinator

    @is_coordinator.setter
    def is_coordinator(self, value: bool) -> None:
        self.assistant.is_coordinator = value

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

    @property
    def unify_key(self) -> str:
        """API key for Unify services.

        Falls back to UNIFY_KEY environment variable if not explicitly set.
        This mirrors how the `unify` package handles API key resolution.
        """
        if self._unify_key:
            return self._unify_key
        return os.environ.get("UNIFY_KEY", "")

    @unify_key.setter
    def unify_key(self, value: str) -> None:
        self._unify_key = value

    @property
    def shared_unify_key(self) -> str:
        """Shared API key for cross-assistant operations.

        Falls back to SHARED_UNIFY_KEY environment variable if not explicitly set.
        """
        if self._shared_unify_key:
            return self._shared_unify_key
        return os.environ.get("SHARED_UNIFY_KEY", "")

    @shared_unify_key.setter
    def shared_unify_key(self, value: str) -> None:
        self._shared_unify_key = value

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
        assistant_email_provider: str = "google_workspace",
        assistant_whatsapp_number: str = "",
        assistant_discord_bot_id: str = "",
        assistant_slack_bot_user_id: str = "",
        assistant_is_coordinator: bool = False,
        assistant_contact_id: int = 0,
        assistant_self_contact_id: int = DEFAULT_SELF_CONTACT_ID,
        user_id: str = "",
        user_first_name: str = "",
        user_surname: str = "",
        user_number: str = "",
        user_email: str = "",
        user_whatsapp_number: str = "",
        user_boss_contact_id: int = DEFAULT_BOSS_CONTACT_ID,
        org_id: int | None = None,
        org_name: str = "",
        team_ids: list[int] | None = None,
        team_summaries: list[TeamSummary | dict] | None = None,
        voice_provider: str = "",
        voice_id: str = "",
        binding_id: str = "",
        desktop_mode: str = "ubuntu",
        user_desktops: "list[UserDesktopLink | dict] | dict[str, UserDesktopLink | dict] | None" = None,
        is_coordinator: bool = False,
    ) -> None:
        """Populate the session with runtime values.

        Called by ConversationManager when a StartupEvent is received.
        """
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
        self.assistant.email_provider = _runtime_str(assistant_email_provider)
        self.assistant.whatsapp_number = _runtime_str(assistant_whatsapp_number)
        self.assistant.discord_bot_id = _runtime_str(assistant_discord_bot_id)
        self.assistant.slack_bot_user_id = _runtime_str(assistant_slack_bot_user_id)
        self.assistant.is_coordinator = assistant_is_coordinator
        self.assistant.contact_id = assistant_contact_id
        self.self_contact_id = assistant_self_contact_id
        self.assistant.binding_id = _runtime_str(binding_id)
        self.assistant.desktop_mode = _runtime_str(desktop_mode)
        self.assistant.user_desktops = (
            normalize_user_desktops(user_desktops) if user_desktops else {}
        )
        self.assistant.is_coordinator = is_coordinator
        self.user.id = _runtime_str(user_id)
        self.user.first_name = _runtime_str(user_first_name)
        self.user.surname = _runtime_str(user_surname)
        self.user.number = _runtime_str(user_number)
        self.user.email = _runtime_str(user_email)
        self.user.whatsapp_number = _runtime_str(user_whatsapp_number)
        self.boss_contact_id = user_boss_contact_id
        self.org.id = org_id
        self.org.name = _runtime_str(org_name)
        self.team_ids = team_ids or []
        self.team_summaries = team_summaries or []
        self.voice.provider = _runtime_str(voice_provider)
        self.voice.id = _runtime_str(voice_id)
        self._initialized = True

    def reset(self) -> None:
        """Reset to default state (useful for tests)."""
        self.assistant = AssistantDetails()
        self.user = UserDetails()
        self.org = OrgDetails()
        self.voice = VoiceConfig()
        self.voice_call = VoiceCallConfig()
        self._unify_key = ""
        self._shared_unify_key = ""
        self._initialized = False

    def export_to_env(self) -> None:
        """Export current values to environment variables.

        Called after populate() to ensure subprocesses can inherit values.
        """
        os.environ["ASSISTANT_ID"] = (
            str(self.assistant.agent_id) if self.assistant.agent_id is not None else ""
        )
        os.environ["ASSISTANT_FIRST_NAME"] = _runtime_str(self.assistant.first_name)
        os.environ["ASSISTANT_SURNAME"] = _runtime_str(self.assistant.surname)
        os.environ["ASSISTANT_NAME"] = _runtime_str(self.assistant.name)
        os.environ["ASSISTANT_AGE"] = _runtime_str(self.assistant.age)
        os.environ["ASSISTANT_NATIONALITY"] = _runtime_str(
            self.assistant.nationality,
        )
        os.environ["ASSISTANT_TIMEZONE"] = _runtime_str(self.assistant.timezone)
        os.environ["ASSISTANT_ABOUT"] = _runtime_str(self.assistant.about)
        os.environ["ASSISTANT_JOB_TITLE"] = _runtime_str(self.assistant.job_title)
        os.environ["ASSISTANT_NUMBER"] = _runtime_str(self.assistant.number)
        os.environ["ASSISTANT_EMAIL"] = _runtime_str(self.assistant.email)
        os.environ["ASSISTANT_EMAIL_PROVIDER"] = _runtime_str(
            self.assistant.email_provider,
        )
        os.environ["ASSISTANT_WHATSAPP_NUMBER"] = _runtime_str(
            self.assistant.whatsapp_number,
        )
        os.environ["ASSISTANT_DISCORD_BOT_ID"] = _runtime_str(
            self.assistant.discord_bot_id,
        )
        os.environ["ASSISTANT_SLACK_BOT_USER_ID"] = _runtime_str(
            self.assistant.slack_bot_user_id,
        )
        os.environ["ASSISTANT_DESKTOP_MODE"] = _runtime_str(
            self.assistant.desktop_mode,
        )
        os.environ["ASSISTANT_DESKTOP_URL"] = _runtime_str(
            self.assistant.desktop_url,
        )
        os.environ["ASSISTANT_USER_DESKTOPS"] = _encode_user_desktops(
            self.assistant.user_desktops,
        )
        os.environ["ASSISTANT_IS_COORDINATOR"] = str(self.assistant.is_coordinator)
        self.export_contact_ids_to_env()
        os.environ["USER_ID"] = _runtime_str(self.user.id)
        os.environ["USER_FIRST_NAME"] = _runtime_str(self.user.first_name)
        os.environ["USER_SURNAME"] = _runtime_str(self.user.surname)
        os.environ["USER_NAME"] = _runtime_str(self.user.name)
        os.environ["USER_NUMBER"] = _runtime_str(self.user.number)
        os.environ["USER_EMAIL"] = _runtime_str(self.user.email)
        os.environ["USER_WHATSAPP_NUMBER"] = _runtime_str(self.user.whatsapp_number)
        os.environ["ORG_ID"] = str(self.org.id) if self.org.id is not None else ""
        os.environ["ORG_NAME"] = _runtime_str(self.org.name)
        self.export_team_ids_to_env()
        self.export_team_summaries_to_env()
        os.environ["VOICE_PROVIDER"] = _runtime_str(self.voice.provider)
        os.environ["VOICE_ID"] = _runtime_str(self.voice.id)
        os.environ["VOICE_MODE"] = _runtime_str(self.voice.mode)
        # Voice call config (for call agent subprocesses)
        os.environ["OUTBOUND"] = str(self.voice_call.outbound)
        os.environ["CHANNEL"] = _runtime_str(self.voice_call.channel)
        os.environ["CONTACT"] = _runtime_str(self.voice_call.contact_json)
        os.environ["BOSS"] = _runtime_str(self.voice_call.boss_json)
        os.environ["UNIFY_KEY"] = self.unify_key

    def export_team_ids_to_env(self) -> None:
        """Export current shared-team memberships to the subprocess env shape."""
        os.environ["TEAM_IDS"] = _encode_int_csv(self.assistant.team_ids)

    def export_team_summaries_to_env(self) -> None:
        """Export current shared-team summaries to the subprocess env shape."""
        os.environ["TEAM_SUMMARIES"] = _encode_team_summaries(
            self.assistant.team_summaries,
        )

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
        if val := os.environ.get("ASSISTANT_FIRST_NAME"):
            self.assistant.first_name = val
        if val := os.environ.get("ASSISTANT_SURNAME"):
            self.assistant.surname = val
        if val := os.environ.get("ASSISTANT_AGE"):
            self.assistant.age = val
        if val := os.environ.get("ASSISTANT_NATIONALITY"):
            self.assistant.nationality = val
        if val := os.environ.get("ASSISTANT_TIMEZONE"):
            self.assistant.timezone = val
        if val := os.environ.get("ASSISTANT_ABOUT"):
            self.assistant.about = val
        if val := os.environ.get("ASSISTANT_JOB_TITLE"):
            self.assistant.job_title = val
        if val := os.environ.get("ASSISTANT_NUMBER"):
            self.assistant.number = val
        if val := os.environ.get("ASSISTANT_EMAIL"):
            self.assistant.email = val
        if val := os.environ.get("ASSISTANT_EMAIL_PROVIDER"):
            self.assistant.email_provider = val
        if val := os.environ.get("ASSISTANT_WHATSAPP_NUMBER"):
            self.assistant.whatsapp_number = val
        if val := os.environ.get("ASSISTANT_DISCORD_BOT_ID"):
            self.assistant.discord_bot_id = val
        if val := os.environ.get("ASSISTANT_SLACK_BOT_USER_ID"):
            self.assistant.slack_bot_user_id = val
        if val := os.environ.get("ASSISTANT_CONTACT_ID"):
            try:
                self.assistant.contact_id = int(val)
            except ValueError:
                pass
        if val := os.environ.get("SELF_CONTACT_ID"):
            try:
                self.self_contact_id = int(val)
            except ValueError:
                pass
        if val := os.environ.get("ASSISTANT_DESKTOP_MODE"):
            self.assistant.desktop_mode = val
        if val := os.environ.get("ASSISTANT_DESKTOP_URL"):
            self.assistant.desktop_url = val if val else None
        if val := os.environ.get("ASSISTANT_USER_DESKTOPS"):
            self.assistant.user_desktops = _decode_user_desktops(val)
        if val := os.environ.get("ASSISTANT_IS_COORDINATOR"):
            self.assistant.is_coordinator = val == "True"
        if val := os.environ.get("USER_ID"):
            self.user.id = val
        if val := os.environ.get("USER_FIRST_NAME"):
            self.user.first_name = val
        if val := os.environ.get("USER_SURNAME"):
            self.user.surname = val
        if val := os.environ.get("USER_NUMBER"):
            self.user.number = val
        if val := os.environ.get("USER_EMAIL"):
            self.user.email = val
        if val := os.environ.get("USER_WHATSAPP_NUMBER"):
            self.user.whatsapp_number = val
        if val := os.environ.get("BOSS_CONTACT_ID"):
            try:
                self.boss_contact_id = int(val)
            except ValueError:
                pass
        if val := os.environ.get("ORG_ID"):
            try:
                self.org.id = int(val)
            except ValueError:
                pass
        if val := os.environ.get("ORG_NAME"):
            self.org.name = val
        if val := os.environ.get("TEAM_IDS"):
            try:
                self.assistant.team_ids = _decode_int_csv(val)
            except (ValueError, TypeError):
                pass
        if val := os.environ.get("TEAM_SUMMARIES"):
            try:
                self.team_summaries = _decode_team_summaries(val)
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                pass
        if val := os.environ.get("VOICE_PROVIDER"):
            self.voice.provider = val
        if val := os.environ.get("VOICE_ID"):
            self.voice.id = val
        if val := os.environ.get("VOICE_MODE"):
            self.voice.mode = val
        # Voice call config (for call agent subprocesses)
        if val := os.environ.get("OUTBOUND"):
            self.voice_call.outbound = val == "True"
        if val := os.environ.get("CHANNEL"):
            self.voice_call.channel = val
        if val := os.environ.get("CONTACT"):
            self.voice_call.contact_json = val
        if val := os.environ.get("BOSS"):
            self.voice_call.boss_json = val
        # General config
        if val := os.environ.get("SHARED_UNIFY_KEY"):
            self.shared_unify_key = val
        if val := os.environ.get("UNIFY_KEY"):
            self.unify_key = val
        self._initialized = True

    def get_subprocess_env(self, **overrides: str) -> dict[str, str]:
        """Get a copy of the current environment for subprocess use.

        This is the only approved way to access os.environ for subprocess
        communication. First exports SESSION_DETAILS values, then returns
        a copy with any overrides applied.

        Args:
            **overrides: Key-value pairs to add/override in the env dict.

        Returns:
            A dict suitable for passing to subprocess.run(env=...).
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
        allow test_conversation_manager/conftest.py to set UNITY_*_IMPL=simulated
        and have it take effect.

        This is intentionally a static method since it doesn't depend on session
        state and is only used during manager initialization.

        Args:
            name: The env var name (e.g., "UNITY_CONTACT_IMPL").
            default: Default value if env var is not set.

        Returns:
            The implementation type ("real" or "simulated").
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
