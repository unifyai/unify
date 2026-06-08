"""Coordinator workspace primitives exposed under ``primitives.coordinator``.

The coordinator slow-brain delegates setup and workspace mutation work to the
actor loop. The actor accesses these methods through the scoped primitives
runtime, which guarantees consistent reachability checks and activity tracking.
"""

from __future__ import annotations

import functools
from collections.abc import Callable, Sequence
from typing import Any, Literal, TypedDict

import unify
from unify.utils.http import RequestError

from unity.common.colleague_cache import (
    assistant_display_name as resolve_assistant_display_name,
    display_name as resolve_display_name,
)
from unity.common.tool_outcome import ToolError
from unity.coordinator_manager.activity import (
    activity_entity,
    coordinator_activity_id,
    publish_coordinator_activity,
    safe_activity_text,
)
from unity.events.types.coordinator_activity import (
    CoordinatorActivityEntity,
    CoordinatorActivityPhase,
    CoordinatorActivityStage,
    CoordinatorActivitySurface,
)
from unity.manager_registry import SingletonABCMeta
from unity.session_details import SESSION_DETAILS


def _coordinator_role_required_error() -> ToolError:
    return {
        "error_kind": "permission_denied",
        "message": "Coordinator workspace primitives are only available in Coordinator sessions.",
        "details": {"is_coordinator": bool(SESSION_DETAILS.is_coordinator)},
    }


def _coordinator_primitive(
    implementation: Callable[..., Any],
) -> Callable[..., Any]:
    """Expose a workspace session method through the coordinator role gate."""

    @functools.wraps(implementation)
    def gateway(
        _self: "CoordinatorWorkspaceManager",
        /,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        permission_error = CoordinatorWorkspaceManager._require_coordinator_role()
        if permission_error is not None:
            return permission_error
        return implementation(_CoordinatorWorkspaceSession(), *args, **kwargs)

    return gateway


InviteOrgRoleName = Literal["Admin", "Member", "Viewer"]
_INVITE_ORG_ROLE_NAMES: tuple[InviteOrgRoleName, ...] = ("Admin", "Member", "Viewer")
_INVITE_ORG_ROLE_BY_NORMALIZED_NAME: dict[str, InviteOrgRoleName] = {
    role_name.lower(): role_name for role_name in _INVITE_ORG_ROLE_NAMES
}
CoordinatorDelegateIntent = Literal[
    "general",
    "schedule_task",
    "add_guidance",
    "add_knowledge",
    "create_function",
    "create_dashboard",
    "data_setup",
]
_DELEGATE_INTENT_SURFACES: dict[str, CoordinatorActivitySurface] = {
    "general": "colleagues",
    "schedule_task": "tasks",
    "add_guidance": "guidance",
    "add_knowledge": "memory",
    "create_function": "functions",
    "create_dashboard": "dashboards",
    "data_setup": "data",
}

AssistantDesktopMode = Literal["ubuntu", "windows", "macos"]


class AssistantConfigPatch(TypedDict, total=False):
    """Editable assistant profile/config fields accepted by update calls."""

    first_name: str
    surname: str
    about: str | None
    job_title: str | None
    age: int
    nationality: str | None
    weekly_limit: float | None
    max_parallel: int | None
    profile_photo: str | None
    profile_video: str | None
    desktop_mode: AssistantDesktopMode
    user_desktop_id: int | None
    user_desktop_filesys_sync: bool | None
    voice_id: str | None
    voice_provider: str | None
    timezone: str | None
    is_local: bool | None
    monthly_spending_cap: float | None


class AssistantCreateConfig(AssistantConfigPatch, total=False):
    """Assistant creation config fields forwarded to the SDK create call."""

    create_infra: bool
    is_local: bool


class TeamPatch(TypedDict, total=False):
    """Editable workspace metadata fields."""

    name: str
    description: str


COORDINATOR_TOOL_METHOD_NAMES: tuple[str, ...] = (
    "create_assistant",
    "delete_assistant",
    "update_assistant_config",
    "list_assistants",
    "list_org_members",
    "invite_org_member",
    "delegate_to_colleague",
    "create_team",
    "delete_team",
    "update_team",
    "add_team_member",
    "remove_team_member",
    "list_teams",
    "list_team_members",
    "list_teams_for_assistant",
    "commission_colleague_into_team",
)


class _CoordinatorWorkspaceSession:
    """Short-lived coordinator workspace session with isolated reachability caches."""

    def __init__(self) -> None:
        self._assistant_cache: list[dict[str, Any]] | None = None
        self._known_assistant_ids: set[str] = set()
        self._team_cache: list[dict[str, Any]] | None = None
        self._known_team_ids: set[str] = set()
        self._activity_metadata: dict[
            str,
            tuple[
                list[CoordinatorActivitySurface],
                list[CoordinatorActivityEntity | dict[str, Any]],
            ],
        ] = {}

    @staticmethod
    def _derived_colleague_job_title(
        *,
        first_name: str,
        surname: str | None,
    ) -> str | None:
        """Return a role-style job title inferred from commissioning inputs."""
        candidate = (surname or "").strip()
        if not candidate:
            return None
        if " " in first_name.strip():
            return candidate
        return None

    @staticmethod
    def _normalize_about(about: str | None) -> str | None:
        """Return trimmed bio text, or None when no usable content was provided."""
        if about is None:
            return None
        trimmed = about.strip()
        if not trimmed:
            return None
        return trimmed

    @staticmethod
    def _normalize_optional_text(value: str | None) -> str | None:
        """Return trimmed text, or None when unset/whitespace."""
        if value is None:
            return None
        trimmed = value.strip()
        if not trimmed:
            return None
        return trimmed

    def _assistant_defaults_from_coordinator(
        self,
        *,
        first_name: str,
        surname: str | None,
        about: str | None = None,
        job_title: str | None = None,
        timezone: str | None = None,
        nationality: str | None = None,
        config: AssistantCreateConfig | None,
    ) -> dict[str, Any] | None:
        """Build coordinator-inherited defaults for colleague creation."""
        merged = dict(config or {})
        normalized_timezone = self._normalize_optional_text(timezone)
        if normalized_timezone is not None:
            merged["timezone"] = normalized_timezone
        normalized_nationality = self._normalize_optional_text(nationality)
        if normalized_nationality is not None:
            merged["nationality"] = normalized_nationality
        normalized_job_title = self._normalize_optional_text(job_title)
        if normalized_job_title is not None:
            merged["job_title"] = normalized_job_title
        coordinator_timezone = (SESSION_DETAILS.assistant.timezone or "").strip()
        coordinator_nationality = (SESSION_DETAILS.assistant.nationality or "").strip()
        if not merged.get("timezone") and coordinator_timezone:
            merged["timezone"] = coordinator_timezone
        if not merged.get("nationality") and coordinator_nationality:
            merged["nationality"] = coordinator_nationality
        if not merged.get("job_title"):
            derived_job_title = self._derived_colleague_job_title(
                first_name=first_name,
                surname=surname,
            )
            if derived_job_title:
                merged["job_title"] = derived_job_title
        if about is not None:
            merged["about"] = about
        return merged or None

    @staticmethod
    def _assistant_config_with_organization(
        config: dict[str, Any] | None,
        *,
        organization_id: int | None,
    ) -> dict[str, Any] | None:
        """Return assistant config with an optional explicit organization target."""
        if organization_id is None:
            return config
        merged = dict(config or {})
        merged["organization_id"] = organization_id
        return merged

    def create_assistant(
        self,
        *,
        first_name: str,
        surname: str | None = None,
        about: str,
        job_title: str | None = None,
        timezone: str | None = None,
        nationality: str | None = None,
        config: AssistantCreateConfig | None = None,
    ) -> dict[str, Any] | ToolError:
        """Create a colleague assistant in the active coordinator workspace.

        Use this when the user has confirmed the colleague profile and asked the
        Coordinator to actually provision the assistant now. The tool merges the
        provided profile fields with coordinator-derived defaults (timezone,
        nationality, and inferred role metadata), then creates the assistant in
        the current organization scope.

        Parameters
        ----------
        first_name : str
            Colleague first name to provision.
        surname : str | None, optional
            Optional colleague surname.
        about : str
            Required profile summary describing the colleague role.
        job_title : str | None, optional
            Optional explicit role title for the colleague profile.
        timezone : str | None, optional
            Optional IANA timezone override for this colleague.
        nationality : str | None, optional
            Optional nationality metadata for profile defaults.
        config : AssistantCreateConfig | None, optional
            Structured assistant-create overrides merged with coordinator
            defaults (for example ``create_infra`` and ``is_local``).
        """
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=False,
            operation_name="create_assistant",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id

        normalized_about = self._normalize_about(about)
        if normalized_about is None:
            return _invalid_argument(
                message=(
                    "Provide a non-empty `about` value before creating a colleague."
                ),
                details={"field": "about"},
            )
        assistant_config = self._assistant_defaults_from_coordinator(
            first_name=first_name,
            surname=surname,
            about=normalized_about,
            job_title=job_title,
            timezone=timezone,
            nationality=nationality,
            config=config,
        )
        assistant_config = self._assistant_config_with_organization(
            assistant_config,
            organization_id=resolved_organization_id,
        )
        colleague_name = _display_name(first_name=first_name, surname=surname)
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title=f"Creating {colleague_name} colleague",
            surfaces=["colleagues"],
            related_entities=[
                activity_entity("colleague", name=colleague_name),
            ],
        )
        try:
            result = unify.create_assistant(
                first_name=first_name,
                surname=surname,
                config=assistant_config,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title=f"Could not create {colleague_name} colleague",
                error=error,
            )
            return error
        self._remember_assistant(result)
        self._clear_assistant_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title=f"Created {safe_activity_text(_assistant_display_name(result), fallback=colleague_name)} colleague",
            surfaces=["colleagues"],
            related_entities=[_assistant_entity(result, fallback=colleague_name)],
            activity_id=activity_id,
        )
        return result

    def delete_assistant(
        self,
        *,
        agent_id: int,
    ) -> dict[str, Any] | str | ToolError:
        """Delete a reachable colleague assistant from the coordinator workspace.

        Use this only for explicitly confirmed destructive actions. The tool
        first verifies that the target assistant is reachable from the current
        coordinator scope, then performs deletion and returns structured errors
        when the target is missing or inaccessible.

        Parameters
        ----------
        agent_id : int
            Assistant identifier to delete after explicit confirmation.
        """

        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Removing colleague",
            surfaces=["colleagues"],
            related_entities=[
                activity_entity("colleague", name="Colleague", entity_id=agent_id),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=False,
            operation_name="delete_assistant",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not remove colleague",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable = self._assistant_is_reachable(
            agent_id,
        )
        if isinstance(reachable, dict):
            self._publish_failure(
                activity_id,
                title="Could not remove colleague",
                error=reachable,
            )
            return reachable
        if not reachable:
            error = _assistant_not_found(agent_id)
            self._publish_failure(
                activity_id,
                title="Could not remove colleague",
                error=error,
            )
            return error
        try:
            result = unify.delete_assistant(
                agent_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not remove colleague",
                error=error,
            )
            return error
        self._known_assistant_ids.discard(str(agent_id))
        self._clear_assistant_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Removed colleague",
            surfaces=["colleagues"],
            related_entities=[
                activity_entity("colleague", name="Colleague", entity_id=agent_id),
            ],
            activity_id=activity_id,
        )
        return result

    def update_assistant_config(
        self,
        *,
        agent_id: int,
        config: AssistantConfigPatch,
    ) -> dict[str, Any] | ToolError:
        """Update config fields for a reachable colleague assistant.

        Use this after the user has confirmed concrete profile or behavior
        edits (for example bio, role, timezone, or other config-backed fields).
        This mutates an existing assistant only; use ``create_assistant`` when
        the colleague does not yet exist.

        Parameters
        ----------
        agent_id : int
            Target colleague assistant identifier.
        config : AssistantConfigPatch
            Structured profile patch payload. Preferred keys include
            ``first_name``, ``surname``, ``about``, ``job_title``,
            ``timezone``, ``nationality``, ``weekly_limit``,
            ``max_parallel``, ``desktop_mode``, and voice fields
            (``voice_id`` with ``voice_provider``).
        """

        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Updating colleague profile",
            surfaces=["colleagues"],
            related_entities=[
                activity_entity("colleague", name="Colleague", entity_id=agent_id),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=False,
            operation_name="update_assistant_config",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not update colleague profile",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable = self._assistant_is_reachable(
            agent_id,
        )
        if isinstance(reachable, dict):
            self._publish_failure(
                activity_id,
                title="Could not update colleague profile",
                error=reachable,
            )
            return reachable
        if not reachable:
            error = _assistant_not_found(agent_id)
            self._publish_failure(
                activity_id,
                title="Could not update colleague profile",
                error=error,
            )
            return error
        try:
            result = unify.update_assistant_config(
                agent_id,
                dict(config),
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not update colleague profile",
                error=error,
            )
            return error
        self._remember_assistant(result)
        self._clear_assistant_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Updated colleague profile",
            surfaces=["colleagues"],
            related_entities=[_assistant_entity(result, fallback="Colleague")],
            activity_id=activity_id,
        )
        return result

    def list_assistants(
        self,
        *,
        phone: str | None = None,
        email: str | None = None,
        agent_id: int | None = None,
    ) -> list[dict[str, Any]] | ToolError:
        """List assistants visible inside the coordinator's reachable scope.

        Use this before assistant/workspace mutations to resolve ids, confirm
        whether a colleague already exists, and disambiguate by phone, email, or
        ``agent_id``. Unfiltered calls also refresh the local reachability cache
        used by other coordinator tools in the same turn.

        Parameters
        ----------
        phone : str | None, optional
            Optional phone filter for narrow assistant lookup.
        email : str | None, optional
            Optional email filter for narrow assistant lookup.
        agent_id : int | None, optional
            Optional assistant id filter for exact match lookup.
        """

        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=False,
            operation_name="list_assistants",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id

        list_all_org = resolved_organization_id is not None
        try:
            assistants = unify.list_assistants(
                phone=phone,
                email=email,
                agent_id=agent_id,
                list_all_org=list_all_org,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)
        if resolved_organization_id is not None:
            assistants = [
                row
                for row in assistants
                if self._assistant_belongs_to_organization(
                    row,
                    organization_id=resolved_organization_id,
                )
            ]
        if phone is None and email is None and agent_id is None:
            self._assistant_cache = assistants
            self._remember_assistants(assistants)
        return assistants

    def list_org_members(self) -> list[dict[str, Any]] | ToolError:
        """List authorized human organization members for membership targeting.

        Use this when membership actions target ``member_user_id`` (human org
        users) rather than assistant ids. This helps validate that a referenced
        org member is authorized and reachable before attempting workspace
        membership mutations.
        """
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="list_org_members",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id
        try:
            return unify.list_org_members(
                resolved_organization_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)

    def invite_org_member(
        self,
        *,
        email: str,
        role_name: InviteOrgRoleName | None = None,
    ) -> dict[str, Any] | ToolError:
        """Invite a human member into the active organization by email.

        Use this when onboarding or team setup requires inviting someone who is
        not yet a member of the organization.

        Parameters
        ----------
        email : str
            Email address to invite into the organization.
        role_name : InviteOrgRoleName | None, optional
            Optional role to assign on acceptance. Accepted values are
            ``"Admin"``, ``"Member"``, and ``"Viewer"``.
        """
        normalized_email = email.strip().lower()
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Inviting organization member",
            surfaces=["invitation"],
            related_entities=[
                activity_entity(
                    "human",
                    name=normalized_email or "Invitee",
                    entity_id=normalized_email or None,
                ),
            ],
        )
        if not normalized_email:
            error = _invalid_argument(
                message="Provide a non-empty email address.",
                details={"field": "email"},
            )
            self._publish_failure(
                activity_id,
                title="Could not invite organization member",
                error=error,
            )
            return error
        normalized_role_name = self._normalize_optional_text(role_name)
        resolved_role_name: InviteOrgRoleName | None = None
        if normalized_role_name is not None:
            resolved_role_name = _INVITE_ORG_ROLE_BY_NORMALIZED_NAME.get(
                normalized_role_name.lower(),
            )
            if resolved_role_name is None:
                error = _invalid_argument(
                    message="`role_name` must be one of: Admin, Member, Viewer.",
                    details={
                        "role_name": normalized_role_name,
                        "accepted_roles": list(_INVITE_ORG_ROLE_NAMES),
                    },
                )
                self._publish_failure(
                    activity_id,
                    title="Could not invite organization member",
                    error=error,
                )
                return error
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="invite_org_member",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not invite organization member",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        try:
            result = unify.invite_org_member(
                resolved_organization_id,
                normalized_email,
                role_name=resolved_role_name,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not invite organization member",
                error=error,
            )
            return error
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Invited organization member",
            surfaces=["invitation"],
            related_entities=[
                activity_entity(
                    "human",
                    name=normalized_email,
                    entity_id=normalized_email,
                ),
            ],
            activity_id=activity_id,
        )
        return result

    def delegate_to_colleague(
        self,
        *,
        target_assistant_id: int,
        instruction: str,
        intent: CoordinatorDelegateIntent = "general",
        dedupe_key: str | None = None,
        related_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | ToolError:
        """Assign asynchronous work to a colleague after user confirmation.
        Use this when a specific colleague should own or execute follow-up work,
        such as creating a task, adding guidance, recording knowledge, preparing a
        function, sending a message, or other durable follow-up. Current-assistant
        manager primitives operate through the current assistant's available manager
        scope, including supported shared team scope where applicable; they do not
        become target-assistant-private operations just because the instruction names
        another assistant. This dispatches the assignment to the colleague's runtime
        so the colleague can perform the work with its own primitives. A successful
        return is an async delegation receipt, not proof that the colleague has
        already processed the wake reason, created durable artifacts, or completed
        the assignment. The response includes ``accepted``, ``completion_status``,
        ``receipt_type``, and ``message`` fields that explain this contract. Treat
        ``status``, ``activation_id``, and related dispatch fields as evidence that
        the assignment was accepted for async processing. After it returns, tell the
        user that the work was assigned to the colleague, not that the colleague
        completed it.

        Parameters
        ----------
        target_assistant_id : int
            The colleague assistant that should handle the work.
        instruction : str
            Plain-English assignment for the colleague.
        intent : CoordinatorDelegateIntent, optional
            Optional category that helps the colleague choose the right manager.
        dedupe_key : str | None, optional
            Optional retry key for avoiding obvious duplicate work.
        related_context : dict[str, Any] | None, optional
            Optional structured context to include in the assignment.
        """
        normalized_instruction = self._normalize_optional_text(instruction)
        if normalized_instruction is None:
            return _invalid_argument(
                message="Delegate instructions must be non-empty.",
                details={"field": "instruction"},
            )
        surfaces = [_surface_for_delegate_intent(intent)]
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Assigning work to colleague",
            surfaces=surfaces,
            related_entities=[
                activity_entity(
                    "colleague",
                    name="Colleague",
                    entity_id=target_assistant_id,
                ),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=False,
            operation_name="delegate_to_colleague",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not assign work to colleague",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable = self._assistant_is_reachable(
            target_assistant_id,
        )
        if isinstance(reachable, dict):
            self._publish_failure(
                activity_id,
                title="Could not assign work to colleague",
                error=reachable,
            )
            return reachable
        if not reachable:
            error = _assistant_not_found(target_assistant_id)
            self._publish_failure(
                activity_id,
                title="Could not assign work to colleague",
                error=error,
            )
            return error
        try:
            result = unify.delegate_to_colleague(
                target_assistant_id,
                instruction=normalized_instruction,
                intent=intent,
                dedupe_key=dedupe_key,
                related_context=related_context,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not assign work to colleague",
                error=error,
            )
            return error
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Assigned work to colleague",
            surfaces=surfaces,
            related_entities=[
                activity_entity(
                    "colleague",
                    name="Colleague",
                    entity_id=target_assistant_id,
                ),
            ],
            activity_id=activity_id,
        )
        return result

    def create_team(
        self,
        *,
        name: str,
        description: str,
        owner_user_id: str | None = None,
    ) -> dict[str, Any] | ToolError:
        """Create a shared workspace in the coordinator organization.

        Use this when the user has agreed on a concrete workspace name/purpose
        and wants the team created now. This tool creates the workspace object;
        membership is a separate step via ``add_team_member`` unless a single
        composite provisioning step is better.

        Parameters
        ----------
        name : str
            Workspace name to create.
        description : str
            Human-readable summary of workspace intent.
        owner_user_id : str | None, optional
            Reserved field for API parity; ownership derives from coordinator scope.
        """
        del owner_user_id
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="create_team",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id
        team_name_label = safe_activity_text(name, fallback="Team")
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title=f"Creating {team_name_label} team",
            surfaces=["teams"],
            related_entities=[
                activity_entity("team", name=team_name_label),
            ],
        )
        try:
            result = unify.create_team(
                resolved_organization_id,
                name=name,
                description=description,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title=f"Could not create {team_name_label} team",
                error=error,
            )
            return error
        self._remember_team(result)
        self._clear_team_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title=f"Created {safe_activity_text(_team_display_name(result), fallback=team_name_label)} team",
            surfaces=["teams"],
            related_entities=[_team_entity(result, fallback=team_name_label)],
            activity_id=activity_id,
        )
        return result

    def delete_team(
        self,
        *,
        team_id: int,
    ) -> dict[str, Any] | ToolError:
        """Delete a reachable shared workspace after explicit confirmation.

        Use only after explicit destructive confirmation for the exact target
        workspace. The tool validates reachability first and returns structured
        errors when the workspace is missing or outside coordinator scope.

        Parameters
        ----------
        team_id : int
            Workspace identifier to delete.
        """

        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Removing team",
            surfaces=["teams"],
            related_entities=[
                activity_entity("team", name="Team", entity_id=team_id),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="delete_team",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not remove team",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable = self._team_is_reachable(
            team_id,
        )
        if isinstance(reachable, dict):
            self._publish_failure(
                activity_id,
                title="Could not remove team",
                error=reachable,
            )
            return reachable
        if not reachable:
            error = _team_not_found(team_id)
            self._publish_failure(
                activity_id,
                title="Could not remove team",
                error=error,
            )
            return error
        try:
            result = unify.delete_team(
                resolved_organization_id,
                team_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not remove team",
                error=error,
            )
            return error
        self._known_team_ids.discard(str(team_id))
        self._clear_team_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Removed team",
            surfaces=["teams"],
            related_entities=[
                activity_entity("team", name="Team", entity_id=team_id),
            ],
            activity_id=activity_id,
        )
        return result

    def update_team(
        self,
        *,
        team_id: int,
        patch: TeamPatch,
    ) -> dict[str, Any] | ToolError:
        """Apply metadata updates to a reachable shared workspace.

        Use this for confirmed workspace field edits (for example name or
        description changes). This tool mutates workspace properties only; use
        membership tools for adding/removing colleagues from the workspace.

        Parameters
        ----------
        team_id : int
            Workspace identifier to update.
        patch : TeamPatch
            Partial update payload for editable workspace fields.
            Accepted keys are ``name`` and ``description``.
        """

        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Updating team",
            surfaces=["teams"],
            related_entities=[
                activity_entity("team", name="Team", entity_id=team_id),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="update_team",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not update team",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable = self._team_is_reachable(
            team_id,
        )
        if isinstance(reachable, dict):
            self._publish_failure(
                activity_id,
                title="Could not update team",
                error=reachable,
            )
            return reachable
        if not reachable:
            error = _team_not_found(team_id)
            self._publish_failure(
                activity_id,
                title="Could not update team",
                error=error,
            )
            return error
        try:
            result = unify.update_team(
                resolved_organization_id,
                team_id,
                patch,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not update team",
                error=error,
            )
            return error
        self._remember_team(result)
        self._clear_team_cache()
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Updated team",
            surfaces=["teams"],
            related_entities=[_team_entity(result, fallback="Workspace")],
            activity_id=activity_id,
        )
        return result

    def add_team_member(
        self,
        *,
        team_id: int,
        assistant_id: int | None = None,
        member_user_id: str | None = None,
    ) -> dict[str, Any] | ToolError:
        """Add one assistant or one organization member to a workspace.

        Use this when the user has confirmed a workspace membership change.
        Provide exactly one selector:
        - ``assistant_id`` to add an assistant colleague
        - ``member_user_id`` to add an authorized human org member

        Parameters
        ----------
        team_id : int
            Workspace receiving the membership grant.
        assistant_id : int | None, optional
            Assistant id to add as a member.
        member_user_id : str | None, optional
            Organization user id to add as a human member.
        """
        has_assistant_id = assistant_id is not None
        normalized_member_user_id = (
            member_user_id.strip() if isinstance(member_user_id, str) else None
        )
        has_member_user_id = bool(normalized_member_user_id)
        if has_assistant_id == has_member_user_id:
            return _invalid_argument(
                message="Provide exactly one of `assistant_id` or `member_user_id`.",
                details={
                    "assistant_id": assistant_id,
                    "member_user_id": member_user_id,
                },
            )
        resolved_target_assistant_id = assistant_id
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Adding colleague to team",
            surfaces=["membership"],
            related_entities=_membership_entities(
                team_id,
                assistant_id=resolved_target_assistant_id,
                member_user_id=normalized_member_user_id,
            ),
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="add_team_member",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not add colleague to team",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        reachable_team = self._team_is_reachable(
            team_id,
        )
        if isinstance(reachable_team, dict):
            self._publish_failure(
                activity_id,
                title="Could not add colleague to team",
                error=reachable_team,
            )
            return reachable_team
        if not reachable_team:
            error = _team_not_found(team_id)
            self._publish_failure(
                activity_id,
                title="Could not add colleague to team",
                error=error,
            )
            return error
        if assistant_id is not None:
            reachable_assistant = self._assistant_is_reachable(
                assistant_id,
            )
            if isinstance(reachable_assistant, dict):
                self._publish_failure(
                    activity_id,
                    title="Could not add colleague to team",
                    error=reachable_assistant,
                )
                return reachable_assistant
            if not reachable_assistant:
                error = _assistant_not_found(assistant_id)
                self._publish_failure(
                    activity_id,
                    title="Could not add colleague to team",
                    error=error,
                )
                return error
        else:
            member_user_id_for_lookup = normalized_member_user_id or ""
            member_reachable = self._org_member_is_reachable(
                member_user_id_for_lookup,
            )
            if isinstance(member_reachable, dict):
                self._publish_failure(
                    activity_id,
                    title="Could not add colleague to team",
                    error=member_reachable,
                )
                return member_reachable
            if not member_reachable:
                error = _org_member_not_found(member_user_id_for_lookup)
                self._publish_failure(
                    activity_id,
                    title="Could not add colleague to team",
                    error=error,
                )
                return error

        try:
            result = unify.add_team_member(
                resolved_organization_id,
                team_id,
                assistant_id=assistant_id,
                member_user_id=normalized_member_user_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not add colleague to team",
                error=error,
            )
            return error
        resolved_target_assistant_id = _extract_assistant_id(result)
        if resolved_target_assistant_id is not None:
            self._known_assistant_ids.add(str(resolved_target_assistant_id))
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Added colleague to team",
            surfaces=["membership"],
            related_entities=_membership_entities(
                team_id,
                assistant_id=resolved_target_assistant_id,
                member_user_id=normalized_member_user_id,
            ),
            activity_id=activity_id,
        )
        return result

    def commission_colleague_into_team(
        self,
        *,
        assistant_first_name: str,
        assistant_surname: str | None = None,
        team_name: str,
        team_description: str,
        assistant_about: str | None = None,
        assistant_job_title: str | None = None,
        assistant_timezone: str | None = None,
        assistant_nationality: str | None = None,
        assistant_config: AssistantCreateConfig | None = None,
        assistant_id: int | None = None,
        team_id: int | None = None,
    ) -> dict[str, Any] | ToolError:
        """Commission a colleague into a workspace with idempotent step reporting.

        Use this when the user has confirmed "set up this colleague in this
        workspace" and the slow-brain should avoid partial primitive sequencing.
        This tool resolves or creates the colleague, resolves or creates the
        workspace, then ensures the colleague is a member.

        Parameters
        ----------
        assistant_first_name : str
            Colleague first name for lookup or creation.
        assistant_surname : str | None, optional
            Colleague surname for lookup or creation.
        team_name : str
            Workspace name for lookup or creation.
        team_description : str
            Workspace description when creation is required.
        assistant_about : str | None, optional
            Required profile summary when a new assistant must be created.
        assistant_job_title : str | None, optional
            Optional explicit colleague job title.
        assistant_timezone : str | None, optional
            Optional explicit colleague timezone.
        assistant_nationality : str | None, optional
            Optional explicit colleague nationality.
        assistant_config : AssistantCreateConfig | None, optional
            Additional structured assistant-create overrides merged when
            creation is needed.
        assistant_id : int | None, optional
            Optional explicit assistant id to reuse instead of name lookup.
        team_id : int | None, optional
            Optional explicit workspace id to reuse instead of name lookup.
        """
        colleague_name = _display_name(
            first_name=assistant_first_name,
            surname=assistant_surname,
        )
        team_name_label = safe_activity_text(team_name, fallback="Workspace")
        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title=f"Commissioning {colleague_name} into {team_name_label}",
            surfaces=["colleagues", "teams", "membership"],
            related_entities=[
                activity_entity(
                    "colleague",
                    name=colleague_name,
                    entity_id=assistant_id,
                ),
                activity_entity("team", name=team_name_label, entity_id=team_id),
            ],
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="commission_colleague_into_team",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title=f"Could not commission {colleague_name} into {team_name_label}",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        assistant_step = self._resolve_or_create_commission_assistant(
            assistant_first_name=assistant_first_name,
            assistant_surname=assistant_surname,
            assistant_about=assistant_about,
            assistant_job_title=assistant_job_title,
            assistant_timezone=assistant_timezone,
            assistant_nationality=assistant_nationality,
            assistant_config=assistant_config,
            assistant_id=assistant_id,
            organization_id=resolved_organization_id,
        )
        if _is_tool_error(assistant_step):
            self._publish_failure(
                activity_id,
                title=f"Could not commission {colleague_name} into {team_name_label}",
                error=assistant_step,
            )
            return assistant_step

        assistant_row = assistant_step["assistant"]
        resolved_assistant_id = int(assistant_row["agent_id"])
        team_step = self._resolve_or_create_commission_team(
            team_name=team_name,
            team_description=team_description,
            team_id=team_id,
            organization_id=resolved_organization_id,
        )
        if _is_tool_error(team_step):
            self._publish_failure(
                activity_id,
                title=f"Could not commission {colleague_name} into {team_name_label}",
                error=team_step,
            )
            return team_step

        team_row = team_step["team"]
        resolved_team_id = _team_id_from_record(team_row) or 0
        if resolved_team_id == 0:
            return _invalid_argument(
                message="Team record is missing a team id.",
                details={"team": team_row},
            )
        membership_step = self._ensure_commission_membership(
            team_id=resolved_team_id,
            assistant_id=resolved_assistant_id,
            organization_id=resolved_organization_id,
        )
        if _is_tool_error(membership_step):
            self._publish_failure(
                activity_id,
                title=f"Could not commission {colleague_name} into {team_name_label}",
                error=membership_step,
            )
            return membership_step

        result = {
            "assistant": {
                "status": assistant_step["status"],
                "assistant_id": resolved_assistant_id,
                "assistant": assistant_row,
            },
            "team": {
                "status": team_step["status"],
                "team_id": resolved_team_id,
                "team": team_row,
            },
            "membership": {
                "status": membership_step["status"],
                "team_id": resolved_team_id,
                "assistant_id": resolved_assistant_id,
            },
        }
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title=f"Commissioned {safe_activity_text(_assistant_display_name(assistant_row), fallback=colleague_name)} into {safe_activity_text(_team_display_name(team_row), fallback=team_name_label)}",
            surfaces=["colleagues", "teams", "membership"],
            related_entities=[
                _assistant_entity(assistant_row, fallback=colleague_name),
                _team_entity(team_row, fallback=team_name_label),
                *_membership_entities(
                    resolved_team_id,
                    assistant_id=resolved_assistant_id,
                ),
            ],
            activity_id=activity_id,
        )
        return result

    def remove_team_member(
        self,
        *,
        team_id: int,
        assistant_id: int,
    ) -> dict[str, Any] | ToolError:
        """Remove a colleague assistant from a shared workspace.

        Use this only after the user has explicitly confirmed membership removal.
        The tool validates both workspace and assistant reachability before
        mutation so failures return clear, actionable error envelopes.

        Parameters
        ----------
        team_id : int
            Workspace from which membership should be removed.
        assistant_id : int
            Assistant identifier to remove from membership.
        """

        activity_id = self._publish_activity(
            phase="started",
            stage="implementation",
            title="Removing colleague from team",
            surfaces=["membership"],
            related_entities=_membership_entities(
                team_id,
                assistant_id=assistant_id,
            ),
        )
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="remove_team_member",
        )
        if _is_tool_error(resolved_organization_id):
            self._publish_failure(
                activity_id,
                title="Could not remove colleague from team",
                error=resolved_organization_id,
            )
            return resolved_organization_id
        invalid = self._validate_team_and_assistant(
            team_id,
            assistant_id,
        )
        if invalid is not None:
            self._publish_failure(
                activity_id,
                title="Could not remove colleague from team",
                error=invalid,
            )
            return invalid

        try:
            result = unify.remove_team_member(
                resolved_organization_id,
                team_id,
                assistant_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            self._publish_failure(
                activity_id,
                title="Could not remove colleague from team",
                error=error,
            )
            return error
        self._publish_activity(
            phase="completed",
            stage="implementation",
            title="Removed colleague from team",
            surfaces=["membership"],
            related_entities=_membership_entities(
                team_id,
                assistant_id=assistant_id,
            ),
            activity_id=activity_id,
        )
        return result

    def list_teams(
        self,
        *,
        owner_user_id: str | None = None,
    ) -> list[dict[str, Any]] | ToolError:
        """List shared teams visible to the active coordinator.

        Use this to resolve ``team_id`` values, verify workspace existence, and
        avoid duplicate team creation before mutating workspace metadata or
        membership.

        Parameters
        ----------
        owner_user_id : str | None, optional
            Reserved field for caller parity.
        """
        del owner_user_id
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="list_teams",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id
        try:
            teams = unify.list_teams(
                resolved_organization_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)
        self._team_cache = teams
        self._remember_teams(teams)
        return teams

    def list_team_members(
        self,
        *,
        team_id: int,
    ) -> list[dict[str, Any]] | ToolError:
        """List members currently attached to a reachable team.

        Use this before add/remove membership mutations to verify current
        membership, prevent duplicate operations, and confirm who already has
        team access.

        Parameters
        ----------
        team_id : int
            Team identifier whose membership should be listed.
        """

        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="list_team_members",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id

        reachable = self._team_is_reachable(
            team_id,
        )
        if isinstance(reachable, dict):
            return reachable
        if not reachable:
            return _team_not_found(team_id)
        try:
            return unify.list_team_members(
                resolved_organization_id,
                team_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)

    def list_teams_for_assistant(
        self,
        *,
        assistant_id: int,
    ) -> list[dict[str, Any]] | ToolError:
        """List shared teams currently attached to a colleague assistant.

        Use this when auditing a colleague's workspace footprint before changing
        access, performing cleanup, or explaining current ownership boundaries.

        Parameters
        ----------
        assistant_id : int
            Assistant identifier whose workspace memberships should be listed.
        """
        resolved_organization_id = self._resolve_target_organization_id(
            require_organization=True,
            operation_name="list_teams_for_assistant",
        )
        if _is_tool_error(resolved_organization_id):
            return resolved_organization_id
        reachable = self._assistant_is_reachable(
            assistant_id,
        )
        if isinstance(reachable, dict):
            return reachable
        if not reachable:
            return _assistant_not_found(assistant_id)
        try:
            return unify.list_teams_for_assistant(
                assistant_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)

    def _resolve_or_create_commission_assistant(
        self,
        *,
        assistant_first_name: str,
        assistant_surname: str | None,
        assistant_about: str | None,
        assistant_job_title: str | None,
        assistant_timezone: str | None,
        assistant_nationality: str | None,
        assistant_config: AssistantCreateConfig | None,
        assistant_id: int | None,
        organization_id: int | None,
    ) -> dict[str, Any] | ToolError:
        if assistant_id is not None:
            listed = self.list_assistants(agent_id=assistant_id)
            if _is_tool_error(listed):
                return listed
            if not listed:
                return _assistant_not_found(assistant_id)
            if len(listed) > 1:
                return _tool_conflict(
                    message=(
                        "Multiple assistants matched the provided assistant_id while "
                        "commissioning."
                    ),
                    details={"assistant_id": assistant_id, "matches": len(listed)},
                )
            assistant = listed[0]
            self._remember_assistant(assistant)
            return {"status": "reused", "assistant": assistant}

        listed = self.list_assistants()
        if _is_tool_error(listed):
            return listed
        matches = [
            assistant
            for assistant in listed
            if _assistant_name_matches(
                assistant,
                first_name=assistant_first_name,
                surname=assistant_surname,
            )
        ]
        if len(matches) > 1:
            return _tool_conflict(
                message=(
                    "Multiple existing assistants matched the requested name. "
                    "Pass assistant_id to disambiguate."
                ),
                details={
                    "assistant_first_name": assistant_first_name,
                    "assistant_surname": assistant_surname,
                    "matches": [row.get("agent_id") for row in matches],
                },
            )
        if len(matches) == 1:
            assistant = matches[0]
            self._remember_assistant(assistant)
            return {"status": "reused", "assistant": assistant}

        normalized_about = self._normalize_about(assistant_about)
        if normalized_about is None:
            return _invalid_argument(
                message=(
                    "Provide `assistant_about` before commissioning a new colleague."
                ),
                details={"field": "assistant_about"},
            )
        resolved_assistant_config = self._assistant_defaults_from_coordinator(
            first_name=assistant_first_name,
            surname=assistant_surname,
            about=normalized_about,
            job_title=assistant_job_title,
            timezone=assistant_timezone,
            nationality=assistant_nationality,
            config=assistant_config,
        )
        resolved_assistant_config = self._assistant_config_with_organization(
            resolved_assistant_config,
            organization_id=organization_id,
        )
        try:
            created = unify.create_assistant(
                first_name=assistant_first_name,
                surname=assistant_surname,
                config=resolved_assistant_config,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)
        self._clear_assistant_cache()
        self._remember_assistant(created)
        return {"status": "created", "assistant": created}

    def _resolve_or_create_commission_team(
        self,
        *,
        team_name: str,
        team_description: str,
        team_id: int | None,
        organization_id: int | None,
    ) -> dict[str, Any] | ToolError:
        if team_id is not None:
            listed = self.list_teams()
            if _is_tool_error(listed):
                return listed
            matches = [
                team
                for team in listed
                if str(_team_id_from_record(team)) == str(team_id)
            ]
            if not matches:
                return _team_not_found(team_id)
            if len(matches) > 1:
                return _tool_conflict(
                    message=(
                        "Multiple teams matched the provided team_id while "
                        "commissioning."
                    ),
                    details={"team_id": team_id, "matches": len(matches)},
                )
            team = matches[0]
            self._remember_team(team)
            return {"status": "reused", "team": team}

        listed = self.list_teams()
        if _is_tool_error(listed):
            return listed
        matches = [
            team for team in listed if _team_name_matches(team, team_name=team_name)
        ]
        if len(matches) > 1:
            return _tool_conflict(
                message=(
                    "Multiple existing teams matched the requested name. "
                    "Pass team_id to disambiguate."
                ),
                details={
                    "team_name": team_name,
                    "matches": [_team_id_from_record(row) for row in matches],
                },
            )
        if len(matches) == 1:
            team = matches[0]
            self._remember_team(team)
            return {"status": "reused", "team": team}

        try:
            created = unify.create_team(
                organization_id,
                name=team_name,
                description=team_description,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            return _request_error_to_tool_error(exc)
        self._clear_team_cache()
        self._remember_team(created)
        return {"status": "created", "team": created}

    def _ensure_commission_membership(
        self,
        *,
        team_id: int,
        assistant_id: int,
        organization_id: int,
    ) -> dict[str, Any] | ToolError:
        listed = self.list_team_members(team_id=team_id)
        if _is_tool_error(listed):
            return listed
        if any(
            _member_matches_assistant(member, assistant_id=assistant_id)
            for member in listed
        ):
            return {"status": "already_member"}
        try:
            unify.add_team_member(
                organization_id,
                team_id,
                assistant_id=assistant_id,
                api_key=SESSION_DETAILS.unify_key,
            )
        except RequestError as exc:
            error = _request_error_to_tool_error(exc)
            if (
                error.get("error_kind") == "conflict"
                and "already"
                in _tool_error_message(
                    error,
                ).lower()
            ):
                return {"status": "already_member"}
            return error
        return {"status": "added"}

    def _org_member_is_reachable(
        self,
        member_user_id: str,
    ) -> bool | ToolError:
        """Return whether a user id belongs to this Coordinator's org roster."""
        members = self.list_org_members()
        if _is_tool_error(members):
            return members
        return any(
            str(member.get("user_id") or member.get("id") or "") == str(member_user_id)
            for member in members
        )

    def _assistant_is_reachable(
        self,
        agent_id: int,
    ) -> bool | ToolError:
        if str(agent_id) in self._known_assistant_ids:
            return True
        assistants = self._assistant_cache
        if assistants is None:
            listed = self.list_assistants(agent_id=agent_id)
            if _is_tool_error(listed):
                return listed
            assistants = listed
        reachable_ids = {str(row.get("agent_id")) for row in assistants}
        self._known_assistant_ids.update(reachable_ids)
        return str(agent_id) in reachable_ids

    def _team_is_reachable(
        self,
        team_id: int,
    ) -> bool | ToolError:
        listed = self.list_teams()
        if _is_tool_error(listed):
            return listed
        teams = listed
        reachable_ids = {
            str(resolved_team_id)
            for row in teams
            if (resolved_team_id := _team_id_from_record(row)) is not None
        }
        self._known_team_ids.update(reachable_ids)
        return str(team_id) in reachable_ids

    def _validate_team_and_assistant(
        self,
        team_id: int,
        assistant_id: int,
    ) -> ToolError | None:
        reachable_team = self._team_is_reachable(team_id)
        if isinstance(reachable_team, dict):
            return reachable_team
        if not reachable_team:
            return _team_not_found(team_id)

        reachable_assistant = self._assistant_is_reachable(assistant_id)
        if isinstance(reachable_assistant, dict):
            return reachable_assistant
        if not reachable_assistant:
            return _assistant_not_found(assistant_id)
        return None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _assistant_belongs_to_organization(
        cls,
        assistant: dict[str, Any],
        *,
        organization_id: int,
    ) -> bool:
        assistant_organization_id = cls._coerce_int(
            assistant.get("organization_id") or assistant.get("org_id"),
        )
        return assistant_organization_id == organization_id

    def _resolve_target_organization_id(
        self,
        *,
        require_organization: bool,
        operation_name: str,
    ) -> int | None | ToolError:
        active_organization_id = SESSION_DETAILS.org_id
        if active_organization_id is not None:
            return active_organization_id
        if require_organization:
            return _invalid_argument(
                message=(
                    f"`{operation_name}` requires an organization workspace Coordinator. "
                    "Switch to the target organization workspace and try again."
                ),
                details={
                    "active_organization_id": active_organization_id,
                },
            )
        return None

    def _clear_assistant_cache(self) -> None:
        self._assistant_cache = None

    def _clear_team_cache(self) -> None:
        self._team_cache = None

    def _remember_assistants(self, assistants: list[dict[str, Any]]) -> None:
        self._known_assistant_ids.update(
            str(row["agent_id"])
            for row in assistants
            if row.get("agent_id") is not None
        )

    def _remember_assistant(self, assistant: dict[str, Any]) -> None:
        agent_id = assistant.get("agent_id")
        if agent_id is not None:
            self._known_assistant_ids.add(str(agent_id))

    def _remember_team(self, team: dict[str, Any]) -> None:
        team_id = _team_id_from_record(team)
        if team_id is not None:
            self._known_team_ids.add(str(team_id))

    def _remember_teams(self, teams: list[dict[str, Any]]) -> None:
        for team in teams:
            self._remember_team(team)

    def _publish_activity(
        self,
        *,
        phase: CoordinatorActivityPhase,
        stage: CoordinatorActivityStage,
        title: str,
        surfaces: Sequence[CoordinatorActivitySurface],
        related_entities: Sequence[CoordinatorActivityEntity | dict[str, Any]] = (),
        activity_id: str | None = None,
    ) -> str:
        resolved_activity_id = activity_id or coordinator_activity_id("setup")
        publish_coordinator_activity(
            phase=phase,
            stage=stage,
            title=title,
            surfaces=surfaces,
            related_entities=related_entities,
            activity_id=resolved_activity_id,
            correlation_id=resolved_activity_id,
        )
        if phase in {"started", "progress", "needs_input", "blocked"}:
            self._activity_metadata[resolved_activity_id] = (
                list(dict.fromkeys(surfaces)),
                list(related_entities),
            )
        elif phase in {"completed", "failed"}:
            self._activity_metadata.pop(resolved_activity_id, None)
        return resolved_activity_id

    def _publish_failure(
        self,
        activity_id: str,
        *,
        title: str,
        error: ToolError,
    ) -> None:
        surfaces, related_entities = self._activity_metadata.pop(
            activity_id,
            ([], []),
        )
        publish_coordinator_activity(
            phase="failed",
            stage="implementation",
            title=title,
            surfaces=surfaces,
            related_entities=related_entities,
            activity_id=activity_id,
            correlation_id=activity_id,
            status="error",
            error=_tool_error_message(error),
        )


def _tool_conflict(*, message: str, details: dict[str, Any]) -> ToolError:
    return {
        "error_kind": "conflict",
        "message": message,
        "details": details,
    }


def _invalid_argument(*, message: str, details: dict[str, Any]) -> ToolError:
    return {
        "error_kind": "invalid_argument",
        "message": message,
        "details": details,
    }


def _is_tool_error(value: Any) -> bool:
    return isinstance(value, dict) and "error_kind" in value


def _normalize_lookup_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _assistant_name_matches(
    assistant: dict[str, Any],
    *,
    first_name: str,
    surname: str | None,
) -> bool:
    candidate = _normalize_lookup_text(
        f"{assistant.get('first_name') or assistant.get('firstName') or ''} "
        f"{assistant.get('surname') or assistant.get('last_name') or assistant.get('lastName') or ''}",
    )
    target = _normalize_lookup_text(f"{first_name} {surname or ''}")
    return bool(target) and candidate == target


def _team_name_matches(team: dict[str, Any], *, team_name: str) -> bool:
    return _normalize_lookup_text(team.get("name")) == _normalize_lookup_text(
        team_name,
    )


def _member_matches_assistant(member: dict[str, Any], *, assistant_id: int) -> bool:
    member_assistant_id = member.get("agent_id")
    if member_assistant_id is None:
        member_assistant_id = member.get("assistant_id")
    return str(member_assistant_id) == str(assistant_id)


def _assistant_not_found(agent_id: int) -> ToolError:
    """Build a tool error for assistant ids outside the reachable set."""

    return {
        "error_kind": "not_found",
        "message": f"Assistant {agent_id} is not reachable by this Coordinator.",
        "details": {"agent_id": agent_id},
    }


def _display_name(*, first_name: object, surname: object | None = None) -> str:
    name = resolve_display_name(first_name=first_name, surname=surname)
    return safe_activity_text(name, fallback="New colleague")


def _assistant_display_name(assistant: dict[str, Any]) -> str:
    return safe_activity_text(
        resolve_assistant_display_name(assistant),
        fallback="New colleague",
    )


def _assistant_entity(
    assistant: dict[str, Any],
    *,
    fallback: str,
):
    return activity_entity(
        "colleague",
        name=_assistant_display_name(assistant) or fallback,
        entity_id=assistant.get("agent_id")
        or assistant.get("agentId")
        or assistant.get("id"),
    )


def _team_display_name(team: dict[str, Any]) -> str:
    return safe_activity_text(team.get("name"), fallback="Team")


def _team_entity(
    team: dict[str, Any],
    *,
    fallback: str,
):
    return activity_entity(
        "team",
        name=_team_display_name(team) or fallback,
        entity_id=_team_id_from_record(team),
    )


def _membership_entities(
    team_id: int,
    *,
    assistant_id: int | None = None,
    member_user_id: str | None = None,
):
    entities: list[CoordinatorActivityEntity | dict[str, Any]] = [
        activity_entity("team", name="Team", entity_id=team_id),
    ]
    if assistant_id is not None:
        entities.append(
            activity_entity("colleague", name="Colleague", entity_id=assistant_id),
        )
    if member_user_id:
        entities.append(
            activity_entity("human", name="Member", entity_id=member_user_id),
        )
    return entities


def _extract_assistant_id(result: dict[str, Any]) -> int | None:
    assistant_id = result.get("assistant_id") or result.get("agent_id")
    if assistant_id is None:
        return None
    try:
        return int(assistant_id)
    except (TypeError, ValueError):
        return None


def _surface_for_delegate_intent(intent: str) -> CoordinatorActivitySurface:
    """Return the activity surface that best matches a colleague assignment."""

    return _DELEGATE_INTENT_SURFACES.get(intent.strip().lower(), "colleagues")


def _tool_error_message(error: ToolError) -> str:
    return safe_activity_text(
        error.get("message"),
        fallback="The setup step could not finish.",
    )


def _team_id_from_record(team: dict[str, Any]) -> int | None:
    """Return the canonical team id from an Orchestra team payload."""
    raw_team_id = team.get("team_id")
    if raw_team_id is None:
        raw_team_id = team.get("id")
    if raw_team_id is None:
        return None
    try:
        return int(raw_team_id)
    except (TypeError, ValueError):
        return None


def _team_not_found(team_id: int) -> ToolError:
    """Build a tool error for team ids outside the reachable set."""

    return {
        "error_kind": "not_found",
        "message": f"Team {team_id} is not reachable by this Coordinator.",
        "details": {"team_id": team_id},
    }


def _org_member_not_found(member_user_id: str) -> ToolError:
    """Build a tool error for org member ids outside the reachable set."""

    return {
        "error_kind": "not_found",
        "message": (
            f"Organization member {member_user_id} is not reachable by this Coordinator."
        ),
        "details": {"member_user_id": member_user_id},
    }


def _request_error_to_tool_error(exc: RequestError) -> ToolError:
    """Translate an SDK request failure into the shared tool-error envelope."""

    response = exc.response
    status_code = getattr(response, "status_code", 500)
    if status_code in (400, 422):
        error_kind = "invalid_argument"
    elif status_code in (401, 403):
        error_kind = "permission_denied"
    elif status_code == 404:
        error_kind = "not_found"
    elif status_code == 409:
        error_kind = "conflict"
    else:
        error_kind = "internal"

    response_text = getattr(response, "text", "") or str(exc)
    message = response_text
    details: dict[str, Any] = {"status_code": status_code}

    detail_payload: Any = None
    if response is not None:
        try:
            response_payload = response.json()
        except Exception:
            response_payload = None
        if isinstance(response_payload, dict):
            detail_payload = response_payload.get("detail")

    if isinstance(detail_payload, dict):
        message = safe_activity_text(
            detail_payload.get("message"),
            fallback=safe_activity_text(
                detail_payload.get("error"),
                fallback=response_text,
            ),
        )
        existing_id = detail_payload.get("existing_id")
        if isinstance(existing_id, int):
            details["existing_id"] = existing_id
        error_code = detail_payload.get("error")
        if isinstance(error_code, str) and error_code.strip():
            details["error"] = error_code
        nested_details = detail_payload.get("details")
        if isinstance(nested_details, dict):
            details.update(nested_details)
    elif isinstance(detail_payload, str) and detail_payload.strip():
        message = detail_payload

    return {
        "error_kind": error_kind,
        "message": message,
        "details": details,
    }


class CoordinatorWorkspaceManager(metaclass=SingletonABCMeta):
    """Coordinator-only workspace lifecycle primitive owner."""

    _PRIMITIVE_METHODS = COORDINATOR_TOOL_METHOD_NAMES

    @staticmethod
    def _require_coordinator_role() -> ToolError | None:
        if SESSION_DETAILS.is_coordinator:
            return None
        return _coordinator_role_required_error()


for _method_name in COORDINATOR_TOOL_METHOD_NAMES:
    setattr(
        CoordinatorWorkspaceManager,
        _method_name,
        _coordinator_primitive(getattr(_CoordinatorWorkspaceSession, _method_name)),
    )
