from __future__ import annotations

import logging
from typing import Any, Dict

from unify.db import DuplicateKey, StoreError

_log = logging.getLogger(__name__)

from ..session_details import (
    PLACEHOLDER_ASSISTANT_BIO,
    PLACEHOLDER_ASSISTANT_EMAIL,
    PLACEHOLDER_ASSISTANT_FIRST_NAME,
    PLACEHOLDER_ASSISTANT_PHONE,
    PLACEHOLDER_ASSISTANT_SURNAME,
    PLACEHOLDER_USER_EMAIL,
    PLACEHOLDER_USER_FIRST_NAME,
    PLACEHOLDER_USER_SURNAME,
)
from .ops import partition_create_kwargs, partition_update_kwargs


def _is_duplicate_contact_error(error: StoreError) -> bool:
    return isinstance(error, DuplicateKey)


def _is_assistant_populated() -> bool:
    """Return True if SESSION_DETAILS has real assistant profile data."""
    from ..session_details import SESSION_DETAILS

    if not SESSION_DETAILS.is_initialized:
        return False
    return bool(SESSION_DETAILS.assistant.first_name)


def _resolve_user_details(self) -> Dict[str, Any]:
    """Resolve user details from SESSION_DETAILS, API, or defaults.

    When SESSION_DETAILS has not been initialized (e.g., during tests),
    returns default user info to avoid calling real APIs.

    Returns
    -------
    dict
        User info dict with first_name, last_name, email, and optionally phone_number.
    """
    from ..session_details import SESSION_DETAILS

    # If SESSION_DETAILS hasn't been initialized, use defaults.
    # This ensures tests don't call real APIs for user info.
    if not SESSION_DETAILS.is_initialized:
        return {
            "first_name": PLACEHOLDER_USER_FIRST_NAME,
            "last_name": PLACEHOLDER_USER_SURNAME,
            "email": PLACEHOLDER_USER_EMAIL,
        }

    user_info: Dict[str, Any] = {
        "first_name": SESSION_DETAILS.user.first_name or PLACEHOLDER_USER_FIRST_NAME,
        "last_name": SESSION_DETAILS.user.surname or PLACEHOLDER_USER_SURNAME,
        "email": SESSION_DETAILS.user.email or PLACEHOLDER_USER_EMAIL,
    }
    if SESSION_DETAILS.user.number:
        user_info["phone_number"] = SESSION_DETAILS.user.number
    return user_info


def provision_assistant_contact(
    self,
    assistant_log,
    *,
    contact_id: int | None = None,
) -> None:
    """Provision the assistant system contact.

    Creates or updates the assistant contact using details from
    SESSION_DETAILS or default values.
    """
    from ..session_details import SESSION_DETAILS

    resolved_contact_id = int(contact_id or SESSION_DETAILS.self_contact_id)
    populated = _is_assistant_populated()
    ast = SESSION_DETAILS.assistant

    base_fields = {fld: None for fld in self._BUILTIN_FIELDS if fld != "contact_id"}
    base_fields["should_respond"] = True
    base_fields["response_policy"] = ""
    base_fields["is_system"] = True
    base_fields.update(
        {
            "first_name": (
                ast.first_name if populated else PLACEHOLDER_ASSISTANT_FIRST_NAME
            ),
            "surname": ast.surname if populated else PLACEHOLDER_ASSISTANT_SURNAME,
            "email_address": ast.email if populated else PLACEHOLDER_ASSISTANT_EMAIL,
            "phone_number": ast.number if populated else PLACEHOLDER_ASSISTANT_PHONE,
            "bio": ast.about if populated else PLACEHOLDER_ASSISTANT_BIO,
            "job_title": (ast.job_title or None) if populated else None,
            "timezone": (ast.timezone or "UTC") if populated else "UTC",
            "rolling_summary": None,
        },
    )

    if assistant_log is not None:
        try:
            entries = assistant_log.entries
            fetched_bio = ast.about if populated else None
            fetched_tz = ast.timezone if populated else None
            fetched_phone = ast.number if populated else None
            fetched_first_name = ast.first_name if populated else None
            fetched_surname = ast.surname if populated else None
            fetched_job_title = (ast.job_title or None) if populated else None

            needs_timezone = fetched_tz and entries.get("timezone") != fetched_tz
            needs_bio = fetched_bio and entries.get("bio") != fetched_bio
            needs_job_title = (
                populated and (entries.get("job_title") or None) != fetched_job_title
            )
            needs_phone = fetched_phone and entries.get("phone_number") != fetched_phone
            needs_is_system = entries.get("is_system") is not True
            needs_first_name = (
                fetched_first_name and entries.get("first_name") != fetched_first_name
            )
            needs_surname = (
                fetched_surname and entries.get("surname") != fetched_surname
            )

            if (
                needs_timezone
                or needs_bio
                or needs_job_title
                or needs_phone
                or needs_is_system
                or needs_first_name
                or needs_surname
            ):
                update_kwargs: Dict[str, Any] = {
                    "contact_id": resolved_contact_id,
                    "_log_id": assistant_log.id,
                }
                if needs_timezone:
                    update_kwargs["timezone"] = fetched_tz
                if needs_bio:
                    update_kwargs["bio"] = fetched_bio
                if needs_job_title:
                    update_kwargs["job_title"] = fetched_job_title
                if needs_phone:
                    update_kwargs["phone_number"] = fetched_phone
                if needs_is_system:
                    update_kwargs["is_system"] = True
                if needs_first_name:
                    update_kwargs["first_name"] = fetched_first_name
                if needs_surname:
                    update_kwargs["surname"] = fetched_surname
                self.update_contact(**partition_update_kwargs(update_kwargs))
            else:
                # Warm local cache when no change needed
                self._data_store.put(entries)
        except Exception:
            pass
        return

    # Insert the assistant row. Race conditions are handled by the store's
    # field-level uniqueness enforcement on email_address / phone_number.
    try:
        outcome = self._create_contact(
            **partition_create_kwargs(
                {"contact_id": resolved_contact_id, **base_fields},
            ),
        )
        if int(outcome["details"]["contact_id"]) != resolved_contact_id:
            raise RuntimeError("Assistant self contact was created with the wrong id.")
        resolved_contact_id = int(outcome["details"]["contact_id"])
    except StoreError as e:
        if not _is_duplicate_contact_error(e):
            raise


def provision_user_contact(self, user_log, *, contact_id: int | None = None) -> None:
    """Provision the user system contact.

    Creates or updates the user (boss) contact using details resolved from
    SESSION_DETAILS, the Unify API, or default values.
    """
    from ..session_details import SESSION_DETAILS

    resolved_contact_id = int(contact_id or SESSION_DETAILS.boss_contact_id)

    user_info = _resolve_user_details(self)

    base_fields: Dict[str, Any] = {
        fld: None
        for fld in self._BUILTIN_FIELDS
        if fld not in {"contact_id", "rolling_summary"}
    }
    base_fields["should_respond"] = True
    base_fields["is_system"] = True
    base_fields.update(
        {
            "first_name": user_info.get("first_name"),
            "surname": user_info.get("last_name"),
            "email_address": user_info.get("email"),
            "phone_number": user_info.get("phone_number"),
            "bio": user_info.get("bio"),
            "response_policy": self.USER_MANAGER_RESPONSE_POLICY,
        },
    )

    # Use fetched timezone if available, fallback to UTC
    base_fields["timezone"] = user_info.get("timezone") or "UTC"

    if user_log is not None:
        try:
            entries = user_log.entries
            fetched_bio = user_info.get("bio")
            fetched_tz = user_info.get("timezone")
            fetched_phone = user_info.get("phone_number")

            needs_timezone = fetched_tz and entries.get("timezone") != fetched_tz
            needs_bio = fetched_bio and entries.get("bio") != fetched_bio
            needs_phone = fetched_phone and entries.get("phone_number") != fetched_phone
            needs_is_system = entries.get("is_system") is not True

            if needs_timezone or needs_bio or needs_phone or needs_is_system:
                update_kwargs: Dict[str, Any] = {
                    "contact_id": resolved_contact_id,
                    "_log_id": user_log.id,
                }
                if needs_timezone:
                    update_kwargs["timezone"] = fetched_tz
                if needs_bio:
                    update_kwargs["bio"] = fetched_bio
                if needs_phone:
                    update_kwargs["phone_number"] = fetched_phone
                if needs_is_system:
                    update_kwargs["is_system"] = True
                self.update_contact(**partition_update_kwargs(update_kwargs))
            else:
                # Warm local cache when no change needed
                self._data_store.put(entries)
        except Exception:
            pass
        return

    # Insert the user row. Race conditions are handled by the store's
    # field-level uniqueness enforcement on email_address / phone_number.
    try:
        outcome = self._create_contact(
            **partition_create_kwargs(
                {
                    "contact_id": resolved_contact_id,
                    **{k: v for k, v in base_fields.items() if v is not None},
                },
            ),
        )
        if int(outcome["details"]["contact_id"]) != resolved_contact_id:
            raise RuntimeError("Boss contact was created with the wrong id.")
        resolved_contact_id = int(outcome["details"]["contact_id"])
    except StoreError as e:
        if not _is_duplicate_contact_error(e):
            raise
