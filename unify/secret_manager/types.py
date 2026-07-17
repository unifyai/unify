from __future__ import annotations

from typing import List

from pydantic import Field

from unify.common.authorship import AuthoredRow

UNASSIGNED_SECRET_ID = -1


class Secret(AuthoredRow):
    """Fixed schema for storing secrets.

    The schema is intentionally immutable: columns cannot be added or removed.
    """

    secret_id: int = Field(
        default=UNASSIGNED_SECRET_ID,
        ge=UNASSIGNED_SECRET_ID,
        description=(
            "Stable integer identifier for the secret row (auto-counted). Safe to surface to LLMs."
        ),
    )
    name: str = Field(
        description="Unique identifier for the secret. Used in placeholders like ${name}.",
    )
    value: str = Field(description="The raw secret value. Never expose to LLMs.")
    description: str = Field(
        default="",
        description="Human-readable description of the secret's purpose.",
        json_schema_extra={"ui_editable": True},
    )
    destination: str = Field(
        default="personal",
        description="Vault that owns the credential metadata, such as personal or team:<id>.",
    )
    description_emb: List[float] = Field(
        default_factory=list,
        description="Vector embedding of the description for semantic search.",
    )
    custom_key: str | None = Field(
        default=None,
        description="Stable source key for deployment-defined secrets.",
    )
    custom_hash: str | None = Field(
        default=None,
        description="Content hash for deployment-defined secrets.",
    )


class SecretMeta(AuthoredRow):
    """Metadata record for source-defined custom secret sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_secrets_hash: str = Field(
        "",
        description="Hash of all source-defined custom secret entries.",
    )
