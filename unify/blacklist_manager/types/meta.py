"""Pydantic model for the BlackList/Meta context."""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class BlacklistMeta(AuthoredRow):
    """Metadata record for source-defined custom blacklist sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_blacklist_hash: str = Field(
        "",
        description="Hash of all source-defined custom blacklist entries.",
    )
