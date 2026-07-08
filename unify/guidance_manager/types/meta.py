"""
Pydantic model for the Guidance/Meta context.

Stores metadata about source-defined custom guidance sync state.
"""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class GuidanceMeta(AuthoredRow):
    """Metadata record for tenant Guidance sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_guidance_hash: str = Field(
        "",
        description="Hash of all source-defined custom guidance entries.",
    )
