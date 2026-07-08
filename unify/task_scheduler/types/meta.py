"""Pydantic model for the Tasks/Meta context."""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class TaskMeta(AuthoredRow):
    """Metadata record for source-defined custom task sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_tasks_hash: str = Field(
        "",
        description="Hash of all source-defined custom task entries.",
    )
