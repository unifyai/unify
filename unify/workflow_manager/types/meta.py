"""Pydantic model for the ``Workflows/Meta`` context."""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class WorkflowMeta(AuthoredRow):
    """Metadata record for source-defined workflow catalogue sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_workflow_hash: str = Field(
        "",
        description="Hash of all source-defined workflow installations.",
    )
