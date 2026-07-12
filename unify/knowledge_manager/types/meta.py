"""Pydantic model for the Knowledge/Meta context."""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class KnowledgeMeta(AuthoredRow):
    """Metadata record for tenant Knowledge sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_knowledge_hash: str = Field(
        "",
        description="Hash of all source-defined custom knowledge claims.",
    )
