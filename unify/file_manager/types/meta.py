from pydantic import Field

from unify.common.authorship import AuthoredRow


class FileMeta(AuthoredRow):
    """Metadata record for source-defined custom file sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_files_hash: str = Field(
        "",
        description="Hash of all source-defined required file mappings.",
    )
