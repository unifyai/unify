from pydantic import Field

from unify.common.authorship import AuthoredRow


class DataMeta(AuthoredRow):
    """Metadata record for source-defined custom data sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_data_hash: str = Field(
        "",
        description="Hash of all source-defined custom data rows.",
    )
