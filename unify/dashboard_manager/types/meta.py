from pydantic import Field

from unify.common.authorship import AuthoredRow


class DashboardMeta(AuthoredRow):
    """Metadata record for source-defined custom dashboard sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_dashboards_hash: str = Field(
        "",
        description="Hash of all source-defined custom dashboard tiles and layouts.",
    )
