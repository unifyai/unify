"""Pydantic model for the Contacts/Meta context."""

from pydantic import Field

from unify.common.authorship import AuthoredRow


class ContactMeta(AuthoredRow):
    """Metadata record for source-defined custom contact sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_contacts_hash: str = Field(
        "",
        description="Hash of all source-defined custom contact entries.",
    )
