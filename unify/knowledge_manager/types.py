from enum import StrEnum

from pydantic import Field

from unify.common.authorship import AuthoredRow


class ColumnType(StrEnum):
    str = "str"
    int = "int"
    float = "float"
    bool = "bool"
    dict = "dict"
    list = "list"
    datetime = "datetime"
    date = "date"
    time = "time"


column_type_schema = {
    "title": "ColumnType",
    "type": "string",
    "enum": [member.value for member in ColumnType],
    "description": "Allowed types for a column.",
}


class KnowledgeMeta(AuthoredRow):
    """Metadata record for source-defined custom knowledge sync state."""

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    custom_knowledge_hash: str = Field(
        "",
        description="Hash of all source-defined custom knowledge rows.",
    )
