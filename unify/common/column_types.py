"""Shared column type labels used when provisioning table schemas."""

from enum import StrEnum


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
