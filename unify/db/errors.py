"""Exceptions raised by the local store.

Every failure the store reports is one of these, so callers can react to the
specific condition (a missing context, a duplicate unique key, a lost race)
without inspecting message text.
"""

from __future__ import annotations


class StoreError(Exception):
    """Base class for every store failure."""


class NotFound(StoreError):
    """The named project, context, field or row does not exist."""


class AlreadyExists(StoreError):
    """A project, context or field with that name already exists."""


class DuplicateKey(StoreError):
    """A write would produce two live rows with the same unique key."""

    def __init__(self, field: str, value: object) -> None:
        super().__init__(f"Duplicate entry for unique field {field!r}: {value!r}")
        self.field = field
        self.value = value


class InvalidExpression(StoreError):
    """A filter, sorting or derived-column expression could not be compiled."""


class Conflict(StoreError):
    """A write lost a race with another writer (claims, overwrite=False)."""
