"""Stable hashing helpers for FunctionManager materialized rows."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any


def stable_hash_for_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    fields: Iterable[str],
    sort_field: str = "name",
    digest_chars: int | None = None,
    projection: str = "json",
) -> str:
    """Hash selected row fields in deterministic order.

    FunctionManager uses this for materialized row idempotency checks. Callers
    choose their own field list because static primitives and provider-backed
    integration rows have different lifecycle metadata, but both need the same
    deterministic projection and SHA-256 calculation.
    """

    selected_fields = tuple(fields)
    ordered_rows = sorted(rows, key=lambda item: str(item.get(sort_field) or ""))
    if projection == "delimited":
        # Preserve the historical static-primitive hash payload:
        # ``name|argspec|docstring`` lines joined by newlines.
        payload = "\n".join(
            "|".join(str(row.get(key, "")) for key in selected_fields)
            for row in ordered_rows
        )
    else:
        stable_rows = [
            {key: row.get(key) for key in selected_fields if key in row}
            for row in ordered_rows
        ]
        payload = json.dumps(stable_rows, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return digest[:digest_chars] if digest_chars is not None else digest
