"""Trust hash and ledger fold for compositional functions.

``function_trust_hash`` names the exact content a verification summary
applies to: the normalised source, every compositional dependency's own trust
hash (transitively), the venv and its pyproject, and the language. Any
component changing changes the hash, which is what invalidates trust.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional

from ..types.verification import (
    MAX_DISTINCT_ARGS_SIGNATURES,
    VerdictKind,
    VerificationRow,
    VerificationSummary,
)

RowResolver = Callable[[str], Optional[Mapping[str, Any]]]
VenvResolver = Callable[[int], Optional[Mapping[str, Any]]]


def normalize_implementation(source: Optional[str]) -> str:
    """Canonical form of a function body for hashing: line endings and trailing whitespace only."""
    if not source:
        return ""
    lines = source.replace("\r\n", "\n").replace("\r", "\n").strip("\n").split("\n")
    return "\n".join(line.rstrip() for line in lines)


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def function_trust_hash(
    fn: Mapping[str, Any],
    *,
    resolve_row: RowResolver,
    resolve_venv: VenvResolver,
) -> str:
    """Return the trust hash of ``fn`` (a compositional row as a dict).

    ``resolve_row(name)`` returns the row of a compositional dependency by
    name (or None when it does not resolve); ``resolve_venv(venv_id)``
    returns the venv row (whose ``venv`` field is the pyproject content).
    Dependency hashes are computed recursively over content, so a change
    anywhere in the closure changes the root's hash; cycles fall back to the
    dependency's name.
    """
    cache: Dict[str, str] = {}

    def _hash(row: Mapping[str, Any], stack: frozenset) -> str:
        name = str(row.get("name") or "")
        if name in cache:
            return cache[name]
        components: list[Any] = [
            "impl",
            normalize_implementation(row.get("implementation")),
            "language",
            str(row.get("language") or "python"),
        ]
        dep_parts: list[str] = []
        for dep in sorted(
            d for d in (row.get("depends_on") or []) if isinstance(d, str)
        ):
            if "." in dep:
                dep_parts.append(f"{dep}=primitive")
                continue
            if dep in stack:
                dep_parts.append(f"{dep}=cycle")
                continue
            dep_row = resolve_row(dep)
            if dep_row is None:
                dep_parts.append(f"{dep}=unresolved")
                continue
            dep_parts.append(f"{dep}={_hash(dep_row, stack | {name})}")
        components.extend(["deps", *dep_parts])
        venv_id = row.get("venv_id")
        if venv_id is not None:
            venv_row = resolve_venv(int(venv_id))
            venv_content = "" if venv_row is None else str(venv_row.get("venv") or "")
            components.extend(["venv", str(int(venv_id)), _sha256(venv_content)])
        digest = _sha256(json.dumps(components, ensure_ascii=False))
        cache[name] = digest
        return digest

    return _hash(fn, frozenset())


def args_signature(kwargs: Mapping[str, Any]) -> str:
    """Stable digest of a call's keyword arguments."""
    canonical = json.dumps(
        dict(kwargs),
        sort_keys=True,
        default=str,
        ensure_ascii=False,
    )
    return _sha256(canonical)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def apply_verdict(
    summary: VerificationSummary,
    row: VerificationRow,
) -> VerificationSummary:
    """Fold one verdict row into a summary, returning the updated summary."""
    updated = summary.model_copy(deep=True)
    if row.verdict == "PASS":
        key = str(row.kind)
        updated.passes[key] = updated.pass_count(key) + 1
    elif row.verdict == "FAIL":
        updated.fails += 1
    else:
        updated.unsure += 1
    if row.kind == VerdictKind.spot_check:
        updated.spot_checks += 1
    signature = row.args_signature
    if signature and signature not in updated.distinct_args_signatures:
        updated.distinct_args_signatures.append(signature)
        if len(updated.distinct_args_signatures) > MAX_DISTINCT_ARGS_SIGNATURES:
            del updated.distinct_args_signatures[0]
    updated.last_verdict_at = row.created_at or utcnow()
    return updated


def fold_rows(rows: Iterable[VerificationRow]) -> VerificationSummary:
    """Fold verdict rows (all for one hash) into a fresh summary."""
    summary = VerificationSummary()
    for row in rows:
        summary = apply_verdict(summary, row)
    return summary
