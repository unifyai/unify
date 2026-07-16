"""Resolve authored task resource requirements.

``requires_filesystem`` and ``requires_computer`` are the canonical knobs.
Legacy ``browser_target == "assistant_desktop"`` implies ``requires_computer``.
"""

from __future__ import annotations

from typing import Any, Mapping


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no", ""}:
            return False
    return bool(value)


def resolve_requires_filesystem(data: Mapping[str, Any] | None) -> bool:
    """Return whether assistant Local must be ready before the run starts."""

    if not isinstance(data, Mapping):
        return False
    return _coerce_bool(data.get("requires_filesystem"))


def resolve_requires_computer(data: Mapping[str, Any] | None) -> bool:
    """Return whether a computer-use desktop must be ready before the run starts.

    Legacy rows that only set ``browser_target="assistant_desktop"`` are treated
    as requiring computer use.
    """

    if not isinstance(data, Mapping):
        return False
    if _coerce_bool(data.get("requires_computer")):
        return True
    browser_target = data.get("browser_target")
    if (
        isinstance(browser_target, str)
        and browser_target.strip() == "assistant_desktop"
    ):
        return True
    return False


def resolve_task_resource_requirements(
    data: Mapping[str, Any] | None,
) -> tuple[bool, bool]:
    """Return ``(requires_filesystem, requires_computer)`` for one task/activation."""

    return resolve_requires_filesystem(data), resolve_requires_computer(data)


def desktop_required_for_resources(
    *,
    requires_filesystem: bool,
    requires_computer: bool,
) -> bool:
    """Return whether infra must bind a desktop for the requested resources.

    Assistant Local lives on the desktop workspace today, so filesystem
    readiness still requires a desktop binding even when computer-use is off.
    """

    return bool(requires_filesystem or requires_computer)
