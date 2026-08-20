"""Fixtures: recorded (args, result) pairs a ``safe_noop`` function must reproduce.

Captured on calls whose verdicts passed, hand-authored by a librarian, and
replayed whenever the function's trust hash changes so a repair or edit
cannot silently change what a pure function computes.
"""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, List, Mapping, Optional

from ..types.verification import Fixture
from .ledger import args_signature


class FixtureRegressionError(ValueError):
    """A new implementation no longer reproduces a recorded fixture."""

    def __init__(self, function_name: str, fixture: Fixture, actual: Any) -> None:
        self.function_name = function_name
        self.fixture = fixture
        self.actual = actual
        super().__init__(
            f"Function {function_name!r} no longer reproduces a recorded fixture: "
            f"for args {canonical_json(fixture.args)} it returned "
            f"{canonical_json(actual)} instead of {canonical_json(fixture.result)}.",
        )


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, ensure_ascii=False)


def results_equal(expected: Any, actual: Any) -> bool:
    return canonical_json(expected) == canonical_json(actual)


def make_fixture(
    *,
    args: Mapping[str, Any],
    result: Any,
    max_bytes: int,
    run_key: Optional[str] = None,
) -> Optional[Fixture]:
    """Build a fixture, or None when the pair is not JSON-serialisable within ``max_bytes``."""
    try:
        payload = canonical_json({"args": dict(args), "result": result})
    except (TypeError, ValueError):
        return None
    if len(payload.encode("utf-8")) > max_bytes:
        return None
    return Fixture(
        args=json.loads(canonical_json(dict(args))),
        result=json.loads(canonical_json(result)),
        args_signature=args_signature(args),
        captured_at=datetime.now(timezone.utc),
        run_key=run_key,
    )


def add_fixture(
    existing: Iterable[Mapping[str, Any]],
    fixture: Fixture,
    *,
    cap: int,
) -> List[dict]:
    """Return ``existing`` with ``fixture`` merged in: distinct by signature, oldest evicted."""
    fixtures = [Fixture.model_validate(dict(item)) for item in existing]
    fixtures = [
        item for item in fixtures if item.args_signature != fixture.args_signature
    ]
    fixtures.append(fixture)
    while len(fixtures) > max(cap, 0):
        fixtures.pop(0)
    return [item.model_dump(mode="json") for item in fixtures]


def coerce_fixtures(
    items: Iterable[Mapping[str, Any] | Fixture],
    *,
    max_bytes: int,
) -> List[Fixture]:
    """Validate librarian-authored fixtures; each needs ``args`` and ``result``."""
    fixtures: List[Fixture] = []
    for item in items:
        if isinstance(item, Fixture):
            candidate = item
        else:
            data = dict(item)
            if "args" not in data or "result" not in data:
                raise ValueError("Each fixture needs 'args' and 'result'.")
            candidate = make_fixture(
                args=dict(data["args"]),
                result=data["result"],
                max_bytes=max_bytes,
                run_key=data.get("run_key"),
            )
            if candidate is None:
                raise ValueError(
                    f"Fixture for args {canonical_json(data['args'])} is not "
                    f"JSON-serialisable within {max_bytes} bytes.",
                )
        fixtures.append(candidate)
    return fixtures


async def replay_fixtures(
    fixtures: Iterable[Fixture],
    call: Callable[..., Any | Awaitable[Any]],
    *,
    function_name: str,
) -> int:
    """Run every fixture through ``call``; raise ``FixtureRegressionError`` on the first mismatch.

    Returns the number of fixtures replayed.
    """
    count = 0
    for fixture in fixtures:
        outcome = call(**fixture.args)
        if inspect.isawaitable(outcome):
            outcome = await outcome
        if not results_equal(fixture.result, outcome):
            raise FixtureRegressionError(function_name, fixture, outcome)
        count += 1
    return count
