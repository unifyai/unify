"""Two provider events sharing an identity-hmac prefix must not share a run key.

The run key is the create-or-adopt idempotency token. Collapse two distinct
events onto one key and the second adopts the first's execution instead of
minting its own — the event is accepted, acknowledged, and silently never run.
Truncating the identity digest the way the revision digest is truncated is
exactly what would collapse them, so the digest is embedded in full.
"""

from __future__ import annotations

import json
from pathlib import Path

from unify.task_scheduler.offline_runner_contract import build_provider_event_run_key

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "fixtures"
    / "task_trigger_contract"
    / "provider_event_run_key_collision.json"
)


def _fixture() -> dict:
    return json.loads(_FIXTURE_PATH.read_text(encoding="utf-8"))


def test_shared_prefix_identities_produce_distinct_run_keys() -> None:
    fixture = _fixture()
    first_hmac, second_hmac = fixture["shared_prefix_hmacs"]
    shared = {
        "assistant_id": fixture["assistant_id"],
        "task_id": fixture["task_id"],
        "binding_id": fixture["binding_id"],
        "revision": fixture["revision"],
    }

    first = build_provider_event_run_key(event_identity_hmac=first_hmac, **shared)
    second = build_provider_event_run_key(event_identity_hmac=second_hmac, **shared)

    assert first != second
    assert first.endswith(f":{first_hmac}")
    assert second.endswith(f":{second_hmac}")


def test_fixture_identities_overlap_past_the_truncation_widths() -> None:
    """The case only bites while the two identities share a leading run of hex.

    Both digests in this builder are cut to 12 hex, so an overlap shorter than
    that would let the assertion above pass against a truncating implementation.
    """

    first_hmac, second_hmac = _fixture()["shared_prefix_hmacs"]

    assert first_hmac != second_hmac
    assert first_hmac[:16] == second_hmac[:16]
