"""The execution claim that keeps a redelivered invocation from running twice.

The dispatch event is delivered at least once, and the failure this guards is
concrete: two deliveries of one bulk-send both observing `pending` and both
executing, so the recipients get the email twice while the audit trail shows
one run. The claim is an atomic compare-and-set, so exactly one delivery wins.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from unify.canvas_manager.canvas_manager import STALE_CLAIM_SECONDS, CanvasManager
from unify.canvas_manager.types.action import CanvasInvocationRecord


class _ClaimDataManager:
    """Fake DM exposing the atomic compare-and-set the claim rides on."""

    def __init__(self, *, wins: bool = True):
        self.wins = wins
        self.claim_calls: List[Dict[str, Any]] = []

    def claim(
        self,
        context: str,
        *,
        expect: Dict[str, Any],
        updates: Dict[str, Any],
        limit: int = 1,
    ) -> List[Dict[str, Any]]:
        self.claim_calls.append(
            {"context": context, "expect": dict(expect), "updates": dict(updates)},
        )
        return [{**expect, **updates}] if self.wins else []


def _manager(dm: _ClaimDataManager) -> CanvasManager:
    manager = CanvasManager()
    manager._get_dm = lambda: dm  # noqa: SLF001 -- the seam under test
    return manager


def _record(**overrides: Any) -> CanvasInvocationRecord:
    base: Dict[str, Any] = {
        "canvas_token": "tok123",
        "action_name": "bulk_send",
        "status": "pending",
        "invocation_id": 0,
    }
    base.update(overrides)
    return CanvasInvocationRecord.model_validate(base)


class TestClaim:
    def test_the_winning_delivery_gets_a_nonce(self):
        dm = _ClaimDataManager(wins=True)
        nonce = _manager(dm)._claim_invocation("ctx", _record())
        assert nonce is not None
        updates = dm.claim_calls[0]["updates"]
        assert updates["claim_key"] == nonce
        assert updates["status"] == "running"
        assert updates["claimed_at"]

    def test_a_losing_delivery_stands_down(self):
        # The compare-and-set matched nothing: another delivery already moved
        # the row. Standing down is what stops the second send.
        dm = _ClaimDataManager(wins=False)
        assert _manager(dm)._claim_invocation("ctx", _record()) is None

    def test_the_claim_expects_exactly_the_observed_state(self):
        """A claim must name the row and the state it saw.

        Expecting the id alone would let a claim land on a row another
        delivery had already moved to `running`, which is the double-execution
        this exists to prevent.
        """
        dm = _ClaimDataManager(wins=True)
        _manager(dm)._claim_invocation("ctx", _record())
        expect = dm.claim_calls[0]["expect"]
        assert expect == {
            "canvas_token": "tok123",
            "invocation_id": 0,
            "status": "pending",
        }

    def test_reclaiming_a_stale_run_fences_on_the_dead_claim(self):
        dm = _ClaimDataManager(wins=True)
        record = _record(status="running", claim_key="dead")
        _manager(dm)._claim_invocation("ctx", record)
        assert dm.claim_calls[0]["expect"]["claim_key"] == "dead"


class TestStaleness:
    @pytest.fixture
    def manager(self):
        return CanvasManager()

    def test_a_fresh_claim_is_live(self, manager):
        record = _record(
            status="running",
            claimed_at=datetime.now(timezone.utc).isoformat(),
        )
        assert manager._claim_is_stale(record) is False

    def test_an_expired_claim_may_be_taken_over(self, manager):
        stale = datetime.now(timezone.utc) - timedelta(
            seconds=STALE_CLAIM_SECONDS + 1,
        )
        record = _record(status="running", claimed_at=stale.isoformat())
        assert manager._claim_is_stale(record) is True

    def test_a_running_row_without_a_claim_is_not_wedged_forever(self, manager):
        # Rows written before claims existed have no `claimed_at`; treating
        # them as live would leave them `running` until manually edited.
        assert manager._claim_is_stale(_record(status="running")) is True
