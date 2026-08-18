"""Verification types shared by the function store and the actor runtime.

A compositional function earns trust: verifier passes record verdicts, the
ledger folds them, and a deterministic policy derives ``Function.verify``
from that evidence. These models are the vocabulary of that ledger.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class SideEffectClass(StrEnum):
    """Effect class of a function, ordered from harmless to irreversible."""

    safe_noop = "safe_noop"
    read_only = "read_only"
    idempotent_effectful = "idempotent_effectful"
    unsafe_effectful = "unsafe_effectful"

    @property
    def rank(self) -> int:
        return _EFFECT_RANK[self]

    @property
    def is_effectful(self) -> bool:
        return self in {
            SideEffectClass.idempotent_effectful,
            SideEffectClass.unsafe_effectful,
        }

    @classmethod
    def max_of(cls, *classes: "SideEffectClass") -> "SideEffectClass":
        """Return the most dangerous class among ``classes`` (``safe_noop`` when empty)."""
        best = cls.safe_noop
        for candidate in classes:
            if candidate.rank > best.rank:
                best = candidate
        return best


_EFFECT_RANK: Dict[SideEffectClass, int] = {
    SideEffectClass.safe_noop: 0,
    SideEffectClass.read_only: 1,
    SideEffectClass.idempotent_effectful: 2,
    SideEffectClass.unsafe_effectful: 3,
}

ClassSource = Literal["pure", "primitives", "inferred_third_party", "librarian"]


class VerdictKind(StrEnum):
    """Which verification pass produced a verdict."""

    static = "static"
    args = "args"
    precondition = "precondition"
    post = "post"
    tier0 = "tier0"
    spot_check = "spot_check"


VerdictValue = Literal["PASS", "FAIL", "UNSURE"]
Fault = Literal["leaf", "caller"]


class _FaultRequiredOnFail(BaseModel):
    """Shared rule: a FAIL must name who is at fault so repair can target it."""

    @model_validator(mode="after")
    def _fault_required_on_fail(self):
        if (
            getattr(self, "verdict", None) == "FAIL"
            and getattr(self, "fault", None) is None
        ):
            raise ValueError("A FAIL verdict must name the fault: 'leaf' or 'caller'.")
        return self


class Verdict(_FaultRequiredOnFail):
    """Outcome of one verification pass on one call."""

    verdict: VerdictValue
    reason: str = ""
    fault: Optional[Fault] = None


class VerificationPolicy(BaseModel):
    """Librarian-settable overrides. Every knob can only raise the bar."""

    always_verify: bool = False
    required_passes: Optional[int] = None
    min_distinct_inputs: Optional[int] = None
    fixture_only: bool = False
    spot_check_rate: Optional[float] = None


class VerificationSummary(BaseModel):
    """Fold of the ``Functions/Verifications`` rows for ``verified_hash``."""

    passes: Dict[str, int] = Field(default_factory=dict)
    fails: int = 0
    unsure: int = 0
    distinct_args_signatures: List[str] = Field(default_factory=list)
    last_verdict_at: Optional[datetime] = None
    spot_checks: int = 0

    def pass_count(self, kind: VerdictKind | str) -> int:
        return int(self.passes.get(str(kind), 0))


MAX_DISTINCT_ARGS_SIGNATURES = 32


class StaticReviewRecord(BaseModel):
    """Cached static-review verdict for one ``verified_hash``."""

    verdict: VerdictValue
    reason: str = ""
    function_hash: str
    reviewed_at: Optional[datetime] = None
    model: Optional[str] = None


class FunctionContract(BaseModel):
    """Tier-0 contract: JSON schemas from type hints plus authored postconditions."""

    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    postconditions: List[str] = Field(default_factory=list)
    source: Literal["type_hints", "librarian", "none"] = "none"


class Fixture(BaseModel):
    """A recorded (args, result) pair a ``safe_noop`` function must reproduce."""

    args: Dict[str, Any] = Field(default_factory=dict)
    result: Any = None
    args_signature: str
    captured_at: Optional[datetime] = None
    run_key: Optional[str] = None


class VerificationRow(_FaultRequiredOnFail):
    """One append-only row in ``Functions/Verifications``."""

    function_id: int
    function_hash: Optional[str] = None
    kind: VerdictKind
    verdict: VerdictValue
    reason: str = ""
    fault: Optional[Fault] = None
    call_site: str = "root"
    args_signature: Optional[str] = None
    run_key: Optional[str] = None
    task_id: Optional[int] = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost: float = 0.0
    wall_ms: int = 0
    created_at: Optional[datetime] = None

    @field_validator("reason", mode="before")
    @classmethod
    def _cap_reason(cls, value):
        text = "" if value is None else str(value)
        return text[:2000]
