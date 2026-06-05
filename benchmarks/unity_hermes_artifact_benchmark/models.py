"""Shared models for the Unity vs Hermes artifact benchmark.

The benchmark is intentionally dry-run only. These models describe the inputs,
artifact inspection results, and orchestration traces we want to compare across
systems without touching a live mailbox.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Literal

EmailCategory = Literal[
    "urgent_action",
    "needs_reply",
    "meeting_or_calendar",
    "fyi",
    "newsletter",
    "ignore",
]

BenchmarkPhase = Literal["first_run", "consolidation", "repeat_run"]


class ArtifactKind(str, Enum):
    """Reusable artifact shapes the benchmark knows how to score."""

    UNITY_FUNCTION = "unity_function"
    HERMES_SKILL_WITH_SCRIPT = "hermes_skill_with_script"
    HERMES_NO_AGENT_SCRIPT = "hermes_no_agent_script"
    PROMPT_ONLY = "prompt_only"


@dataclass(frozen=True)
class EmailMessage:
    message_id: str
    received_at: str
    from_email: str
    from_name: str
    subject: str
    body: str
    thread: list[dict[str, str]] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExpectedEmailOutcome:
    message_id: str
    category: EmailCategory
    needs_reply: bool
    draft_reply: str | None
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EmailBatch:
    """A fixed synthetic inbox slice for a single benchmark run."""

    batch_id: str
    since: str
    description: str
    emails: tuple[EmailMessage, ...]
    expected: tuple[ExpectedEmailOutcome, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "since": self.since,
            "description": self.description,
            "emails": [email.to_dict() for email in self.emails],
            "expected": [outcome.to_dict() for outcome in self.expected],
        }


@dataclass(frozen=True)
class OutputContract:
    """Filesystem contract for dry-run benchmark outputs."""

    root_pattern: str
    corpus_file: PurePosixPath
    drafts_file: PurePosixPath
    artifact_file: PurePosixPath
    trace_file: PurePosixPath
    report_file: PurePosixPath

    def to_dict(self) -> dict[str, str]:
        return {
            "root_pattern": self.root_pattern,
            "corpus_file": str(self.corpus_file),
            "drafts_file": str(self.drafts_file),
            "artifact_file": str(self.artifact_file),
            "trace_file": str(self.trace_file),
            "report_file": str(self.report_file),
        }


@dataclass(frozen=True)
class PromptSet:
    first_run: str
    consolidation: str
    repeat_run: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class InspectionChecklist:
    items: tuple[str, ...]

    def to_dict(self) -> dict[str, list[str]]:
        return {"items": list(self.items)}


@dataclass(frozen=True)
class ArtifactObservation:
    """Observed reusable artifact emitted by one benchmark arm."""

    arm_id: str
    name: str
    kind: ArtifactKind
    entrypoint: str | None
    invocation_path: tuple[str, ...]
    has_stable_input_schema: bool
    has_stable_output_schema: bool
    has_dry_run_mode: bool
    semantic_calls_inside_artifact: bool
    cheap_semantic_model: str | None
    scheduler_binding: str | None
    requires_procedural_prompt_reread: bool
    exposes_supporting_script_directly: bool
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["kind"] = self.kind.value
        data["invocation_path"] = list(self.invocation_path)
        return data


@dataclass(frozen=True)
class TraceEvent:
    phase: BenchmarkPhase
    action: str
    detail: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    tool_call: bool = False

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RunTrace:
    arm_id: str
    seed: int
    batch_id: str
    events: tuple[TraceEvent, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm_id": self.arm_id,
            "seed": self.seed,
            "batch_id": self.batch_id,
            "events": [event.to_dict() for event in self.events],
        }


@dataclass(frozen=True)
class ArtifactQualityScore:
    arm_id: str
    artifact_name: str
    direct_invocability: float
    boundary_clarity: float
    semantic_isolation: float
    future_run_autonomy: float
    scheduler_readiness: float
    total: float
    rationale: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TraceMeasurement:
    arm_id: str
    seed: int
    batch_id: str
    total_tokens: int
    first_run_tokens: int
    consolidation_tokens: int
    repeat_run_tokens: int
    repeat_orchestration_tokens: int
    tool_calls_before_execution: int
    first_execution_action: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkResult:
    arm_id: str
    artifact: ArtifactObservation
    score: ArtifactQualityScore
    traces: tuple[RunTrace, ...]
    measurements: tuple[TraceMeasurement, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm_id": self.arm_id,
            "artifact": self.artifact.to_dict(),
            "score": self.score.to_dict(),
            "traces": [trace.to_dict() for trace in self.traces],
            "measurements": [
                measurement.to_dict() for measurement in self.measurements
            ],
        }
