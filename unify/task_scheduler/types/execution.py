"""Canonical Execution model for the minimal task identity surface.

Two durable Orchestra surfaces:

- ``Tasks`` — definition only (series), keyed by ``task_id``
- ``Tasks/Executions`` — one wake/attempt, unique ``run_key`` (idempotency key)

Occurrence and attempt are the same row. There is no ``Tasks/Activations`` and
no ``instance_id`` on the execution ledger. Recurrence creates the *next*
Execution when the current one **starts** (not a Task-row clone).
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "Delivery",
    "ExecutionState",
    "Wake",
    "EXECUTION_CONTEXT_LEAF",
    "EXECUTIONS_CONTEXT_NAME",
]


EXECUTION_CONTEXT_LEAF = "Executions"
EXECUTIONS_CONTEXT_NAME = f"Tasks/{EXECUTION_CONTEXT_LEAF}"


class Delivery(StrEnum):
    """Where an execution runs (replaces Task.offline + execution_mode)."""

    live = "live"
    offline = "offline"

    @classmethod
    def from_offline_flag(cls, offline: bool | None) -> Delivery:
        return cls.offline if offline else cls.live

    @classmethod
    def normalize(cls, value: str | Delivery | None) -> Delivery:
        if isinstance(value, cls):
            return value
        try:
            return cls((value or cls.live).strip().lower())
        except ValueError:
            return cls.live


class Wake(StrEnum):
    """Why an execution exists (replaces source_type / activation_kind / activated_by)."""

    scheduled = "scheduled"
    triggered = "triggered"
    explicit = "explicit"
    provider_event = "provider_event"

    @classmethod
    def normalize(cls, value: str | Wake | None) -> Wake:
        if isinstance(value, cls):
            return value
        try:
            return cls((value or cls.explicit).strip().lower())
        except ValueError:
            return cls.explicit


class ExecutionState(StrEnum):
    """Lifecycle of one ``Tasks/Executions`` row."""

    scheduled = "scheduled"
    triggerable = "triggerable"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"

    @classmethod
    def normalize(cls, value: str | ExecutionState | None) -> ExecutionState:
        if isinstance(value, cls):
            return value
        try:
            return cls((value or cls.scheduled).strip().lower())
        except ValueError:
            return cls.scheduled

    @property
    def is_open(self) -> bool:
        """Return True when the execution is still waiting or in flight."""

        return self in {
            ExecutionState.scheduled,
            ExecutionState.triggerable,
            ExecutionState.running,
        }

    @property
    def is_terminal(self) -> bool:
        return self in {
            ExecutionState.completed,
            ExecutionState.failed,
            ExecutionState.cancelled,
        }
