"""Provider-event dispatch inbox for Unity live execution.

Temporary SQLite-backed launch-claim store for the initial live provider-event
slice. It is container-local and not shared across Unity instances. Replace it
with Orchestra-backed downstream adoption once dispatch convergence is wired,
then remove this module and any callers that read local adoption state.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DispatchInboxState = Literal["adopted", "starting", "started", "terminal"]


@dataclass(frozen=True)
class LiveDispatchInboxRecord:
    """One durable adoption record keyed by dispatch operation id."""

    operation_id: str
    run_id: int
    captured_task_revision: int
    state: DispatchInboxState
    launch_count: int


class ProviderEventLiveDispatchInbox:
    """Container-local SQLite inbox for owner-only live task-instance launch.

    Interim implementation only. Delete once Orchestra owns cross-instance
    adoption state for provider-event dispatch operations.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _ensure_schema(self) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS provider_event_live_dispatch_inbox (
                        operation_id TEXT PRIMARY KEY,
                        run_id INTEGER NOT NULL,
                        captured_task_revision INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        launch_count INTEGER NOT NULL DEFAULT 0
                    )
                    """,
                )
                connection.commit()

    def adopt_or_get(
        self,
        *,
        operation_id: str,
        run_id: int,
        captured_task_revision: int,
    ) -> LiveDispatchInboxRecord:
        """Insert or return the durable inbox row for one live dispatch operation."""

        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO provider_event_live_dispatch_inbox (
                        operation_id, run_id, captured_task_revision, state, launch_count
                    ) VALUES (?, ?, ?, 'adopted', 0)
                    ON CONFLICT(operation_id) DO NOTHING
                    """,
                    (operation_id, run_id, captured_task_revision),
                )
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, captured_task_revision, state, launch_count
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                connection.commit()
        assert row is not None
        return LiveDispatchInboxRecord(
            operation_id=row["operation_id"],
            run_id=row["run_id"],
            captured_task_revision=row["captured_task_revision"],
            state=row["state"],
            launch_count=row["launch_count"],
        )

    def claim_start(self, *, operation_id: str) -> LiveDispatchInboxRecord:
        """Atomically claim start ownership for one adopted operation."""

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, captured_task_revision, state, launch_count
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown operation_id: {operation_id}")
                if row["state"] == "started":
                    return LiveDispatchInboxRecord(
                        operation_id=row["operation_id"],
                        run_id=row["run_id"],
                        captured_task_revision=row["captured_task_revision"],
                        state="started",
                        launch_count=row["launch_count"],
                    )
                updated = connection.execute(
                    """
                    UPDATE provider_event_live_dispatch_inbox
                    SET state = 'starting'
                    WHERE operation_id = ? AND state = 'adopted'
                    """,
                    (operation_id,),
                )
                if updated.rowcount == 0:
                    row = connection.execute(
                        """
                        SELECT operation_id, run_id, captured_task_revision, state, launch_count
                        FROM provider_event_live_dispatch_inbox
                        WHERE operation_id = ?
                        """,
                        (operation_id,),
                    ).fetchone()
                    assert row is not None
                    return LiveDispatchInboxRecord(
                        operation_id=row["operation_id"],
                        run_id=row["run_id"],
                        captured_task_revision=row["captured_task_revision"],
                        state=row["state"],
                        launch_count=row["launch_count"],
                    )
                connection.commit()
                return LiveDispatchInboxRecord(
                    operation_id=row["operation_id"],
                    run_id=row["run_id"],
                    captured_task_revision=row["captured_task_revision"],
                    state="starting",
                    launch_count=row["launch_count"],
                )

    def start_if_owner(self, *, operation_id: str) -> LiveDispatchInboxRecord:
        """Start exactly one live task instance for an adopted operation."""

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, captured_task_revision, state, launch_count
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown operation_id: {operation_id}")
                if row["state"] == "started":
                    return LiveDispatchInboxRecord(
                        operation_id=row["operation_id"],
                        run_id=row["run_id"],
                        captured_task_revision=row["captured_task_revision"],
                        state="started",
                        launch_count=row["launch_count"],
                    )
                if row["state"] != "starting":
                    raise RuntimeError(
                        f"operation {operation_id} is not owned for start",
                    )
                launch_count = int(row["launch_count"]) + 1
                connection.execute(
                    """
                    UPDATE provider_event_live_dispatch_inbox
                    SET state = 'started', launch_count = ?
                    WHERE operation_id = ?
                    """,
                    (launch_count, operation_id),
                )
                connection.commit()
                return LiveDispatchInboxRecord(
                    operation_id=row["operation_id"],
                    run_id=row["run_id"],
                    captured_task_revision=row["captured_task_revision"],
                    state="started",
                    launch_count=launch_count,
                )
