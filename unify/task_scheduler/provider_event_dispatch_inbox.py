"""Provider-event dispatch inbox for Unity live execution.

# TODO: Purge this module once Orchestra ``provider_event_dispatches.downstream_adoption_*``
is the cross-instance source of truth for live provider-event dispatch. This SQLite
inbox is container-local launch-claim machinery only — not shared across Unity
instances and not durable architecture.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

DispatchInboxState = Literal["adopted", "starting", "started", "terminal"]


@dataclass(frozen=True)
class LiveDispatchInboxSnapshot:
    """Authorization fields captured when one live dispatch operation is adopted."""

    run_key: str
    receipt_id: str
    accepted_activation_revision: str
    captured_task_revision: int


@dataclass(frozen=True)
class LiveDispatchInboxRecord:
    """One durable adoption record keyed by dispatch operation id."""

    operation_id: str
    run_id: int
    run_key: str
    receipt_id: str
    accepted_activation_revision: str
    captured_task_revision: int
    state: DispatchInboxState
    launch_count: int
    terminal_reason: str | None
    owns_start: bool = False


class ProviderEventInboxMismatchError(ValueError):
    """Raised when a retry presents different authorization for one operation."""


class ProviderEventLiveDispatchInbox:
    """Container-local SQLite inbox for owner-only live task-instance launch.

    # TODO: Delete this class once Orchestra owns cross-instance adoption state
    for provider-event dispatch operations (downstream adoption convergence).
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
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
                        run_key TEXT NOT NULL DEFAULT '',
                        receipt_id TEXT NOT NULL DEFAULT '',
                        accepted_activation_revision TEXT NOT NULL DEFAULT '',
                        captured_task_revision INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        launch_count INTEGER NOT NULL DEFAULT 0,
                        terminal_reason TEXT
                    )
                    """,
                )
                existing_columns = {
                    row["name"]
                    for row in connection.execute(
                        "PRAGMA table_info(provider_event_live_dispatch_inbox)",
                    )
                }
                for column_name, ddl in (
                    (
                        "run_key",
                        "ALTER TABLE provider_event_live_dispatch_inbox "
                        "ADD COLUMN run_key TEXT NOT NULL DEFAULT ''",
                    ),
                    (
                        "receipt_id",
                        "ALTER TABLE provider_event_live_dispatch_inbox "
                        "ADD COLUMN receipt_id TEXT NOT NULL DEFAULT ''",
                    ),
                    (
                        "accepted_activation_revision",
                        "ALTER TABLE provider_event_live_dispatch_inbox "
                        "ADD COLUMN accepted_activation_revision "
                        "TEXT NOT NULL DEFAULT ''",
                    ),
                    (
                        "terminal_reason",
                        "ALTER TABLE provider_event_live_dispatch_inbox "
                        "ADD COLUMN terminal_reason TEXT",
                    ),
                ):
                    if column_name not in existing_columns:
                        connection.execute(ddl)
                connection.commit()

    def _record_from_row(self, row: sqlite3.Row) -> LiveDispatchInboxRecord:
        return LiveDispatchInboxRecord(
            operation_id=row["operation_id"],
            run_id=int(row["run_id"]),
            run_key=row["run_key"],
            receipt_id=row["receipt_id"],
            accepted_activation_revision=row["accepted_activation_revision"],
            captured_task_revision=int(row["captured_task_revision"]),
            state=row["state"],
            launch_count=int(row["launch_count"]),
            terminal_reason=row["terminal_reason"],
        )

    def _assert_snapshot_matches(
        self,
        *,
        row: sqlite3.Row,
        run_id: int,
        snapshot: LiveDispatchInboxSnapshot,
    ) -> None:
        if (
            int(row["run_id"]) != run_id
            or row["run_key"] != snapshot.run_key
            or row["receipt_id"] != snapshot.receipt_id
            or row["accepted_activation_revision"]
            != snapshot.accepted_activation_revision
            or int(row["captured_task_revision"]) != snapshot.captured_task_revision
        ):
            raise ProviderEventInboxMismatchError(
                "provider_event_dispatch_inbox_authorization_mismatch",
            )

    def adopt_or_get(
        self,
        *,
        operation_id: str,
        run_id: int,
        snapshot: LiveDispatchInboxSnapshot,
    ) -> LiveDispatchInboxRecord:
        """Insert or return the durable inbox row for one live dispatch operation."""

        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO provider_event_live_dispatch_inbox (
                        operation_id,
                        run_id,
                        run_key,
                        receipt_id,
                        accepted_activation_revision,
                        captured_task_revision,
                        state,
                        launch_count
                    ) VALUES (?, ?, ?, ?, ?, ?, 'adopted', 0)
                    ON CONFLICT(operation_id) DO NOTHING
                    """,
                    (
                        operation_id,
                        run_id,
                        snapshot.run_key,
                        snapshot.receipt_id,
                        snapshot.accepted_activation_revision,
                        snapshot.captured_task_revision,
                    ),
                )
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                connection.commit()
        assert row is not None
        self._assert_snapshot_matches(row=row, run_id=run_id, snapshot=snapshot)
        return self._record_from_row(row)

    def claim_start(self, *, operation_id: str) -> LiveDispatchInboxRecord:
        """Atomically claim start ownership for one adopted operation."""

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown operation_id: {operation_id}")
                if row["state"] in {"started", "terminal"}:
                    return self._record_from_row(row)
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
                        SELECT operation_id, run_id, run_key, receipt_id,
                               accepted_activation_revision,
                               captured_task_revision, state, launch_count,
                               terminal_reason
                        FROM provider_event_live_dispatch_inbox
                        WHERE operation_id = ?
                        """,
                        (operation_id,),
                    ).fetchone()
                    assert row is not None
                    return LiveDispatchInboxRecord(
                        **{
                            **self._record_from_row(row).__dict__,
                            "owns_start": False,
                        },
                    )
                connection.commit()
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                assert row is not None
                return LiveDispatchInboxRecord(
                    **{
                        **self._record_from_row(row).__dict__,
                        "owns_start": True,
                    },
                )

    def start_if_owner(self, *, operation_id: str) -> LiveDispatchInboxRecord:
        """Start exactly one live task instance for an adopted operation."""

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown operation_id: {operation_id}")
                if row["state"] == "started":
                    return self._record_from_row(row)
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
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                assert row is not None
                return self._record_from_row(row)

    def get(self, *, operation_id: str) -> LiveDispatchInboxRecord | None:
        """Return the durable inbox row for one dispatch operation."""

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
        if row is None:
            return None
        return self._record_from_row(row)

    def mark_terminal(
        self,
        *,
        operation_id: str,
        reason: str,
    ) -> LiveDispatchInboxRecord:
        """Record a terminal inbox outcome for one dispatch operation."""

        with self._lock:
            with self._connect() as connection:
                updated = connection.execute(
                    """
                    UPDATE provider_event_live_dispatch_inbox
                    SET state = 'terminal', terminal_reason = ?
                    WHERE operation_id = ?
                    """,
                    (reason, operation_id),
                )
                if updated.rowcount == 0:
                    raise KeyError(f"unknown operation_id: {operation_id}")
                connection.commit()
                row = connection.execute(
                    """
                    SELECT operation_id, run_id, run_key, receipt_id,
                           accepted_activation_revision, captured_task_revision,
                           state, launch_count, terminal_reason
                    FROM provider_event_live_dispatch_inbox
                    WHERE operation_id = ?
                    """,
                    (operation_id,),
                ).fetchone()
                assert row is not None
                return self._record_from_row(row)
