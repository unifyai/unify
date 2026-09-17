"""Filter-path update_rows must not re-send immutable authoring fields."""

from __future__ import annotations

from unify.common.authorship import AUTHORING_ASSISTANT_ID_FIELD
from unify.data_manager.ops import mutation_ops

_CONTEXT = "user/11/Data/GTM/OutboundBindings"


def test_filter_update_sends_only_the_callers_columns(monkeypatch) -> None:
    """The filter path must never round-trip the row it read.

    The store merges partial payloads and rejects any update that names an
    immutable field even with an unchanged value — so re-sending the row's
    own columns (auto-counted identity keys, authorship) turns an ordinary
    state update into a rejected write. This is the failure that left every
    ingestion run stuck at `queued`.
    """

    captured: list[dict] = []

    class _Log:
        id = 101
        entries = {
            "binding_key": "stargazer-v1",
            "campaign_slug": "stargazer-v1",
            AUTHORING_ASSISTANT_ID_FIELD: 1406,
        }

    def fake_get_logs(*, context, filter):
        assert context == _CONTEXT
        assert "stargazer-v1" in filter
        return [_Log()]

    def fake_update_logs(*, logs, context, entries, overwrite):
        captured.append(
            {
                "logs": logs,
                "context": context,
                "entries": dict(entries),
                "overwrite": overwrite,
            },
        )

    monkeypatch.setattr(mutation_ops.db, "get_logs", fake_get_logs)
    monkeypatch.setattr(mutation_ops.db, "update_logs", fake_update_logs)

    updated = mutation_ops.update_rows_impl(
        _CONTEXT,
        {
            "binding_hash": "abc123",
            "campaign_slug": "stargazer-v1",
            AUTHORING_ASSISTANT_ID_FIELD: 999,
        },
        filter="binding_key == 'stargazer-v1'",
    )

    assert updated == 1
    assert len(captured) == 1
    payload = captured[0]
    assert payload["logs"] == [101]
    assert payload["overwrite"] is True
    assert AUTHORING_ASSISTANT_ID_FIELD not in payload["entries"]
    assert payload["entries"]["binding_hash"] == "abc123"
    assert payload["entries"]["campaign_slug"] == "stargazer-v1"
    # The row's own columns stay in the store; only the update is sent.
    assert "binding_key" not in payload["entries"]


def test_log_id_update_strips_authoring_from_delta_payload(monkeypatch) -> None:
    """log_ids path sends only the update delta, still without authorship."""

    captured: list[dict] = []

    def fake_update_logs(*, logs, context, entries, overwrite):
        captured.append(
            {"logs": logs, "entries": dict(entries), "overwrite": overwrite},
        )

    monkeypatch.setattr(mutation_ops.db, "update_logs", fake_update_logs)

    updated = mutation_ops.update_rows_impl(
        _CONTEXT,
        {"binding_hash": "xyz", AUTHORING_ASSISTANT_ID_FIELD: 7},
        log_ids=[55],
        overwrite=True,
    )

    assert updated == 1
    assert captured[0]["logs"] == [55]
    assert AUTHORING_ASSISTANT_ID_FIELD not in captured[0]["entries"]
    assert captured[0]["entries"] == {"binding_hash": "xyz"}
