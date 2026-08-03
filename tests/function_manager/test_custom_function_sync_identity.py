"""The function/venv reconcile must never renumber rows it re-syncs.

Task entrypoints hold ``function_id`` and function rows hold ``venv_id``, so
a delete-and-reinsert on reconcile orphans every reference. Two behaviors pin
that: legacy rows whose ``custom_key`` column is null still land in the
managed index (matched by name), and same-named rows outside the index are
adopted in place rather than replaced.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import unify.function_manager.function_manager as fm_module
from unify.function_manager.function_manager import (
    _FunctionSyncAdapter,
    _VenvSyncAdapter,
)


def _fake_log(entries: Dict[str, Any], log_id: int = 1) -> SimpleNamespace:
    return SimpleNamespace(id=log_id, entries=entries)


def _manager_stub() -> SimpleNamespace:
    return SimpleNamespace(
        _compositional_ctx="ctx/Functions/Compositional",
        _venvs_ctx="ctx/Functions/VirtualEnvs",
    )


def test_live_rows_defaults_null_custom_key_to_name(monkeypatch):
    rows = [
        _fake_log(
            {
                "function_id": 1,
                "name": "run_gtm_stargazer_enrich_tick",
                "custom_key": None,
                "custom_hash": "hash-enrich",
            },
        ),
    ]
    monkeypatch.setattr(
        fm_module.unisdk,
        "get_logs",
        lambda **kwargs: rows,
    )
    monkeypatch.setattr(
        fm_module,
        "list_private_fields",
        lambda ctx: [],
    )
    adapter = _FunctionSyncAdapter(_manager_stub(), venv_name_to_id={})
    live = adapter.live_rows()
    assert live[0]["custom_key"] == "run_gtm_stargazer_enrich_tick"


def test_find_adoptable_claims_same_named_row(monkeypatch):
    existing = _fake_log(
        {"function_id": 29, "name": "run_gtm_stargazer_enrich_tick"},
    )
    captured: Dict[str, Any] = {}

    def fake_get_logs(**kwargs: Any) -> List[SimpleNamespace]:
        captured.update(kwargs)
        return [existing]

    monkeypatch.setattr(fm_module.unisdk, "get_logs", fake_get_logs)
    adapter = _FunctionSyncAdapter(_manager_stub(), venv_name_to_id={})
    row = adapter.find_adoptable(
        "run_gtm_stargazer_enrich_tick",
        {"name": "run_gtm_stargazer_enrich_tick"},
    )
    assert row["function_id"] == 29
    assert "run_gtm_stargazer_enrich_tick" in captured["filter"]


def test_adopt_updates_in_place_preserving_function_id():
    updates: List[Dict[str, Any]] = []
    manager = _manager_stub()
    manager._update_custom_function = lambda *, function_id, data: updates.append(
        {"function_id": function_id, **data},
    )
    adapter = _FunctionSyncAdapter(manager, venv_name_to_id={})
    adapter.adopt(
        "run_gtm_stargazer_enrich_tick",
        {"function_id": 29, "name": "run_gtm_stargazer_enrich_tick"},
        {"name": "run_gtm_stargazer_enrich_tick", "custom_hash": "h"},
    )
    assert updates[0]["function_id"] == 29
    assert updates[0]["custom_hash"] == "h"


def test_venv_adapter_adopts_same_named_row(monkeypatch):
    existing = _fake_log({"venv_id": 7, "name": "scraping"})
    monkeypatch.setattr(fm_module.unisdk, "get_logs", lambda **kwargs: [existing])
    adapter = _VenvSyncAdapter(_manager_stub())
    row = adapter.find_adoptable("scraping", {"name": "scraping"})
    assert row["venv_id"] == 7
