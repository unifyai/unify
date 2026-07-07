from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _seed_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "seed_builtins_catalog.py"
    spec = importlib.util.spec_from_file_location("seed_builtins_catalog", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_force_override_defaults_to_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _seed_module()

    monkeypatch.delenv("UNITY_INTEGRATION_BOOTSTRAP_FORCE_OVERRIDE", raising=False)

    assert module._force_override_from_env() is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("on", True),
        ("false", False),
        ("off", False),
        ("manifest", None),
        ("auto", None),
    ],
)
def test_force_override_accepts_tristate_values(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    expected: bool | None,
) -> None:
    module = _seed_module()

    monkeypatch.setenv("UNITY_INTEGRATION_BOOTSTRAP_FORCE_OVERRIDE", value)

    assert module._force_override_from_env() is expected


def test_force_override_rejects_ambiguous_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _seed_module()

    monkeypatch.setenv("UNITY_INTEGRATION_BOOTSTRAP_FORCE_OVERRIDE", "sometimes")

    with pytest.raises(ValueError, match="UNITY_INTEGRATION_BOOTSTRAP_FORCE_OVERRIDE"):
        module._force_override_from_env()
