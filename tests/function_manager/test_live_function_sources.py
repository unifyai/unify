"""Live-source resolution for deployment-owned custom functions."""

import asyncio
import textwrap

from unify.function_manager import custom_functions as cf


def _write_module(directory, name, body):
    (directory / name).write_text(textwrap.dedent(body))


def test_resolve_live_custom_callable_prefers_registered_dirs(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(cf, "_LIVE_SOURCE_DIRS", [])
    monkeypatch.setattr(cf, "_LIVE_CALLABLE_CACHE", None)

    _write_module(
        tmp_path,
        "funcs.py",
        """
        from unify.function_manager.custom import custom_function

        MODULE_STATE = "from-live-module"

        @custom_function()
        async def live_fn() -> str:
            \"\"\"Return module state to prove the live module is bound.\"\"\"
            return MODULE_STATE
        """,
    )

    assert cf.resolve_live_custom_callable("live_fn") is None

    cf.register_live_function_source_dirs([tmp_path])
    fn = cf.resolve_live_custom_callable("live_fn")
    assert fn is not None
    assert asyncio.run(fn()) == "from-live-module"
    assert cf.resolve_live_custom_callable("missing_fn") is None


def test_later_registered_dirs_override_earlier(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "_LIVE_SOURCE_DIRS", [])
    monkeypatch.setattr(cf, "_LIVE_CALLABLE_CACHE", None)

    base = tmp_path / "base"
    override = tmp_path / "override"
    base.mkdir()
    override.mkdir()
    _write_module(
        base,
        "funcs.py",
        """
        from unify.function_manager.custom import custom_function

        @custom_function()
        async def shared_fn() -> str:
            \"\"\"Base variant.\"\"\"
            return "base"
        """,
    )
    _write_module(
        override,
        "funcs.py",
        """
        from unify.function_manager.custom import custom_function

        @custom_function()
        async def shared_fn() -> str:
            \"\"\"Override variant.\"\"\"
            return "override"
        """,
    )

    cf.register_live_function_source_dirs([base])
    assert asyncio.run(cf.resolve_live_custom_callable("shared_fn")()) == "base"

    # New registration invalidates the cache; later dirs win on collisions.
    cf.register_live_function_source_dirs([override])
    assert asyncio.run(cf.resolve_live_custom_callable("shared_fn")()) == "override"
