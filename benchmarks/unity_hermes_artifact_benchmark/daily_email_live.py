"""Live five-day recurring email benchmark for Unity vs Hermes.

This module drives the actual agent surfaces when requested. It is intentionally
separate from ``runner.py`` so the original deterministic reference benchmark
stays fast and stable.
"""

from __future__ import annotations

import argparse
import asyncio
import ast
import contextlib
import json
import os
import pprint
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .fixtures import workweek_email_batches

USER_REQUEST = (
    "Could you please check my emails every morning at 9am, and then draft "
    "basic replies to each of them?"
)
LIVE_BENCHMARK_DAYS = ("monday", "tuesday")


@dataclass
class LiveTurn:
    arm_id: str
    phase: str
    day: str | None
    prompt: str
    final_response: str
    elapsed_seconds: float
    message_count: int = 0
    tool_call_count: int = 0
    usage: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LiveBenchmarkResult:
    arm_id: str
    status: str
    workspace: str
    turns: list[LiveTurn]
    artifacts: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm_id": self.arm_id,
            "status": self.status,
            "workspace": self.workspace,
            "turns": [turn.to_dict() for turn in self.turns],
            "artifacts": self.artifacts,
            "usage": self.usage,
            "error": self.error,
        }


def _json_dump(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json_dump(data), encoding="utf-8")


def _sum_usage(turns: Iterable[LiveTurn]) -> dict[str, Any]:
    totals: dict[str, Any] = {
        "calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "cache_read_tokens": 0,
        "cache_write_tokens": 0,
        "reasoning_tokens": 0,
        "total_tokens": 0,
        "provider_cost_usd": 0.0,
        "billed_cost_usd": 0.0,
        "estimated_cost_usd": 0.0,
        "by_model": {},
    }
    for turn in turns:
        usage = turn.usage or {}
        for key in (
            "calls",
            "prompt_tokens",
            "completion_tokens",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "reasoning_tokens",
            "total_tokens",
        ):
            totals[key] += int(usage.get(key) or 0)
        for key in ("provider_cost_usd", "billed_cost_usd", "estimated_cost_usd"):
            totals[key] += float(usage.get(key) or 0.0)
        for model, row in (usage.get("by_model") or {}).items():
            dest = totals["by_model"].setdefault(
                model,
                {
                    "calls": 0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                    "provider_cost_usd": 0.0,
                    "billed_cost_usd": 0.0,
                    "estimated_cost_usd": 0.0,
                },
            )
            for key in (
                "calls",
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
            ):
                dest[key] += int(row.get(key) or 0)
            for key in ("provider_cost_usd", "billed_cost_usd", "estimated_cost_usd"):
                dest[key] += float(row.get(key) or 0.0)
    return totals


def _workweek_payload() -> dict[str, Any]:
    batches = _live_email_batches()
    visible_batches = {}
    for batch in batches:
        batch_dict = batch.to_dict()
        batch_dict.pop("expected", None)
        visible_batches[batch.batch_id] = batch_dict
    return {
        "days": [batch.batch_id.split("-", 1)[0] for batch in batches],
        "batches": visible_batches,
    }


def _live_email_batches() -> tuple[Any, ...]:
    return tuple(
        batch
        for batch in workweek_email_batches()
        if batch.batch_id.split("-", 1)[0] in LIVE_BENCHMARK_DAYS
    )


def prepare_workspace(root: Path) -> dict[str, str]:
    """Create a fake inbox workspace with a deterministic ``get_emails`` helper."""

    root.mkdir(parents=True, exist_ok=True)
    payload = _workweek_payload()
    _write_json(root / "emails_by_day.json", payload)
    (root / "drafts").mkdir(exist_ok=True)
    helper = f'''"""Synthetic inbox helper for the recurring email benchmark."""

from __future__ import annotations

EMAILS_BY_DAY = {pprint.pformat(payload["batches"], sort_dicts=True, width=100)}


def get_emails(day: str | None = None, since: str | None = None) -> list[dict]:
    """Return synthetic emails for a benchmark day.

    Args:
        day: Day name such as ``"monday"`` or batch id such as
            ``"monday-2026-06-01"``.
        since: Accepted for mailbox-like call signatures. It is not needed
            because this fixture is already partitioned by daily activation.
    """

    if day is None:
        raise ValueError("Pass day='monday' or day='tuesday' in this benchmark")
    normalized = day.lower().strip()
    if normalized in EMAILS_BY_DAY:
        return list(EMAILS_BY_DAY[normalized]["emails"])
    for batch_id, batch in EMAILS_BY_DAY.items():
        if batch_id.startswith(normalized):
            return list(batch["emails"])
    raise KeyError(f"No synthetic inbox batch for {{day!r}}")
'''
    (root / "email_fixture.py").write_text(helper, encoding="utf-8")
    return {
        "workspace": str(root),
        "emails_by_day": str(root / "emails_by_day.json"),
        "email_fixture": str(root / "email_fixture.py"),
        "drafts_dir": str(root / "drafts"),
    }


def _setup_prompt(workspace: Path, arm_id: str) -> str:
    return f"""\
You are running the live {arm_id} recurring-email benchmark.

User request:
{USER_REQUEST}

Benchmark constraints:
- Do not touch Gmail, Calendar, or any external mailbox.
- A fake inbox helper already exists at `{workspace / "email_fixture.py"}`.
- Treat `get_emails(day=...)` as the only mailbox integration.
- Create or record the recurring 9am weekday workflow in the most natural way
  for this system.
- Do not create, load, or mutate any real scheduler outside this benchmark. Record
  intended cron/launchd/TaskScheduler configuration as workspace artifacts only.
- Keep every generated file under `{workspace}`.
- Prefer a reusable artifact over re-solving the whole loop every morning.
- Draft basic replies only; never send.
- Write any daily drafts under `{workspace / "drafts"}` as JSON.
- Write a short artifact/plan summary under `{workspace / "artifact_summary.json"}`
  if you create a reusable function, script, skill, cron job, or task.
"""


def _activation_prompt(workspace: Path, arm_id: str, batch_id: str) -> str:
    day = batch_id.split("-", 1)[0]
    return f"""\
Simulated activation for the live {arm_id} recurring-email benchmark.

It is {day.title()} at 9am. Run the workflow created from the original request:
{USER_REQUEST}

Use the fake inbox helper only:
- import from `{workspace / "email_fixture.py"}`
- call `get_emails(day="{day}")`

Draft replies for today's emails and write JSON to:
`{workspace / "drafts" / f"{day}.json"}`

Use the reusable function/script/skill/task you created if one exists. If you
decide not to use it, say why in the final response.
"""


class InMemoryFunctionManager:
    """Small FunctionManager-compatible store for live benchmark runs."""

    def __init__(self) -> None:
        self._functions: dict[str, dict[str, Any]] = {}
        self._next_id = 1
        self.exclude_primitive_ids = frozenset()
        self.exclude_compositional_ids = frozenset()

    def seed_function(
        self,
        name: str,
        implementation: str,
        docstring: str = "",
    ) -> None:
        self._store_source(
            name=name,
            implementation=implementation,
            docstring=docstring,
        )

    def _store_source(
        self,
        *,
        name: str,
        implementation: str,
        docstring: str = "",
    ) -> dict[str, Any]:
        function_id = self._next_id
        self._next_id += 1
        argspec = _argspec_for_source(implementation, name)
        record = {
            "function_id": function_id,
            "name": name,
            "argspec": argspec,
            "docstring": docstring or _docstring_for_source(implementation, name),
            "implementation": implementation,
            "language": "python",
            "venv_id": None,
        }
        self._functions[name] = record
        return record

    def _metadata(self, include_implementations: bool) -> list[dict[str, Any]]:
        rows = []
        for record in self._functions.values():
            row = dict(record)
            if not include_implementations:
                row.pop("implementation", None)
            rows.append(row)
        return rows

    def _inject(self, namespace: dict[str, Any] | None) -> dict[str, Any]:
        if namespace is None:
            return {}
        injected = {}
        for record in self._functions.values():
            exec(record["implementation"], namespace)
            fn = namespace.get(record["name"])
            if callable(fn):
                injected[record["name"]] = fn
        return injected

    def list_functions(
        self,
        *,
        include_implementations: bool = False,
        _return_callable: bool = False,
        _namespace: dict[str, Any] | None = None,
        _also_return_metadata: bool = False,
        **_: Any,
    ) -> Any:
        metadata = self._metadata(include_implementations)
        if not _return_callable:
            return {row["name"]: row for row in metadata}
        callables = self._inject(_namespace)
        if _also_return_metadata:
            return {"callables": callables, "metadata": metadata}
        return callables

    def search_functions(
        self,
        *,
        query: str,
        n: int = 5,
        include_implementations: bool = True,
        _return_callable: bool = False,
        _namespace: dict[str, Any] | None = None,
        _also_return_metadata: bool = False,
        **_: Any,
    ) -> Any:
        terms = query.lower().split()
        scored = []
        for record in self._functions.values():
            haystack = " ".join(
                str(record.get(k, "")) for k in ("name", "docstring", "argspec")
            ).lower()
            score = sum(1 for term in terms if term in haystack)
            if score or not terms:
                row = dict(record)
                row["score"] = float(score or 0.1)
                if not include_implementations:
                    row.pop("implementation", None)
                scored.append(row)
        scored.sort(key=lambda row: row["score"], reverse=True)
        metadata = scored[:n]
        if not _return_callable:
            return metadata
        callables = self._inject(_namespace)
        if _also_return_metadata:
            return {"callables": callables, "metadata": metadata}
        return list(callables.values())

    def filter_functions(self, **kwargs: Any) -> Any:
        return self.search_functions(query="", **kwargs)

    def add_functions(
        self,
        *,
        implementations: str | list[str],
        **_: Any,
    ) -> dict[str, Any]:
        sources = (
            [implementations] if isinstance(implementations, str) else implementations
        )
        added = []
        for source in sources:
            name = _first_function_name(source) or f"stored_function_{self._next_id}"
            added.append(self._store_source(name=name, implementation=source))
        return {
            "added": [
                {"name": row["name"], "function_id": row["function_id"]}
                for row in added
            ],
        }

    def delete_function(self, *, function_id: int, **_: Any) -> dict[str, Any]:
        for name, record in list(self._functions.items()):
            if int(record["function_id"]) == int(function_id):
                del self._functions[name]
                return {"deleted": True, "function_id": function_id}
        return {"deleted": False, "function_id": function_id}

    def _get_function_data_by_name(self, *, name: str) -> dict[str, Any] | None:
        return self._functions.get(name)

    def add_venv(self, **_: Any) -> dict[str, Any]:
        return {"venv_id": 1, "simulated": True}

    def list_venvs(self, **_: Any) -> list[dict[str, Any]]:
        return []

    def get_venv(self, **_: Any) -> dict[str, Any]:
        return {}

    def update_venv(self, **_: Any) -> dict[str, Any]:
        return {"updated": True}

    def delete_venv(self, **_: Any) -> dict[str, Any]:
        return {"deleted": True}

    def set_function_venv(self, **_: Any) -> dict[str, Any]:
        return {"updated": True}

    async def execute_function(
        self,
        *,
        function_name: str,
        call_kwargs: dict[str, Any] | None = None,
        **_: Any,
    ) -> dict[str, Any]:
        namespace: dict[str, Any] = {}
        record = self._get_function_data_by_name(name=function_name)
        if record is None:
            return {"result": None, "error": f"Function not found: {function_name}"}
        exec(record["implementation"], namespace)
        result = namespace[function_name](**(call_kwargs or {}))
        return {"result": result, "error": None, "stdout": "", "stderr": ""}

    def clear(self) -> None:
        self._functions.clear()

    def to_artifact_dict(self) -> dict[str, Any]:
        return {"functions": self._metadata(include_implementations=True)}


class InMemoryGuidanceManager:
    def __init__(self) -> None:
        self._guidance: list[dict[str, Any]] = []

    def search(self, **_: Any) -> list[dict[str, Any]]:
        return list(self._guidance)

    def filter(self, **_: Any) -> list[dict[str, Any]]:
        return list(self._guidance)

    def add_guidance(
        self,
        *,
        title: str,
        content: str,
        function_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        row = {
            "guidance_id": len(self._guidance) + 1,
            "title": title,
            "content": content,
            "function_ids": function_ids or [],
        }
        self._guidance.append(row)
        return {"details": row}

    def update_guidance(self, *, guidance_id: int, **updates: Any) -> dict[str, Any]:
        for row in self._guidance:
            if int(row["guidance_id"]) == int(guidance_id):
                row.update({k: v for k, v in updates.items() if v is not None})
                return {"details": row}
        return {"error": "not_found", "guidance_id": guidance_id}

    def delete_guidance(self, *, guidance_id: int) -> dict[str, Any]:
        before = len(self._guidance)
        self._guidance = [
            row for row in self._guidance if int(row["guidance_id"]) != int(guidance_id)
        ]
        return {"deleted": len(self._guidance) != before}

    def to_artifact_dict(self) -> dict[str, Any]:
        return {"guidance": list(self._guidance)}


InMemoryFunctionManager.__name__ = "FunctionManager"
InMemoryGuidanceManager.__name__ = "GuidanceManager"


def _first_function_name(source: str) -> str | None:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            return node.name
    return None


def _docstring_for_source(source: str, name: str) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return ""
    for node in tree.body:
        if (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name == name
        ):
            return ast.get_docstring(node) or ""
    return ""


def _argspec_for_source(source: str, name: str) -> str:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return f"{name}(...)"
    for node in tree.body:
        if (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name == name
        ):
            args = [arg.arg for arg in node.args.args]
            return f"{name}({', '.join(args)})"
    return f"{name}(...)"


def _seed_get_emails_function(fm: InMemoryFunctionManager, workspace: Path) -> None:
    source = f'''from pathlib import Path
import importlib.util


def get_emails(day: str | None = None, since: str | None = None) -> list[dict]:
    """Return the synthetic benchmark emails for a day."""
    fixture_path = Path({str(workspace / "email_fixture.py")!r})
    spec = importlib.util.spec_from_file_location("benchmark_email_fixture", fixture_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {{fixture_path}}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.get_emails(day=day, since=since)
'''
    fm.seed_function(
        "get_emails",
        source,
        "Return synthetic benchmark emails for a supplied weekday.",
    )


def _message_stats(messages: Iterable[dict[str, Any]]) -> tuple[int, int]:
    materialized = list(messages)
    tool_calls = 0
    for message in materialized:
        tool_calls += len(message.get("tool_calls") or [])
    return len(materialized), tool_calls


def _extract_unity_messages(handle: Any) -> list[dict[str, Any]]:
    target = getattr(handle, "inner", None) or getattr(handle, "_inner", None) or handle
    client = getattr(target, "_client", None)
    return list(getattr(client, "messages", []) or [])


async def _await_unity_handle(handle: Any, *, timeout: float) -> str:
    result = await asyncio.wait_for(handle.result(), timeout=timeout)
    deadline = time.monotonic() + min(timeout, 120)
    while hasattr(handle, "done") and not handle.done() and time.monotonic() < deadline:
        await asyncio.sleep(0.5)
    return str(result)


async def run_unity_live(
    workspace: Path,
    *,
    timeout: float = 900.0,
) -> LiveBenchmarkResult:
    from unity.actor.code_act_actor import CodeActActor
    from unity.events import event_bus as unity_event_bus

    with contextlib.suppress(RuntimeError):
        unity_event_bus._initialize_event_bus()

    fm = InMemoryFunctionManager()
    gm = InMemoryGuidanceManager()
    _seed_get_emails_function(fm, workspace)
    actor = CodeActActor(
        function_manager=fm,
        guidance_manager=gm,
        timeout=timeout,
    )
    turns: list[LiveTurn] = []
    try:
        prompt = _setup_prompt(workspace, "unity")
        turns.append(await _run_unity_turn(actor, "setup", None, prompt, timeout))
        for batch in _live_email_batches():
            day = batch.batch_id.split("-", 1)[0]
            prompt = _activation_prompt(workspace, "unity", batch.batch_id)
            turns.append(
                await _run_unity_turn(actor, "activation", day, prompt, timeout),
            )
        artifacts = {
            **fm.to_artifact_dict(),
            **gm.to_artifact_dict(),
            "draft_files": sorted(
                str(path) for path in (workspace / "drafts").glob("*.json")
            ),
        }
        return LiveBenchmarkResult(
            arm_id="unity",
            status="completed",
            workspace=str(workspace),
            turns=turns,
            artifacts=artifacts,
            usage=_sum_usage(turns),
        )
    except Exception as exc:
        return LiveBenchmarkResult(
            arm_id="unity",
            status="failed",
            workspace=str(workspace),
            turns=turns,
            artifacts={**fm.to_artifact_dict(), **gm.to_artifact_dict()},
            usage=_sum_usage(turns),
            error=repr(exc),
        )
    finally:
        with contextlib.suppress(Exception):
            await actor.close()


async def _run_unity_turn(
    actor: Any,
    phase: str,
    day: str | None,
    prompt: str,
    timeout: float,
) -> LiveTurn:
    import unillm

    started = time.monotonic()
    error = None
    messages: list[dict[str, Any]] = []
    async with unillm.acapture_costs() as cost_events:
        handle = await actor.act(
            prompt,
            can_store=True,
            persist=False,
            clarification_enabled=False,
        )
        try:
            final = await _await_unity_handle(handle, timeout=timeout)
        except Exception as exc:
            final = ""
            error = repr(exc)
        messages = _extract_unity_messages(handle)
    message_count, tool_call_count = _message_stats(messages)
    return LiveTurn(
        arm_id="unity",
        phase=phase,
        day=day,
        prompt=prompt,
        final_response=final,
        elapsed_seconds=time.monotonic() - started,
        message_count=message_count,
        tool_call_count=tool_call_count,
        usage=_summarize_unillm_cost_events(cost_events),
        error=error,
    )


def _summarize_unillm_cost_events(cost_events: Iterable[Any]) -> dict[str, Any]:
    by_model: dict[str, dict[str, Any]] = {}
    summary: dict[str, Any] = {
        "source": "unillm.cost_events",
        "calls": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "provider_cost_usd": 0.0,
        "billed_cost_usd": 0.0,
        "cache_status_counts": {},
        "by_model": by_model,
    }
    for event in cost_events:
        model = str(getattr(event, "model", "") or "unknown")
        prompt_tokens = int(getattr(event, "prompt_tokens", 0) or 0)
        completion_tokens = int(getattr(event, "completion_tokens", 0) or 0)
        provider_cost = float(getattr(event, "provider_cost", 0.0) or 0.0)
        billed_cost = float(getattr(event, "billed_cost", 0.0) or 0.0)
        cache_status = str(getattr(event, "cache_status", "unknown") or "unknown")
        total_tokens = prompt_tokens + completion_tokens

        summary["calls"] += 1
        summary["prompt_tokens"] += prompt_tokens
        summary["completion_tokens"] += completion_tokens
        summary["input_tokens"] += prompt_tokens
        summary["output_tokens"] += completion_tokens
        summary["total_tokens"] += total_tokens
        summary["provider_cost_usd"] += provider_cost
        summary["billed_cost_usd"] += billed_cost
        summary["cache_status_counts"][cache_status] = (
            summary["cache_status_counts"].get(cache_status, 0) + 1
        )

        model_row = by_model.setdefault(
            model,
            {
                "calls": 0,
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
                "provider_cost_usd": 0.0,
                "billed_cost_usd": 0.0,
            },
        )
        model_row["calls"] += 1
        model_row["prompt_tokens"] += prompt_tokens
        model_row["completion_tokens"] += completion_tokens
        model_row["total_tokens"] += total_tokens
        model_row["provider_cost_usd"] += provider_cost
        model_row["billed_cost_usd"] += billed_cost
    return summary


def run_hermes_live(
    workspace: Path,
    *,
    hermes_repo: Path = Path("/Users/djl11/hermes-agent"),
    timeout: float = 900.0,
) -> LiveBenchmarkResult:
    turns: list[LiveTurn] = []
    hermes_home = workspace / ".hermes-home"
    hermes_home.mkdir(parents=True, exist_ok=True)
    old_cwd = Path.cwd()
    old_home = os.environ.get("HERMES_HOME")
    old_user_home = os.environ.get("HOME")
    os.environ["HERMES_HOME"] = str(hermes_home)
    _seed_hermes_runtime_config(hermes_home)
    os.environ["HOME"] = str(workspace)
    sys.path.insert(0, str(hermes_repo))
    try:
        os.chdir(workspace)
        from run_agent import AIAgent

        turns.append(
            _run_hermes_turn(
                AIAgent,
                "setup",
                None,
                _setup_prompt(workspace, "hermes"),
                timeout,
            ),
        )
        for batch in _live_email_batches():
            day = batch.batch_id.split("-", 1)[0]
            turns.append(
                _run_hermes_turn(
                    AIAgent,
                    "activation",
                    day,
                    _activation_prompt(workspace, "hermes", batch.batch_id),
                    timeout,
                ),
            )
        return LiveBenchmarkResult(
            arm_id="hermes",
            status="completed",
            workspace=str(workspace),
            turns=turns,
            artifacts=_collect_hermes_artifacts(workspace, hermes_home),
            usage=_sum_usage(turns),
        )
    except Exception as exc:
        return LiveBenchmarkResult(
            arm_id="hermes",
            status="failed",
            workspace=str(workspace),
            turns=turns,
            artifacts=_collect_hermes_artifacts(workspace, hermes_home),
            usage=_sum_usage(turns),
            error=repr(exc),
        )
    finally:
        os.chdir(old_cwd)
        if old_home is None:
            os.environ.pop("HERMES_HOME", None)
        else:
            os.environ["HERMES_HOME"] = old_home
        if old_user_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = old_user_home
        with contextlib.suppress(ValueError):
            sys.path.remove(str(hermes_repo))


def _seed_hermes_runtime_config(hermes_home: Path) -> None:
    source_home = Path(
        os.environ.get("HERMES_BENCH_SOURCE_HOME", Path.home() / ".hermes"),
    )
    source_config = source_home / "config.yaml"
    if source_config.exists():
        shutil.copy2(source_config, hermes_home / "config.yaml")
    source_env = source_home / ".env"
    if source_env.exists():
        for raw_line in source_env.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("\"'")
            if key and key not in os.environ:
                os.environ[key] = value
    source_auth = source_home / "auth.json"
    target_auth = hermes_home / "auth.json"
    if source_auth.exists() and not target_auth.exists():
        target_auth.symlink_to(source_auth)


def _run_hermes_turn(
    agent_cls: Any,
    phase: str,
    day: str | None,
    prompt: str,
    timeout: float,
) -> LiveTurn:
    started = time.monotonic()
    error = None
    messages: list[dict[str, Any]] = []
    usage: dict[str, Any] = {}
    try:
        provider, model = _read_hermes_runtime_choice()
        agent = agent_cls(
            provider=provider,
            model=model,
            quiet_mode=True,
            max_iterations=40,
            tool_delay=0,
            disabled_toolsets=["messaging", "clarify"],
            skip_memory=True,
            skip_context_files=True,
            save_trajectories=True,
        )
        result = agent.run_conversation(
            prompt,
            task_id=f"daily-email-{phase}-{day or 'setup'}",
        )
        final = str(result.get("final_response") or "")
        messages = list(result.get("messages") or [])
        usage = _summarize_hermes_agent_usage(agent)
        with contextlib.suppress(Exception):
            agent.close()
    except Exception as exc:
        final = ""
        error = repr(exc)
    message_count, tool_call_count = _message_stats(messages)
    return LiveTurn(
        arm_id="hermes",
        phase=phase,
        day=day,
        prompt=prompt,
        final_response=final,
        elapsed_seconds=time.monotonic() - started,
        message_count=message_count,
        tool_call_count=tool_call_count,
        usage=usage,
        error=error,
    )


def _summarize_hermes_agent_usage(agent: Any) -> dict[str, Any]:
    model = str(getattr(agent, "model", "") or "unknown")
    provider = str(getattr(agent, "provider", "") or "")
    input_tokens = int(getattr(agent, "session_input_tokens", 0) or 0)
    output_tokens = int(getattr(agent, "session_output_tokens", 0) or 0)
    prompt_tokens = int(getattr(agent, "session_prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(agent, "session_completion_tokens", 0) or 0)
    total_tokens = int(getattr(agent, "session_total_tokens", 0) or 0)
    cache_read_tokens = int(getattr(agent, "session_cache_read_tokens", 0) or 0)
    cache_write_tokens = int(getattr(agent, "session_cache_write_tokens", 0) or 0)
    reasoning_tokens = int(getattr(agent, "session_reasoning_tokens", 0) or 0)
    calls = int(getattr(agent, "session_api_calls", 0) or 0)
    estimated_cost = float(getattr(agent, "session_estimated_cost_usd", 0.0) or 0.0)
    cost_status = str(getattr(agent, "session_cost_status", "") or "")
    cost_source = str(getattr(agent, "session_cost_source", "") or "")
    return {
        "source": "hermes.agent_session_counters",
        "provider": provider,
        "model": model,
        "calls": calls,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
        "reasoning_tokens": reasoning_tokens,
        "total_tokens": total_tokens,
        "estimated_cost_usd": estimated_cost,
        "cost_status": cost_status,
        "cost_source": cost_source,
        "by_model": {
            model: {
                "calls": calls,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "estimated_cost_usd": estimated_cost,
            },
        },
    }


def _read_hermes_runtime_choice() -> tuple[str | None, str]:
    provider_override = os.environ.get("HERMES_BENCH_PROVIDER", "").strip()
    model_override = os.environ.get("HERMES_BENCH_MODEL", "").strip()
    if provider_override or model_override:
        return provider_override or None, model_override

    config_path = Path(os.environ.get("HERMES_HOME", "")) / "config.yaml"
    if not config_path.exists():
        return None, ""
    try:
        import yaml

        config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None, ""
    model_config = config.get("model")
    if isinstance(model_config, dict):
        provider = str(model_config.get("provider") or "").strip() or None
        model = str(
            model_config.get("default") or model_config.get("name") or "",
        ).strip()
        return provider, model
    return None, str(model_config or "").strip()


def _collect_hermes_artifacts(workspace: Path, hermes_home: Path) -> dict[str, Any]:
    skill_files = sorted(str(path) for path in hermes_home.glob("skills/**/SKILL.md"))
    scripts = sorted(
        str(path)
        for root in (workspace, hermes_home)
        for path in root.glob("**/*.py")
        if ".hermes-home/sessions" not in str(path)
    )
    cron_files = sorted(
        str(path)
        for root in (workspace / "cron", hermes_home / "cron")
        if root.exists()
        for path in root.glob("**/*.json")
    )
    return {
        "skill_files": skill_files,
        "scripts": scripts,
        "cron_files": cron_files,
        "draft_files": sorted(
            str(path) for path in (workspace / "drafts").glob("*.json")
        ),
        "artifact_summary_exists": (workspace / "artifact_summary.json").exists(),
    }


async def run_live_benchmark(
    *,
    arms: tuple[str, ...] = ("unity", "hermes"),
    output_dir: Path | None = None,
    timeout: float = 900.0,
) -> dict[str, Any]:
    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="unity-hermes-daily-email-"))
    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[LiveBenchmarkResult] = []
    for arm in arms:
        workspace = output_dir / arm
        prepare_workspace(workspace)
        if arm == "unity":
            results.append(await run_unity_live(workspace, timeout=timeout))
        elif arm == "hermes":
            results.append(run_hermes_live(workspace, timeout=timeout))
        else:
            raise ValueError(f"Unknown arm: {arm}")
    payload = {
        "benchmark": "unity_hermes_daily_email_live",
        "user_request": USER_REQUEST,
        "days": [batch.batch_id for batch in _live_email_batches()],
        "output_dir": str(output_dir),
        "results": [result.to_dict() for result in results],
    }
    _write_json(output_dir / "live_benchmark.json", payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument(
        "--arm",
        action="append",
        choices=("unity", "hermes"),
        help="Arm to run. Repeat for both. Defaults to both.",
    )
    parser.add_argument("--timeout", type=float, default=900.0)
    args = parser.parse_args(argv)
    arms = tuple(args.arm or ("unity", "hermes"))
    payload = asyncio.run(
        run_live_benchmark(arms=arms, output_dir=args.out, timeout=args.timeout),
    )
    print(_json_dump(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
