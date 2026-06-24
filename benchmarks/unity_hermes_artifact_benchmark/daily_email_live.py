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
import copy
import inspect
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
from .models import ArtifactKind, ArtifactObservation
from .scoring import score_artifact

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
        data = asdict(self)
        data["agent_visible_prompt"] = self.prompt
        return data


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
    """Create an isolated workspace with a local ``get_emails`` helper."""

    root.mkdir(parents=True, exist_ok=True)
    payload = _workweek_payload()
    _write_json(root / "emails_by_day.json", payload)
    if payload["batches"]:
        (root / "active_batch_id.txt").write_text(
            next(iter(payload["batches"])),
            encoding="utf-8",
        )
    (root / "drafts").mkdir(exist_ok=True)
    helper = f'''"""Local inbox helper available to the email assistant."""

from __future__ import annotations

from pathlib import Path

EMAILS_BY_DAY = {pprint.pformat(payload["batches"], sort_dicts=True, width=100)}
ACTIVE_BATCH_FILE = Path(__file__).with_name("active_batch_id.txt")


def get_emails(day: str | None = None, since: str | None = None) -> list[dict]:
    """Return recent emails from the local mailbox adapter.

    Args:
        day: Optional day name such as ``"monday"``.
        since: Optional mailbox-style cursor. The local adapter is already
            partitioned by scheduled run.
    """

    if day is None:
        if not ACTIVE_BATCH_FILE.exists():
            raise ValueError("No active mailbox batch is configured for this run")
        day = ACTIVE_BATCH_FILE.read_text(encoding="utf-8").strip()
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
        "active_batch_id": str(root / "active_batch_id.txt"),
        "email_fixture": str(root / "email_fixture.py"),
        "drafts_dir": str(root / "drafts"),
    }


def _set_workspace_active_batch(workspace: Path, batch_id: str) -> None:
    (workspace / "active_batch_id.txt").write_text(batch_id, encoding="utf-8")


def _setup_prompt(workspace: Path | None = None, arm_id: str | None = None) -> str:
    return USER_REQUEST


def _activation_prompt(
    workspace: Path | None = None,
    arm_id: str | None = None,
    batch_id: str | None = None,
    *,
    scheduled_task_description: str | None = None,
) -> str:
    return scheduled_task_description or USER_REQUEST


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
        *,
        callable_fn: Any | None = None,
    ) -> None:
        self._store_source(
            name=name,
            implementation=implementation,
            docstring=docstring,
            callable_fn=callable_fn,
        )

    def _store_source(
        self,
        *,
        name: str,
        implementation: str,
        docstring: str = "",
        callable_fn: Any | None = None,
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
            "_callable": callable_fn,
        }
        self._functions[name] = record
        return record

    def _metadata(self, include_implementations: bool) -> list[dict[str, Any]]:
        rows = []
        for record in self._functions.values():
            row = dict(record)
            row.pop("_callable", None)
            if not include_implementations:
                row.pop("implementation", None)
            rows.append(row)
        return rows

    def _inject(self, namespace: dict[str, Any] | None) -> dict[str, Any]:
        if namespace is None:
            return {}
        injected = {}
        for record in self._functions.values():
            if record.get("_callable") is not None:
                namespace[record["name"]] = record["_callable"]
                fn = record["_callable"]
            else:
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
                row.pop("_callable", None)
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
        if record.get("_callable") is not None:
            fn = record["_callable"]
        else:
            exec(record["implementation"], namespace)
            fn = namespace[function_name]
        result = fn(**(call_kwargs or {}))
        if inspect.isawaitable(result):
            result = await result
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


class LiveMailboxContext:
    """Harness-side mailbox state with hidden scheduled-run activation context."""

    def __init__(self, batches: Iterable[Any]) -> None:
        self._batches = {batch.batch_id: batch.to_dict() for batch in batches}
        self.active_batch_id: str | None = next(iter(self._batches), None)

    def set_active_batch(self, batch_id: str) -> None:
        if batch_id not in self._batches:
            raise KeyError(f"Unknown mailbox batch: {batch_id}")
        self.active_batch_id = batch_id

    def get_emails(
        self,
        day: str | None = None,
        since: str | None = None,
    ) -> list[dict[str, Any]]:
        del since
        batch = self._select_batch(day)
        return copy.deepcopy(batch["emails"])

    def harness_context(self) -> dict[str, Any]:
        return {
            "active_batch_id": self.active_batch_id,
            "available_batch_ids": list(self._batches),
            "expected_outputs_hidden": True,
        }

    def _select_batch(self, day: str | None) -> dict[str, Any]:
        if day is None:
            if self.active_batch_id is None:
                raise RuntimeError("No active mailbox batch is configured")
            return self._batches[self.active_batch_id]
        normalized = day.lower().strip()
        if normalized in self._batches:
            return self._batches[normalized]
        for batch_id, batch in self._batches.items():
            if batch_id.startswith(normalized):
                return batch
        raise KeyError(f"No mailbox batch for {day!r}")


class _CompletedToolHandle:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    async def result(self) -> str:
        return _json_dump(self._payload)

    async def ask(self, question: str, **_: Any) -> "_CompletedToolHandle":
        return _CompletedToolHandle({"question": question, "result": self._payload})

    async def interject(self, message: str, **_: Any) -> None:
        del message

    async def stop(self, reason: str | None = None) -> None:
        del reason

    async def pause(self) -> None:
        return None

    async def resume(self) -> None:
        return None

    def done(self) -> bool:
        return True

    async def next_clarification(self) -> dict[str, Any]:
        return {}

    async def next_notification(self) -> dict[str, Any]:
        return {}

    async def answer_clarification(self, call_id: str, answer: str) -> None:
        del call_id, answer


class MockTaskScheduler:
    """Production-shaped task manager that records schedules in memory only."""

    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []
        self.executions: list[dict[str, Any]] = []
        self._next_id = 1

    async def update(
        self,
        text: str,
        *,
        response_format: Any | None = None,
        **_: Any,
    ) -> _CompletedToolHandle:
        del response_format
        record = {
            "task_id": self._next_id,
            "description": text,
            "status": "scheduled",
            "repeat": _infer_repeat(text),
            "start_at": _infer_start_at(text),
            "entrypoint_candidate": _infer_entrypoint_candidate(text),
        }
        self._next_id += 1
        self.records.append(record)
        return _CompletedToolHandle({"created_or_updated": record})

    async def execute(self, task_id: int, **_: Any) -> _CompletedToolHandle:
        record = {"task_id": task_id, "status": "started"}
        self.executions.append(record)
        return _CompletedToolHandle(record)

    async def ask(self, text: str, **_: Any) -> _CompletedToolHandle:
        return _CompletedToolHandle({"query": text, "tasks": self.records})

    def to_artifact_dict(self) -> dict[str, Any]:
        return {
            "scheduler_records": copy.deepcopy(self.records),
            "scheduler_executions": copy.deepcopy(self.executions),
        }


@dataclass
class _LivePrimitives:
    tasks: MockTaskScheduler


class LiveTaskEnvironment:
    """Small production-like primitives surface for recurring task setup."""

    namespace = "primitives"

    def __init__(self, scheduler: MockTaskScheduler) -> None:
        self.scheduler = scheduler
        self._instance = _LivePrimitives(tasks=scheduler)
        self._clarification_up_q = None
        self._clarification_down_q = None

    def get_instance(self) -> _LivePrimitives:
        return self._instance

    def get_sandbox_instance(self) -> _LivePrimitives:
        return self._instance

    def get_tools(self) -> dict[str, Any]:
        from unity.actor.environments.base import ToolMetadata

        return {
            "primitives.tasks.update": ToolMetadata(
                name="primitives.tasks.update",
                is_impure=True,
                is_steerable=True,
            ),
            "primitives.tasks.execute": ToolMetadata(
                name="primitives.tasks.execute",
                is_impure=True,
                is_steerable=True,
            ),
            "primitives.tasks.ask": ToolMetadata(
                name="primitives.tasks.ask",
                is_impure=False,
                is_steerable=True,
            ),
        }

    def get_prompt_context(self) -> str:
        return """\
### Task Scheduling

Use `await primitives.tasks.update(text=...)` to create or update durable tasks,
including recurring work. Put the user's goal, cadence, and any reusable
entrypoint/function choice in the natural-language request. The task manager
returns a handle; await `handle.result()` when you need confirmation.

Use `await primitives.tasks.execute(task_id=...)` only when intentionally
starting an existing task now."""

    async def capture_state(self) -> dict[str, Any]:
        return self.scheduler.to_artifact_dict()


def _infer_repeat(text: str) -> str | None:
    lowered = text.lower()
    if "every morning" in lowered or "daily" in lowered:
        return "daily"
    if "weekday" in lowered or "monday" in lowered:
        return "weekdays"
    return None


def _infer_start_at(text: str) -> str | None:
    lowered = text.lower()
    if "9am" in lowered or "9:00" in lowered:
        return "09:00"
    return None


def _infer_entrypoint_candidate(text: str) -> str | None:
    lowered = text.lower()
    marker_terms = ("entrypoint", "function", "call")
    if not any(term in lowered for term in marker_terms):
        return None
    tokens = [token.strip("`'\".,:()[]{}") for token in text.replace("\n", " ").split()]
    for token in tokens:
        if token and token.replace("_", "").isalnum() and "_" in token:
            return token
    return None


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


def _seed_get_emails_function(
    fm: InMemoryFunctionManager,
    mailbox: LiveMailboxContext,
) -> None:
    source = '''def get_emails(day: str | None = None, since: str | None = None) -> list[dict]:
    """Return recent emails from the user's mailbox."""
    raise RuntimeError("Mailbox adapter is only available in the configured runtime")
'''
    fm.seed_function(
        "get_emails",
        source,
        "Return recent emails from the user's mailbox.",
        callable_fn=mailbox.get_emails,
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

    batches = _live_email_batches()
    mailbox = LiveMailboxContext(batches)
    scheduler = MockTaskScheduler()
    fm = InMemoryFunctionManager()
    gm = InMemoryGuidanceManager()
    _seed_get_emails_function(fm, mailbox)
    actor = CodeActActor(
        environments=[LiveTaskEnvironment(scheduler)],
        function_manager=fm,
        guidance_manager=gm,
        timeout=timeout,
    )
    turns: list[LiveTurn] = []
    try:
        if batches:
            mailbox.set_active_batch(batches[0].batch_id)
        prompt = _setup_prompt(workspace, "unity")
        turns.append(await _run_unity_turn(actor, "setup", None, prompt, timeout))
        for batch in batches:
            day = batch.batch_id.split("-", 1)[0]
            mailbox.set_active_batch(batch.batch_id)
            prompt = _activation_prompt(
                workspace,
                "unity",
                batch.batch_id,
                scheduled_task_description=_scheduled_task_description(scheduler),
            )
            turns.append(
                await _run_unity_turn(actor, "activation", day, prompt, timeout),
            )
        artifacts = _collect_unity_artifacts(workspace, fm, gm, scheduler, mailbox)
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
            artifacts=_collect_unity_artifacts(workspace, fm, gm, scheduler, mailbox),
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


def _scheduled_task_description(scheduler: MockTaskScheduler) -> str | None:
    if not scheduler.records:
        return None
    return str(scheduler.records[-1].get("description") or "").strip() or None


def _collect_unity_artifacts(
    workspace: Path,
    fm: InMemoryFunctionManager,
    gm: InMemoryGuidanceManager,
    scheduler: MockTaskScheduler,
    mailbox: LiveMailboxContext,
) -> dict[str, Any]:
    functions = fm.to_artifact_dict()
    guidance = gm.to_artifact_dict()
    files = _workspace_generated_files(workspace)
    observation = _observe_unity_artifact(functions, scheduler, files)
    score = score_artifact(observation)
    return {
        **functions,
        **guidance,
        **scheduler.to_artifact_dict(),
        "workspace_files": files,
        "draft_files": sorted(
            str(path) for path in (workspace / "drafts").glob("*.json")
        ),
        "mailbox_harness_context": mailbox.harness_context(),
        "observed_artifact_kind": observation.kind.value,
        "observed_artifact": observation.to_dict(),
        "observed_artifact_score": score.to_dict(),
    }


def _observe_unity_artifact(
    functions_artifact: dict[str, Any],
    scheduler: MockTaskScheduler,
    files: list[str],
) -> ArtifactObservation:
    user_functions = [
        row
        for row in functions_artifact.get("functions", [])
        if row.get("name") != "get_emails"
    ]
    scheduler_binding = _scheduler_binding(scheduler)
    if user_functions:
        chosen = user_functions[-1]
        implementation = str(chosen.get("implementation") or "")
        semantic = _contains_semantic_llm_call(implementation)
        return ArtifactObservation(
            arm_id="unity",
            name=str(chosen.get("name") or "stored_function"),
            kind=ArtifactKind.UNITY_FUNCTION,
            entrypoint=str(chosen.get("name") or ""),
            invocation_path=("FunctionManager_search_functions", "execute_function"),
            has_stable_input_schema=bool(chosen.get("argspec")),
            has_stable_output_schema=_looks_structured(implementation),
            has_dry_run_mode="dry_run" in implementation,
            semantic_calls_inside_artifact=semantic,
            cheap_semantic_model=_extract_model_hint(implementation),
            scheduler_binding=scheduler_binding,
            requires_procedural_prompt_reread=False,
            exposes_supporting_script_directly=False,
            notes="Stored FunctionManager function observed after live run.",
        )
    if files:
        return ArtifactObservation(
            arm_id="unity",
            name=Path(files[0]).name,
            kind=ArtifactKind.FILESYSTEM_SCRIPT,
            entrypoint=files[0],
            invocation_path=("locate_workspace_file", "terminal_run_script"),
            has_stable_input_schema=False,
            has_stable_output_schema=_file_mentions_json(Path(files[0])),
            has_dry_run_mode=_file_contains(Path(files[0]), "dry_run"),
            semantic_calls_inside_artifact=_file_mentions_semantic_llm(Path(files[0])),
            cheap_semantic_model=_extract_model_hint(_safe_read_text(Path(files[0]))),
            scheduler_binding=scheduler_binding,
            requires_procedural_prompt_reread=True,
            exposes_supporting_script_directly=True,
            notes="Filesystem script fallback observed after live run.",
        )
    return ArtifactObservation(
        arm_id="unity",
        name="prompt_only",
        kind=ArtifactKind.PROMPT_ONLY,
        entrypoint=None,
        invocation_path=("regenerate_workflow",),
        has_stable_input_schema=False,
        has_stable_output_schema=False,
        has_dry_run_mode=False,
        semantic_calls_inside_artifact=False,
        cheap_semantic_model=None,
        scheduler_binding=scheduler_binding,
        requires_procedural_prompt_reread=True,
        exposes_supporting_script_directly=False,
        notes="No reusable function or generated script found.",
    )


def _workspace_generated_files(workspace: Path) -> list[str]:
    ignored = {
        workspace / "email_fixture.py",
        workspace / "emails_by_day.json",
        workspace / "active_batch_id.txt",
        workspace / "live_benchmark.json",
    }
    files: list[str] = []
    for path in workspace.glob("**/*"):
        if not path.is_file() or path in ignored:
            continue
        if ".hermes-home/sessions" in str(path):
            continue
        if path.suffix in {".py", ".json", ".yaml", ".yml", ".sh", ".md"}:
            files.append(str(path))
    return sorted(files)


def _scheduler_binding(scheduler: MockTaskScheduler) -> str | None:
    if not scheduler.records:
        return None
    record = scheduler.records[-1]
    cadence = record.get("repeat") or "scheduled"
    start_at = record.get("start_at")
    if start_at:
        return f"{cadence} at {start_at}"
    return str(cadence)


def _contains_semantic_llm_call(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in ("query_llm(", "unillm", "chat.completions"))


def _looks_structured(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in ("pydantic", "basemodel", "dict", "json"))


def _extract_model_hint(text: str) -> str | None:
    for marker in ("model=", "model ="):
        if marker in text:
            after = text.split(marker, 1)[1].strip()
            if after[:1] in {"'", '"'}:
                quote = after[0]
                return after[1:].split(quote, 1)[0]
    return None


def _safe_read_text(path: Path) -> str:
    with contextlib.suppress(Exception):
        return path.read_text(encoding="utf-8")
    return ""


def _file_contains(path: Path, needle: str) -> bool:
    return needle in _safe_read_text(path)


def _file_mentions_json(path: Path) -> bool:
    return "json" in _safe_read_text(path).lower()


def _file_mentions_semantic_llm(path: Path) -> bool:
    return _contains_semantic_llm_call(_safe_read_text(path))


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

        batches = _live_email_batches()
        if batches:
            _set_workspace_active_batch(workspace, batches[0].batch_id)
        turns.append(
            _run_hermes_turn(
                AIAgent,
                "setup",
                None,
                _setup_prompt(workspace, "hermes"),
                timeout,
            ),
        )
        for batch in batches:
            day = batch.batch_id.split("-", 1)[0]
            _set_workspace_active_batch(workspace, batch.batch_id)
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
    observation = _observe_hermes_artifact(skill_files, scripts, cron_files)
    score = score_artifact(observation)
    return {
        "skill_files": skill_files,
        "scripts": scripts,
        "cron_files": cron_files,
        "draft_files": sorted(
            str(path) for path in (workspace / "drafts").glob("*.json")
        ),
        "artifact_summary_exists": (workspace / "artifact_summary.json").exists(),
        "observed_artifact_kind": observation.kind.value,
        "observed_artifact": observation.to_dict(),
        "observed_artifact_score": score.to_dict(),
    }


def _observe_hermes_artifact(
    skill_files: list[str],
    scripts: list[str],
    cron_files: list[str],
) -> ArtifactObservation:
    scheduler_binding = "cron/task file" if cron_files else None
    generated_scripts = [
        path
        for path in scripts
        if not path.endswith("email_fixture.py")
        and not path.endswith("__init__.py")
        and ".hermes-home/sessions" not in path
    ]
    if skill_files:
        skill_text = "\n".join(_safe_read_text(Path(path)) for path in skill_files)
        script_text = "\n".join(
            _safe_read_text(Path(path)) for path in generated_scripts
        )
        return ArtifactObservation(
            arm_id="hermes",
            name=Path(skill_files[0]).parent.name,
            kind=ArtifactKind.HERMES_SKILL_WITH_SCRIPT,
            entrypoint=generated_scripts[0] if generated_scripts else None,
            invocation_path=(
                "load_or_preload_skill_text",
                "infer_supporting_script_path",
                "terminal_run_script",
            ),
            has_stable_input_schema=_looks_structured(script_text),
            has_stable_output_schema=_looks_structured(script_text),
            has_dry_run_mode="dry_run" in script_text,
            semantic_calls_inside_artifact=_contains_semantic_llm_call(
                skill_text + "\n" + script_text,
            ),
            cheap_semantic_model=_extract_model_hint(skill_text + "\n" + script_text),
            scheduler_binding=scheduler_binding,
            requires_procedural_prompt_reread=True,
            exposes_supporting_script_directly=bool(generated_scripts),
            notes="Hermes skill observed after live run.",
        )
    if generated_scripts:
        script_text = _safe_read_text(Path(generated_scripts[0]))
        return ArtifactObservation(
            arm_id="hermes",
            name=Path(generated_scripts[0]).name,
            kind=ArtifactKind.HERMES_NO_AGENT_SCRIPT,
            entrypoint=generated_scripts[0],
            invocation_path=("terminal_run_script",),
            has_stable_input_schema=_looks_structured(script_text),
            has_stable_output_schema=_looks_structured(script_text),
            has_dry_run_mode="dry_run" in script_text,
            semantic_calls_inside_artifact=_contains_semantic_llm_call(script_text),
            cheap_semantic_model=_extract_model_hint(script_text),
            scheduler_binding=scheduler_binding,
            requires_procedural_prompt_reread=False,
            exposes_supporting_script_directly=True,
            notes="Standalone script observed after live run.",
        )
    return ArtifactObservation(
        arm_id="hermes",
        name="prompt_only",
        kind=ArtifactKind.PROMPT_ONLY,
        entrypoint=None,
        invocation_path=("regenerate_workflow",),
        has_stable_input_schema=False,
        has_stable_output_schema=False,
        has_dry_run_mode=False,
        semantic_calls_inside_artifact=False,
        cheap_semantic_model=None,
        scheduler_binding=scheduler_binding,
        requires_procedural_prompt_reread=True,
        exposes_supporting_script_directly=False,
        notes="No skill or generated script found.",
    )


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
        "agent_visible_setup_prompt": _setup_prompt(),
        "harness_context": {
            "mailbox_batches": [batch.batch_id for batch in _live_email_batches()],
            "expected_outputs_hidden_from_agent": True,
            "fake_mailbox_selection": "hidden activation context",
            "scheduler_surface": "mock primitives.tasks sink for Unity; isolated home/workspace for Hermes",
        },
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
