"""Verifier passes for stored functions on the trust ramp.

Four independent LLM passes — static review (once per trust hash), argument
review, precondition probe and post-execution review — plus the read-only
probe they may run. Each pass builds a three-block prompt (constant prefix,
per-call-site stable block, per-call volatile block), asks a verifier client
tagged ``purpose="verification"`` for a ``Verdict``, and appends the verdict
to ``Functions/Verifications``. Passes never repair anything and never see
the conversation transcript or the CodeAct trajectory.

The intent chain lives on ``current_verification_frames``: wrappers push a
``Frame`` on entry and pop it on exit, so a pass for a leaf sees every frame
from the root of the run down to the call under review.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence

from pydantic import ValidationError

from unify.actor.prompt_builders import (
    build_args_review_prompt,
    build_call_stable_block,
    build_post_probe_prompt,
    build_precondition_probe_prompt,
    build_static_review_prompt,
)
from unify.common.async_tool_loop import start_async_tool_loop
from unify.common.llm_client import (
    new_llm_client,
    pydantic_to_json_schema_response_format,
)
from unify.function_manager.types.verification import (
    StaticReviewRecord,
    Verdict,
    VerdictKind,
    VerificationRow,
)
from unify.function_manager.verification.ledger import args_signature, utcnow
from unify.function_manager.verification.source_labels import (
    executed_source_lines,
    function_name_from_filename,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Intent chain
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Frame:
    """One stored-function activation on the path from the root to a call."""

    function_id: int
    name: str
    docstring: str
    effect_class: str
    call_site_line: str
    args_repr: str

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


current_verification_frames: contextvars.ContextVar[tuple[Frame, ...]] = (
    contextvars.ContextVar("current_verification_frames", default=())
)


@contextmanager
def pushed_frame(frame: Frame) -> Iterator[tuple[Frame, ...]]:
    """Push ``frame`` for the duration of a call; yields the full chain."""
    chain = (*current_verification_frames.get(), frame)
    token = current_verification_frames.set(chain)
    try:
        yield chain
    finally:
        current_verification_frames.reset(token)


@dataclass(frozen=True)
class CallSite:
    """Where in a stored function the current call was made from."""

    parent_name: Optional[str]
    line_number: Optional[int]
    line_text: Optional[str]

    @property
    def label(self) -> str:
        return self.parent_name or "root"


def locate_call_site() -> CallSite:
    """Find the nearest enclosing stored-function frame on the Python stack.

    Stored functions are compiled under ``<function:NAME>``; the innermost
    such frame above the caller is the function whose body made this call.
    """
    frame = sys._getframe(1)
    while frame is not None:
        name = function_name_from_filename(frame.f_code.co_filename)
        if name is not None:
            lineno = frame.f_lineno
            lines = executed_source_lines(name)
            text = None
            if lines and 0 < lineno <= len(lines):
                text = lines[lineno - 1].rstrip("\n")
            return CallSite(parent_name=name, line_number=lineno, line_text=text)
        frame = frame.f_back
    return CallSite(parent_name=None, line_number=None, line_text=None)


def executed_source(name: str) -> Optional[str]:
    lines = executed_source_lines(name)
    return "".join(lines) if lines else None


# ---------------------------------------------------------------------------
# Read-only probe
# ---------------------------------------------------------------------------

PROBE_TIMEOUT_S = 60.0
PROBE_OUTPUT_LIMIT = 20_000


async def run_probe(code: str) -> str:
    """Execute a short read-only Python diagnosis snippet in a fresh subprocess.

    Use this to observe the CURRENT behavior of the external interfaces a
    function reads — for example, fetch the endpoint it ingests and print the
    response's shape, keys, and a sample record — so a judgement is grounded
    in observed reality rather than in assumptions or in the function's own
    error messages. The snippet runs in a fresh isolated interpreter with the
    standard library only and must print its observations to stdout.

    Strictly read-only diagnosis: fetch and inspect inputs only. Never
    perform a function's side effects (no writes, deliveries, or state
    mutations on external systems). Output is truncated after 20,000
    characters; the subprocess is killed after 60 seconds.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-I",
        "-c",
        str(code),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        raw, _ = await asyncio.wait_for(proc.communicate(), timeout=PROBE_TIMEOUT_S)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return f"Probe timed out after {int(PROBE_TIMEOUT_S)} seconds and was killed."
    output = raw.decode("utf-8", errors="replace")
    if len(output) > PROBE_OUTPUT_LIMIT:
        output = output[:PROBE_OUTPUT_LIMIT] + "\n... (output truncated)"
    return (
        f"exit_code={proc.returncode}\n{output}"
        if output.strip()
        else f"exit_code={proc.returncode} (no output printed)"
    )


# ---------------------------------------------------------------------------
# Verifier passes
# ---------------------------------------------------------------------------


@dataclass
class PassUsage:
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost: float = 0.0


def _usage_from_completion(completion: Any) -> PassUsage:
    usage = getattr(completion, "usage", None)
    if usage is None and isinstance(completion, dict):
        usage = completion.get("usage")
    if usage is None:
        return PassUsage()

    def _get(name: str) -> Any:
        if isinstance(usage, dict):
            return usage.get(name)
        return getattr(usage, name, None)

    cost = _get("cost")
    return PassUsage(
        prompt_tokens=int(_get("prompt_tokens") or 0),
        completion_tokens=int(_get("completion_tokens") or 0),
        cost=float(cost) if isinstance(cost, (int, float)) else 0.0,
    )


def _parse_verdict(text: Any) -> Optional[Verdict]:
    if isinstance(text, Verdict):
        return text
    if isinstance(text, dict):
        try:
            return Verdict.model_validate(text)
        except ValidationError:
            return None
    if not isinstance(text, str):
        return None
    candidate = text.strip()
    if candidate.startswith("```"):
        candidate = candidate.strip("`")
        if candidate.startswith("json"):
            candidate = candidate[4:]
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        start, end = candidate.find("{"), candidate.rfind("}")
        if start < 0 or end <= start:
            return None
        try:
            payload = json.loads(candidate[start : end + 1])
        except json.JSONDecodeError:
            return None
    try:
        return Verdict.model_validate(payload)
    except ValidationError:
        return None


class VerifierPasses:
    """The four verifier passes for one run, sharing goal, guidance and settings."""

    def __init__(
        self,
        *,
        function_manager: Any,
        guidance_manager: Any = None,
        goal: str = "",
        run_key: Optional[str] = None,
        task_id: Optional[int] = None,
        model: Optional[str] = None,
    ) -> None:
        self.fm = function_manager
        self.gm = guidance_manager
        self.goal = goal
        self.run_key = run_key
        self.task_id = task_id
        self.settings = function_manager.verification_settings
        self.model = model if model is not None else self.settings.model
        self._hash_cache: Dict[int, str] = {}
        self._guidance_cache: Dict[int, Optional[Dict[str, Any]]] = {}
        self._row_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    # ---- shared helpers -------------------------------------------------

    def trust_hash(self, row: Mapping[str, Any]) -> str:
        function_id = int(row["function_id"])
        cached = self._hash_cache.get(function_id)
        if cached is None:
            cached = self.fm.function_trust_hash(dict(row))
            self._hash_cache[function_id] = cached
        return cached

    def forget_hash(self, function_id: int) -> None:
        self._hash_cache.pop(int(function_id), None)

    def row_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        if name not in self._row_cache:
            self._row_cache[name] = self.fm._get_function_data_by_name(name=name)
        return self._row_cache[name]

    def guidance_for(self, row: Mapping[str, Any]) -> List[Dict[str, Any]]:
        """Bodies of the guidance entries linked to ``row``, capped by settings."""
        if self.gm is None:
            return []
        entries: List[Dict[str, Any]] = []
        for guidance_id in row.get("guidance_ids") or []:
            gid = int(guidance_id)
            if gid not in self._guidance_cache:
                try:
                    guidance = self.gm.get_guidance(guidance_id=gid)
                    self._guidance_cache[gid] = {
                        "title": getattr(guidance, "title", None),
                        "content": getattr(guidance, "content", ""),
                    }
                except Exception:
                    self._guidance_cache[gid] = None
            entry = self._guidance_cache[gid]
            if entry is not None:
                entries.append(entry)
        return entries

    def dependencies_of(self, row: Mapping[str, Any]) -> List[Dict[str, Any]]:
        deps: List[Dict[str, Any]] = []
        for name in row.get("depends_on") or []:
            if not isinstance(name, str) or "." in name:
                continue
            dep = self.row_by_name(name)
            if dep is not None:
                deps.append(dep)
        return deps

    def stable_block(
        self,
        row: Mapping[str, Any],
        *,
        frames: Sequence[Frame],
        call_site: CallSite,
        root_row: Optional[Mapping[str, Any]] = None,
    ) -> str:
        guidance = list(self.guidance_for(root_row)) if root_row is not None else []
        if root_row is None or root_row.get("function_id") != row.get("function_id"):
            guidance.extend(self.guidance_for(row))
        parent_source = (
            executed_source(call_site.parent_name) if call_site.parent_name else None
        )
        return build_call_stable_block(
            goal=self.goal,
            guidance=guidance,
            frames=[frame.as_dict() for frame in frames],
            leaf=dict(row),
            parent_source=parent_source,
            call_line=call_site.line_number,
            children=self.dependencies_of(row),
            max_guidance_chars=self.settings.max_guidance_chars,
        )

    def _client(self, origin: str):
        return new_llm_client(self.model, purpose="verification", origin=origin)

    async def _judge(
        self,
        *,
        prefix: str,
        stable: str,
        volatile: str,
        origin: str,
    ) -> tuple[Verdict, PassUsage]:
        """One structured verdict; JSON parse failure retries once, transport failure is UNSURE."""
        client = self._client(origin)
        client.set_system_message(prefix)
        message = stable if not volatile else f"{stable}\n\n{volatile}"
        usage = PassUsage()
        messages = [{"role": "user", "content": message}]
        for attempt in (1, 2):
            try:
                completion = await client.generate(
                    messages=messages,
                    return_full_completion=True,
                    response_format=pydantic_to_json_schema_response_format(Verdict),
                )
            except Exception as exc:
                logger.warning(
                    "Verifier pass %s failed to reach the model: %s",
                    origin,
                    exc,
                )
                return (
                    Verdict(verdict="UNSURE", reason="llm_error", fault=None),
                    usage,
                )
            step = _usage_from_completion(completion)
            usage.prompt_tokens += step.prompt_tokens
            usage.completion_tokens += step.completion_tokens
            usage.cost += step.cost
            content = None
            try:
                content = completion.choices[0].message.content
            except (AttributeError, IndexError):
                content = str(completion)
            verdict = _parse_verdict(content)
            if verdict is not None:
                return verdict, usage
            if attempt == 1:
                messages = messages + [
                    {"role": "assistant", "content": str(content)},
                    {
                        "role": "user",
                        "content": (
                            "That was not a valid verdict object. Reply with exactly one "
                            'JSON object of the form {"verdict": ..., "reason": ..., "fault": ...}.'
                        ),
                    },
                ]
        return (
            Verdict(verdict="UNSURE", reason="unparseable_verdict", fault=None),
            usage,
        )

    def _record(
        self,
        row: Mapping[str, Any],
        *,
        kind: VerdictKind,
        verdict: Verdict,
        call_site: str,
        kwargs: Optional[Mapping[str, Any]],
        usage: PassUsage,
        wall_ms: int,
    ) -> VerificationRow:
        entry = VerificationRow(
            function_id=int(row["function_id"]),
            function_hash=self.trust_hash(row),
            kind=kind,
            verdict=verdict.verdict,
            reason=verdict.reason,
            fault=verdict.fault,
            call_site=call_site,
            args_signature=args_signature(kwargs) if kwargs is not None else None,
            run_key=self.run_key,
            task_id=self.task_id,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            cost=usage.cost,
            wall_ms=wall_ms,
            created_at=utcnow(),
        )
        self.fm.record_verification_nowait(entry)
        return entry

    # ---- passes ---------------------------------------------------------

    async def static_review(self, row: Mapping[str, Any]) -> Verdict:
        """Judge the source alone; cached per trust hash on the row."""
        current = self.trust_hash(row)
        cached = row.get("static_review")
        if isinstance(cached, dict) and cached.get("function_hash") == current:
            return Verdict(
                verdict=cached.get("verdict", "UNSURE"),
                reason=str(cached.get("reason") or ""),
                fault=("leaf" if cached.get("verdict") == "FAIL" else None),
            )
        prefix, stable, _ = build_static_review_prompt(
            row,
            dependencies=self.dependencies_of(row),
        )
        started = time.perf_counter()
        verdict, usage = await self._judge(
            prefix=prefix,
            stable=stable,
            volatile="",
            origin=f"Verifier.static({row.get('name')})",
        )
        if verdict.verdict == "FAIL":
            verdict = Verdict(verdict="FAIL", reason=verdict.reason, fault="leaf")
        wall_ms = int((time.perf_counter() - started) * 1000)
        self._record(
            row,
            kind=VerdictKind.static,
            verdict=verdict,
            call_site="root",
            kwargs=None,
            usage=usage,
            wall_ms=wall_ms,
        )
        record = StaticReviewRecord(
            verdict=verdict.verdict,
            reason=verdict.reason,
            function_hash=current,
            reviewed_at=utcnow(),
            model=self.model,
        )
        if isinstance(row, dict):
            row["static_review"] = record.model_dump(mode="json")
        self.fm.persist_static_review_nowait(dict(row), record)
        return verdict

    async def args_review(
        self,
        row: Mapping[str, Any],
        *,
        kwargs: Mapping[str, Any],
        stable_block: str,
        call_site: str = "root",
        tier0: Optional[str] = None,
        sibling_results: Sequence[Mapping[str, Any]] = (),
    ) -> Verdict:
        prefix, stable, volatile = build_args_review_prompt(
            stable_block=stable_block,
            kwargs=kwargs,
            tier0=tier0,
            sibling_results=sibling_results,
        )
        started = time.perf_counter()
        verdict, usage = await self._judge(
            prefix=prefix,
            stable=stable,
            volatile=volatile,
            origin=f"Verifier.args({row.get('name')})",
        )
        self._record(
            row,
            kind=VerdictKind.args,
            verdict=verdict,
            call_site=call_site,
            kwargs=kwargs,
            usage=usage,
            wall_ms=int((time.perf_counter() - started) * 1000),
        )
        return verdict

    async def precondition_probe(
        self,
        row: Mapping[str, Any],
        *,
        kwargs: Mapping[str, Any],
        stable_block: str,
        call_site: str = "root",
        sibling_results: Sequence[Mapping[str, Any]] = (),
        max_steps: int = 6,
    ) -> Verdict:
        """Judge whether the world satisfies the precondition; may run read-only probes."""
        prefix, stable, volatile = build_precondition_probe_prompt(
            stable_block=stable_block,
            precondition=row.get("precondition"),
            kwargs=kwargs,
            sibling_results=sibling_results,
        )
        started = time.perf_counter()
        client = self._client(f"Verifier.precondition({row.get('name')})")
        client.set_system_message(prefix)
        usage = PassUsage()
        try:
            handle = start_async_tool_loop(
                client=client,
                message=f"{stable}\n\n{volatile}",
                tools={"run_probe": run_probe},
                loop_id=f"VerifierPrecondition({row.get('name')})",
                max_consecutive_failures=1,
                max_steps=max_steps,
                response_format=Verdict,
                log_steps=False,
            )
            outcome = await handle.result()
            verdict = _parse_verdict(outcome)
            if verdict is None:
                verdict = Verdict(
                    verdict="UNSURE",
                    reason="unparseable_verdict",
                    fault=None,
                )
        except Exception as exc:
            logger.warning("Precondition probe for %s failed: %s", row.get("name"), exc)
            verdict = Verdict(verdict="UNSURE", reason="llm_error", fault=None)
        self._record(
            row,
            kind=VerdictKind.precondition,
            verdict=verdict,
            call_site=call_site,
            kwargs=kwargs,
            usage=usage,
            wall_ms=int((time.perf_counter() - started) * 1000),
        )
        return verdict

    async def post_probe(
        self,
        row: Mapping[str, Any],
        *,
        kwargs: Mapping[str, Any],
        result: Any,
        stable_block: str,
        call_site: str = "root",
        tier0: Optional[str] = None,
        sibling_results: Sequence[Mapping[str, Any]] = (),
        probe_output: Optional[str] = None,
        interactions: Sequence[Mapping[str, Any]] = (),
        kind: VerdictKind = VerdictKind.post,
    ) -> Verdict:
        prefix, stable, volatile = build_post_probe_prompt(
            stable_block=stable_block,
            kwargs=kwargs,
            result=result,
            tier0=tier0,
            sibling_results=sibling_results,
            probe_output=probe_output,
            interactions=interactions,
        )
        started = time.perf_counter()
        verdict, usage = await self._judge(
            prefix=prefix,
            stable=stable,
            volatile=volatile,
            origin=f"Verifier.post({row.get('name')})",
        )
        self._record(
            row,
            kind=kind,
            verdict=verdict,
            call_site=call_site,
            kwargs=kwargs,
            usage=usage,
            wall_ms=int((time.perf_counter() - started) * 1000),
        )
        return verdict


__all__ = [
    "CallSite",
    "Frame",
    "PassUsage",
    "VerifierPasses",
    "current_verification_frames",
    "executed_source",
    "locate_call_site",
    "pushed_frame",
    "run_probe",
]
