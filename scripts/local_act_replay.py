#!/usr/bin/env python3
"""
scripts/local_act_replay.py
============================

Local act-replay harness for measuring the cache-stability work (T1-T6,
[[execution-run-cache-stability]]) against the production baseline documented
in [[support-ticket-santos2124-usage-investigation]]: $10.91 billed / $9.09
raw, 72 LLM calls, 2.84M prompt tokens, parent-loop cache-hit 0% after turn 2,
for one NotebookLM connector research question.

Drives a real ``CodeActActor.act()`` against a LOCAL Orchestra instance —
never staging, never hosted. No deployment step is involved.

Prerequisites
-------------
1. Local Orchestra running:  ``orchestra/scripts/local.sh start``
   (never ``DEPLOY_ENV=staging`` — that points at hosted infra).
2. A capped OpenRouter key exported for this shell only, never written to a
   file or committed::

       export OPENROUTER_API_KEY=sk-or-...

Usage
-----
    .venv/bin/python scripts/local_act_replay.py --label cold
    .venv/bin/python scripts/local_act_replay.py --label warm

Each invocation is a fresh process (by design — see the module docstring in
the coordinator brief: OpenRouter's provider-side prompt cache is what "warm"
exercises, not any state kept inside this script). Per-run output lands in
``logs/local_act_replay/<label>/``: a ``calls.csv`` ledger and a
``summary.txt`` report.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# A stale, empty `unify/` leftover in .venv/lib/.../site-packages (no
# __init__.py, pre-existing in this checkout) resolves as a broken PEP 420
# namespace package and shadows the real editable-install package whenever
# sys.path[0] is this script's own directory rather than the repo root —
# exactly what happens when this file is invoked directly. Pinning the repo
# root ahead of site-packages makes `import unify` resolve to the real
# package regardless of invocation style, without touching the shared venv.
sys.path.insert(0, str(REPO_ROOT))

# The exact question the production twin (agent 526) answered — verbatim,
# so a byte-for-byte-comparable request shape reaches the model.
QUESTION = (
    "Research the current NotebookLM connector requirements. Determine "
    "whether the connector can connect directly to a standard personal "
    "Google account's NotebookLM, or whether it requires NotebookLM "
    "Enterprise and/or a Google Cloud project. Read-only research."
)

# Budget guard: abort the run rather than let a runaway fan-out burn the
# capped test key. ~$4 raw on terra ~= ~$8 sol-equivalent, comfortably above
# the $9.09-raw baseline so a healthy run never trips it, but a regression
# that reproduces the old fan-out cost will.
RAW_COST_ABORT_THRESHOLD_USD = 4.0

# How long to wait for the post-act StorageCheck/learning phase after the
# task answer is already in hand (baseline: ~2 min of the 12.7 min run).
STORAGE_PHASE_TIMEOUT_S = 360.0

# Acceptance thresholds from the coordinator brief.
PARENT_LOOP_CACHE_HIT_TARGET = 0.80
PER_ACT_COST_TARGET_USD = 2.0
MAX_SINGLE_CALL_PROMPT_TOKENS = 20_000

# terra is exactly half sol's rates (2.50/15 vs 5/30 input/output,
# cache-read 0.25 vs 0.50) — multiply raw terra cost by 2 for the
# sol-equivalent figure the baseline was measured in.
SOL_EQUIVALENT_MULTIPLIER = 2.0


# ---------------------------------------------------------------------------
# Environment bootstrap — must happen before importing unify/unillm, since
# both read process-level settings (provider keys, UNIFY_MODEL, manager
# IMPL selection) at import time.
# ---------------------------------------------------------------------------


def _bootstrap_env(model: str) -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env")
    except ImportError:
        pass

    if not os.environ.get("OPENROUTER_API_KEY"):
        raise SystemExit(
            "OPENROUTER_API_KEY must be exported in this shell (never written "
            "to a file or committed) before running this harness.",
        )

    os.environ.setdefault("ORCHESTRA_URL", "http://localhost:8000/v0")
    os.environ.setdefault("UNIFY_KEY", "local-test-api-key")
    # Unassigned identity — same local-Orchestra pattern tests/flows/conftest.py
    # uses: any context path under the authenticated test tenant is writable
    # without a pre-existing assistant row.
    os.environ["ASSISTANT_ID"] = ""
    os.environ.setdefault("USER_ID", "default")
    os.environ.setdefault("SELF_HOST", "1")
    os.environ["UNIFY_MODEL"] = model
    os.environ.setdefault("UNIFY_TRANSCRIPT_INVARIANT_CHECKS", "1")

    # Real (not simulated) manager backends, mirroring
    # sandboxes/conversation_manager/actor_factory.py::_apply_manager_impl_env
    # so FunctionManager/GuidanceManager/KnowledgeManager persist to local
    # Orchestra instead of an in-memory stub.
    for var in (
        "UNITY_CONTACT_IMPL",
        "UNITY_TRANSCRIPT_IMPL",
        "UNITY_TASK_IMPL",
        "UNITY_KNOWLEDGE_IMPL",
        "UNITY_GUIDANCE_IMPL",
        "UNITY_SECRET_IMPL",
        "UNITY_WEB_IMPL",
        "UNITY_FILE_IMPL",
        "UNITY_DATA_IMPL",
        "UNITY_FUNCTION_IMPL",
        "UNITY_CONVERSATION_IMPL",
        "UNITY_MEMORY_IMPL",
        "UNITY_CONFIG_IMPL",
    ):
        os.environ.setdefault(var, "real")


# ---------------------------------------------------------------------------
# Per-call recorder
# ---------------------------------------------------------------------------


@dataclass
class CallRecord:
    ts: float
    elapsed_s: float
    model: str
    prompt_tokens: int
    completion_tokens: int
    cached_tokens: int
    cost_usd: float
    label: str | None
    source: str | None
    lineage: str
    phase: str


@dataclass
class Recorder:
    t0: float
    records: list[CallRecord] = field(default_factory=list)

    def add(self, rec: CallRecord) -> None:
        self.records.append(rec)

    def total_cost(self) -> float:
        return sum(r.cost_usd for r in self.records)


def _derive_phase(lineage: list[str]) -> str:
    joined = "->".join(lineage)
    if "StorageCheck" in joined or "ProactiveStorage" in joined:
        return "post_act_storage"
    if "WebSearcher" in joined:
        return "web_ask"
    n_actor_loops = joined.count("CodeActActor.act")
    if n_actor_loops >= 2:
        return "sub_agent_actor"
    if n_actor_loops == 1:
        return "main_act_loop"
    return "other"


def install_cost_hooks(recorder: Recorder) -> None:
    """Wrap unillm's per-call cost computation to also record cache/lineage.

    Monkeypatches the two functions the coordinator brief pointed at
    (``uni_llm._provider_cost_from_stream_usage`` for streamed responses,
    ``uni_llm.compute_cost_from_response`` for non-streamed ones — imported
    into ``uni_llm``'s own module namespace, so patching it there — not on
    ``unillm.costs`` — is what actually intercepts every call site). Original
    behavior (including the real billed cost) is preserved untouched; this
    only adds an observation side effect. unillm itself is never modified.
    """
    import unillm.clients.uni_llm as uni_llm_mod
    from unillm.costs import _get_nested_attr
    from unillm.billing_context import get_billing_context
    from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE

    orig_compute_cost_from_response = uni_llm_mod.compute_cost_from_response
    orig_provider_cost_from_stream_usage = uni_llm_mod._provider_cost_from_stream_usage

    def _cached_tokens(usage: object) -> int:
        if usage is None:
            return 0
        ct = _get_nested_attr(
            usage,
            "prompt_tokens_details",
            "cached_tokens",
        ) or _get_nested_attr(usage, "cache_read_input_tokens")
        try:
            return int(ct or 0)
        except (TypeError, ValueError):
            return 0

    def _record(
        *,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        cached_tokens: int,
        cost: float | None,
    ) -> None:
        try:
            ctx = get_billing_context()
            lineage = list(TOOL_LOOP_LINEAGE.get([]) or [])
            now = time.time()
            recorder.add(
                CallRecord(
                    ts=now,
                    elapsed_s=now - recorder.t0,
                    model=model,
                    prompt_tokens=int(prompt_tokens or 0),
                    completion_tokens=int(completion_tokens or 0),
                    cached_tokens=int(cached_tokens or 0),
                    cost_usd=float(cost or 0.0),
                    label=ctx.label,
                    source=ctx.source,
                    lineage="->".join(lineage),
                    phase=_derive_phase(lineage),
                ),
            )
        except Exception as exc:  # observation must never break billing
            print(
                f"[local_act_replay] WARNING: failed to record call: {exc}",
                file=sys.stderr,
            )

    def patched_compute_cost_from_response(
        model: str,
        response: object,
    ) -> float | None:
        cost = orig_compute_cost_from_response(model, response)
        usage = getattr(response, "usage", None)
        if usage is None and isinstance(response, dict):
            usage = response.get("usage")
        if usage is not None:
            pt = getattr(usage, "prompt_tokens", None)
            if pt is None and isinstance(usage, dict):
                pt = usage.get("prompt_tokens", 0)
            ct = getattr(usage, "completion_tokens", None)
            if ct is None and isinstance(usage, dict):
                ct = usage.get("completion_tokens", 0)
            _record(
                model=model,
                prompt_tokens=pt or 0,
                completion_tokens=ct or 0,
                cached_tokens=_cached_tokens(usage),
                cost=cost,
            )
        return cost

    def patched_provider_cost_from_stream_usage(
        accounting_model: str,
        usage_info: object,
    ) -> tuple[float | None, int, int]:
        cost, prompt_tokens, completion_tokens = orig_provider_cost_from_stream_usage(
            accounting_model,
            usage_info,
        )
        _record(
            model=accounting_model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            cached_tokens=_cached_tokens(usage_info),
            cost=cost,
        )
        return cost, prompt_tokens, completion_tokens

    uni_llm_mod.compute_cost_from_response = patched_compute_cost_from_response
    uni_llm_mod._provider_cost_from_stream_usage = (
        patched_provider_cost_from_stream_usage
    )


# ---------------------------------------------------------------------------
# CSV + summary output
# ---------------------------------------------------------------------------


def write_csv(recorder: Recorder, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "elapsed_s",
                "model",
                "prompt_tokens",
                "completion_tokens",
                "cached_tokens",
                "cache_hit_rate",
                "cost_usd",
                "label",
                "source",
                "phase",
                "lineage",
            ],
        )
        for r in recorder.records:
            hit_rate = (r.cached_tokens / r.prompt_tokens) if r.prompt_tokens else 0.0
            writer.writerow(
                [
                    f"{r.elapsed_s:.3f}",
                    r.model,
                    r.prompt_tokens,
                    r.completion_tokens,
                    r.cached_tokens,
                    f"{hit_rate:.4f}",
                    f"{r.cost_usd:.6f}",
                    r.label or "",
                    r.source or "",
                    r.phase,
                    r.lineage,
                ],
            )


def _phase_breakdown(recorder: Recorder) -> dict[str, dict]:
    phases: dict[str, dict] = {}
    for r in recorder.records:
        p = phases.setdefault(
            r.phase,
            {"calls": 0, "prompt_tokens": 0, "cached_tokens": 0, "cost": 0.0},
        )
        p["calls"] += 1
        p["prompt_tokens"] += r.prompt_tokens
        p["cached_tokens"] += r.cached_tokens
        p["cost"] += r.cost_usd
    return phases


def build_summary(
    recorder: Recorder,
    *,
    label: str,
    wall_clock_s: float,
    model: str,
) -> str:
    lines: list[str] = []
    total_calls = len(recorder.records)
    total_prompt = sum(r.prompt_tokens for r in recorder.records)
    total_completion = sum(r.completion_tokens for r in recorder.records)
    total_cached = sum(r.cached_tokens for r in recorder.records)
    total_cost_raw = sum(r.cost_usd for r in recorder.records)
    overall_hit_rate = (total_cached / total_prompt) if total_prompt else 0.0
    # terra is exactly half sol's rates; a run already on sol needs no
    # multiplier (applying x2 there would double-count). Anything else is
    # reported unmultiplied with a note, rather than silently assuming terra.
    if "gpt-5.6-terra" in model:
        sol_multiplier = SOL_EQUIVALENT_MULTIPLIER
    elif "gpt-5.6-sol" in model:
        sol_multiplier = 1.0
    else:
        sol_multiplier = 1.0
    sol_equiv_cost = total_cost_raw * sol_multiplier

    lines.append(f"=== local_act_replay summary — run '{label}' ===")
    lines.append(f"model: {model}")
    lines.append(f"wall clock: {wall_clock_s:.1f}s")
    lines.append(f"total calls: {total_calls}")
    lines.append(f"total prompt tokens: {total_prompt:,}")
    lines.append(f"total completion tokens: {total_completion:,}")
    lines.append(f"total cached (cache-read) tokens: {total_cached:,}")
    lines.append(f"overall cache-hit rate: {overall_hit_rate:.1%}")
    lines.append(f"total cost (raw, {model}): ${total_cost_raw:.4f}")
    if sol_multiplier != 1.0:
        lines.append(
            f"total cost (sol-equivalent, x{sol_multiplier:.0f}): ${sol_equiv_cost:.4f}",
        )
    else:
        lines.append("total cost (sol-equivalent): same as raw — already sol pricing")
    lines.append("")

    lines.append("--- per-phase breakdown ---")
    phases = _phase_breakdown(recorder)
    for phase, stats in sorted(phases.items(), key=lambda kv: -kv[1]["cost"]):
        hit = (
            (stats["cached_tokens"] / stats["prompt_tokens"])
            if stats["prompt_tokens"]
            else 0.0
        )
        lines.append(
            f"  {phase:18s} calls={stats['calls']:3d}  "
            f"prompt_tok={stats['prompt_tokens']:>9,}  "
            f"cache_hit={hit:6.1%}  "
            f"cost=${stats['cost']:.4f}",
        )
    lines.append("")

    sub_agent_records = [r for r in recorder.records if r.phase == "sub_agent_actor"]
    if sub_agent_records:
        lines.append("--- sub-agent fan-out analysis ---")
        by_sub_agent: dict[str, list[CallRecord]] = {}
        for r in sub_agent_records:
            # The distinct sub-agent instance is its own "CodeActActor.act(xxx)"
            # lineage segment — the last one, since a sub-agent can itself
            # spawn deeper sub-agents.
            segments = r.lineage.split("->")
            sub_agent_segments = [
                s for s in segments if s.startswith("CodeActActor.act(")
            ]
            key = sub_agent_segments[-1] if sub_agent_segments else "unknown"
            by_sub_agent.setdefault(key, []).append(r)
        lines.append(
            f"  {len(by_sub_agent)} distinct sub-agent(s) spawned via primitives.actor.act",
        )
        for key, recs in by_sub_agent.items():
            sub_prompt = sum(r.prompt_tokens for r in recs)
            sub_cached = sum(r.cached_tokens for r in recs)
            sub_cost = sum(r.cost_usd for r in recs)
            sub_hit = (sub_cached / sub_prompt) if sub_prompt else 0.0
            first = recs[0]
            lines.append(
                f"  {key}: calls={len(recs)}  prompt_tok={sub_prompt:,}  "
                f"cache_hit={sub_hit:.1%}  cost=${sub_cost:.4f}  "
                f"first_call={first.prompt_tokens:,}tok/{first.cached_tokens:,}cached "
                "(fat-prompt respawn check)",
            )
        lines.append("")
    else:
        lines.append(
            "--- sub-agent fan-out analysis --- \n"
            "  no primitives.actor.act sub-agent spawn observed in this run "
            "(0 calls tagged sub_agent_actor)",
        )
        lines.append("")

    lines.append("--- worst 5 calls by cost ---")
    worst = sorted(recorder.records, key=lambda r: -r.cost_usd)[:5]
    for r in worst:
        hit = (r.cached_tokens / r.prompt_tokens) if r.prompt_tokens else 0.0
        lines.append(
            f"  ${r.cost_usd:.4f}  prompt={r.prompt_tokens:>7,}  "
            f"cached={r.cached_tokens:>7,} ({hit:.0%})  "
            f"phase={r.phase:16s}  label={r.label!r}",
        )
    lines.append("")

    lines.append("--- acceptance thresholds ---")
    main_loop = phases.get(
        "main_act_loop",
        {"calls": 0, "prompt_tokens": 0, "cached_tokens": 0},
    )
    main_loop_hit = (
        (main_loop["cached_tokens"] / main_loop["prompt_tokens"])
        if main_loop["prompt_tokens"]
        else 0.0
    )
    max_prompt_tokens = max((r.prompt_tokens for r in recorder.records), default=0)
    max_prompt_call = max(recorder.records, key=lambda r: r.prompt_tokens, default=None)

    def _check(ok: bool) -> str:
        return "PASS" if ok else "FAIL"

    lines.append(
        f"  [{_check(main_loop_hit > PARENT_LOOP_CACHE_HIT_TARGET)}] "
        f"parent-loop cache-hit >{PARENT_LOOP_CACHE_HIT_TARGET:.0%}: "
        f"{main_loop_hit:.1%} ({main_loop['calls']} calls)",
    )
    lines.append(
        f"  [{_check(total_cost_raw <= PER_ACT_COST_TARGET_USD)}] "
        f"per-act cost <= ${PER_ACT_COST_TARGET_USD:.2f}: ${total_cost_raw:.4f} raw "
        f"(${sol_equiv_cost:.4f} sol-equivalent)",
    )
    lines.append(
        f"  [{_check(max_prompt_tokens <= MAX_SINGLE_CALL_PROMPT_TOKENS)}] "
        f"no call >{MAX_SINGLE_CALL_PROMPT_TOKENS:,} prompt tokens: "
        f"max={max_prompt_tokens:,}"
        + (
            f" (phase={max_prompt_call.phase}, label={max_prompt_call.label!r})"
            if max_prompt_call
            else ""
        ),
    )
    lines.append("")

    lines.append("--- vs. production baseline (santos2124 post-diet twin) ---")
    lines.append(
        "  baseline: $10.91 billed / $9.09 raw, 72 calls, 2.84M prompt tokens, "
        "parent-loop 0% cache after turn 2 (model: gpt-5.6-sol)",
    )
    lines.append(
        f"  this run: ${total_cost_raw:.4f} raw {model} "
        f"(${sol_equiv_cost:.4f} sol-equivalent), {total_calls} calls, "
        f"{total_prompt:,} prompt tokens, parent-loop cache-hit {main_loop_hit:.1%}",
    )
    if sol_equiv_cost > 0:
        lines.append(
            f"  sol-equivalent cost delta vs $9.09 raw baseline: "
            f"{(sol_equiv_cost / 9.09 - 1) * 100:+.1f}%",
        )
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main run
# ---------------------------------------------------------------------------


async def run_once(
    *,
    label: str,
    model: str,
    out_dir: Path,
    abort_usd: float = RAW_COST_ABORT_THRESHOLD_USD,
) -> None:
    import unify
    from unify.session_details import SESSION_DETAILS

    SESSION_DETAILS.reset()
    SESSION_DETAILS.populate_from_env()
    unify.init(project_name="Assistants")

    from unify.actor.code_act_actor import CodeActActor
    from unify.actor.environments import StateManagerEnvironment
    from unify.function_manager.primitives import Primitives, PrimitiveScope
    from unify.function_manager.function_manager import FunctionManager
    from unify.guidance_manager.guidance_manager import GuidanceManager
    from unify.knowledge_manager.knowledge_manager import KnowledgeManager

    recorder = Recorder(t0=time.time())
    install_cost_hooks(recorder)

    # Scoped to web + computer (the prod fan-out's prompt_functions) plus
    # actor, so the top-level loop keeps the ability to spawn sub-agents via
    # primitives.actor.act the way the prod twin's top-level loop did.
    scope = PrimitiveScope(scoped_managers=frozenset({"web", "computer", "actor"}))
    primitives = Primitives(primitive_scope=scope)
    env = StateManagerEnvironment(primitives)

    function_manager = FunctionManager()
    guidance_manager = GuidanceManager()
    knowledge_manager = KnowledgeManager()

    actor = CodeActActor(
        environments=[env],
        function_manager=function_manager,
        guidance_manager=guidance_manager,
        knowledge_manager=knowledge_manager,
        can_compose=True,
    )

    aborted = False

    async def watchdog(handle) -> None:
        nonlocal aborted
        while True:
            await asyncio.sleep(2.0)
            if recorder.total_cost() > abort_usd:
                aborted = True
                print(
                    f"[local_act_replay] BUDGET GUARD: raw cost "
                    f"${recorder.total_cost():.2f} exceeded "
                    f"${abort_usd:.2f} — stopping run '{label}'.",
                    file=sys.stderr,
                )
                try:
                    await handle.stop(reason="local_act_replay budget guard exceeded")
                except Exception as exc:
                    print(f"[local_act_replay] stop() failed: {exc}", file=sys.stderr)
                return

    result = None
    error: Exception | None = None
    storage_timed_out = False
    t_start = time.time()
    try:
        handle = await actor.act(
            QUESTION,
            clarification_enabled=False,
            can_store=True,
        )
        watcher = asyncio.create_task(watchdog(handle))
        try:
            # handle.result() (BaseCodeActActor's _StorageCheckHandle) only
            # waits on the *task* phase (_task_done_event) — the post-act
            # StorageCheck/learning phase keeps running in the background
            # after result() returns. Explicitly poll handle.done() (set
            # only once BOTH phases finish) so this harness actually
            # captures the storage/learning pipeline's LLM calls, per
            # tests/actor/code_act/test_storage_venv_integration.py's
            # sanctioned wait pattern — otherwise those calls fire after
            # this process has already torn down its event loop and are
            # silently lost.
            result = await handle.result()
            deadline = time.time() + STORAGE_PHASE_TIMEOUT_S
            while not handle.done():
                if aborted:
                    break
                if time.time() > deadline:
                    storage_timed_out = True
                    print(
                        f"[local_act_replay] WARNING: storage/learning phase "
                        f"did not complete within {STORAGE_PHASE_TIMEOUT_S:.0f}s; "
                        "recorded cost is a lower bound.",
                        file=sys.stderr,
                    )
                    break
                await asyncio.sleep(0.5)
        finally:
            watcher.cancel()
            try:
                await watcher
            except (asyncio.CancelledError, Exception):
                pass
    except Exception as exc:
        error = exc
    finally:
        try:
            await actor.close()
        except Exception:
            pass
    wall_clock_s = time.time() - t_start

    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(recorder, out_dir / "calls.csv")
    summary = build_summary(
        recorder,
        label=label,
        wall_clock_s=wall_clock_s,
        model=model,
    )
    if aborted:
        summary += "\n*** RUN ABORTED BY BUDGET GUARD ***\n"
    if storage_timed_out:
        summary += (
            f"\n*** STORAGE/LEARNING PHASE DID NOT FINISH WITHIN "
            f"{STORAGE_PHASE_TIMEOUT_S:.0f}s — recorded cost is a LOWER BOUND ***\n"
        )
    if error is not None:
        summary += f"\n*** RUN FAILED: {type(error).__name__}: {error} ***\n"
    (out_dir / "summary.txt").write_text(summary)
    print(summary)
    if result is not None:
        print("--- answer ---")
        print(result)
    if error is not None:
        raise error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--label",
        required=True,
        help="Run label, e.g. 'cold' or 'warm'.",
    )
    parser.add_argument(
        "--model",
        default=os.environ.get("UNIFY_MODEL", "openai/gpt-5.6-terra@openrouter"),
        help="LLM endpoint to run against (default: env UNIFY_MODEL or terra).",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory (default: logs/local_act_replay/<label>/).",
    )
    parser.add_argument(
        "--abort-usd",
        type=float,
        default=RAW_COST_ABORT_THRESHOLD_USD,
        help=f"Raw-cost budget guard threshold (default: ${RAW_COST_ABORT_THRESHOLD_USD:.2f}).",
    )
    args = parser.parse_args()

    _bootstrap_env(args.model)

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else REPO_ROOT / "logs" / "local_act_replay" / args.label
    )

    asyncio.run(
        run_once(
            label=args.label,
            model=args.model,
            out_dir=out_dir,
            abort_usd=args.abort_usd,
        ),
    )


if __name__ == "__main__":
    main()
