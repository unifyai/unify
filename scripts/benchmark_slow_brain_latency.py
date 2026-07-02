#!/usr/bin/env python3
"""
Benchmark ConversationManager slow-brain LLM latency with and without images.

Runs a small matrix of representative slow-brain turns (text-only and
screen-share multimodal) through the same path production uses:
``build_brain_spec`` → ``new_llm_client`` → ``single_shot_tool_decision``.

Default models mirror the production slow brain:
- ``deepseek-v4-max@deepseek`` with ``reasoning_effort=high`` (OpenRouter: deepseek-v4-pro)
- ``minimax-v3@minimax`` (OpenRouter: MiniMax-M3)

For realistic ConversationManager timings, prefer the pytest benchmark suite::

    cd unity
    UNILLM_CACHE=false ./scripts/run_slow_brain_model_benchmark.sh

That runs real ``initialized_cm.step_until_wait`` scenarios (patched from existing CM
tests) across both models with 3 repeats each.

    cd unity
    uv run python scripts/benchmark_slow_brain_latency.py
    uv run python scripts/benchmark_slow_brain_latency.py --repeat 3 --warmup 1
    uv run python scripts/benchmark_slow_brain_latency.py --models deepseek-v4-max@deepseek

DeepSeek vision scenarios are expected to fail fast (OpenRouter rejects image
input for deepseek-v4-pro). MiniMax-M3 should succeed on both text and vision.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import io
import json
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

# Match conversation-manager test defaults before unity settings load.
os.environ.setdefault("ASSISTANT_EMAIL", "assistant@benchmark.example.com")
os.environ.setdefault("ASSISTANT_NUMBER", "+15550001000")
os.environ.setdefault("ASSISTANT_WHATSAPP_NUMBER", "+15550001000")
os.environ.setdefault("UNITY_CONVERSATION_JOB_NAME", "slow_brain_benchmark")
os.environ["UNILLM_CACHE"] = "false"
os.environ.setdefault("LITELLM_LOG", "ERROR")

from dotenv import load_dotenv

load_dotenv()

from PIL import Image, ImageDraw
from types import SimpleNamespace

from unify.common.llm_client import new_llm_client
from unify.common.single_shot import single_shot_tool_decision
from unify.conversation_manager.cm_types import Mode, ScreenshotEntry
from unify.conversation_manager.domains.brain import build_brain_spec
from unify.session_details import SESSION_DETAILS, AssistantDetails

DEFAULT_MODELS: tuple[dict[str, Any], ...] = (
    {
        "label": "deepseek-v4-pro (high)",
        "model": "deepseek-v4-max@deepseek",
        "reasoning_effort": "high",
    },
    {
        "label": "MiniMax-M3",
        "model": "minimax-v3@minimax",
        "reasoning_effort": "high",
    },
)

_BOSS = {
    "contact_id": 1,
    "first_name": "Dana",
    "surname": "Owner",
    "phone_number": "+15551234567",
    "email_address": "dana@acme.com",
}


def _text_light_state() -> str:
    return """<notifications>
**NEW** [Inbound SMS from Alice Smith (contact_id=2)]: Can you check whether the Memphis shipment left yet?
</notifications>

<in_flight_actions>
None
</in_flight_actions>

<active_conversations>
Conversation with Alice Smith (contact_id=2):
[Alice Smith | SMS | contact_id=2]: Can you check whether the Memphis shipment left yet?
</active_conversations>"""


def _text_rich_state() -> str:
    return """<notifications>
**NEW** [Inbound email from Bob Chen (contact_id=3)]: Please summarize open tasks and flag anything blocked on vendor data.
**NEW** [Inbound Unify chat from Dana Owner (contact_id=1)]: Also confirm whether the Q2 forecast deck is ready for review.
</notifications>

<in_flight_actions>
- handle_id=8841 | act | status=running | started=2026-07-02T14:10:00Z
  query=Pull the latest vendor SLA spreadsheet from shared drive and compare against last week.
- handle_id=8844 | act | status=paused | started=2026-07-02T13:55:00Z
  query=Draft a customer update email about the delayed Memphis shipment.
</in_flight_actions>

<recent_tool_executions>
- tool=ask_about_contacts | origin=SMSSent | contact_id=2 | result=found Alice Smith
- tool=act | origin=EmailReceived | handle_id=8841 | result=started
</recent_tool_executions>

<active_conversations>
Conversation with Dana Owner (contact_id=1):
[Dana Owner | Unify chat | contact_id=1]: Also confirm whether the Q2 forecast deck is ready for review.

Conversation with Alice Smith (contact_id=2):
[Alice Smith | SMS | contact_id=2]: Any update on Memphis?

Conversation with Bob Chen (contact_id=3):
[Bob Chen | Email | contact_id=3]: Please summarize open tasks and flag anything blocked on vendor data.
</active_conversations>

<tasks>
- task_id=1201 | owner=assistant | status=in_progress | title=Prepare Q2 forecast deck review summary
- task_id=1204 | owner=assistant | status=blocked | title=Vendor SLA comparison for procurement
</tasks>"""


def _make_ui_screenshot_jpeg_b64(
    *,
    width: int = 1280,
    height: int = 720,
    label: str,
    quality: int = 85,
) -> str:
    """Synthetic UI screenshot sized like a typical screen-share JPEG."""

    image = Image.new("RGB", (width, height), color=(32, 36, 44))
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, width, 56), fill=(18, 20, 26))
    draw.rectangle((24, 84, width - 24, height - 24), outline=(96, 104, 120), width=2)
    draw.rectangle((48, 120, 420, 260), fill=(52, 58, 72))
    draw.rectangle((460, 120, width - 48, 360), fill=(45, 50, 63))
    draw.text((36, 18), "Operations Console — Shipments", fill=(230, 233, 240))
    draw.text((64, 150), label, fill=(240, 244, 252))
    draw.text((64, 190), "Memphis DC | Delay risk | 14 pallets", fill=(210, 214, 224))
    draw.text((480, 150), "Status: In transit", fill=(210, 214, 224))
    draw.text((480, 190), "ETA: Jul 3, 18:40 local", fill=(210, 214, 224))
    buffer = io.BytesIO()
    image.save(buffer, format="JPEG", quality=quality)
    return base64.b64encode(buffer.getvalue()).decode("ascii")


class _BenchmarkTools:
    """Side-effect-free slow-brain tool surface (inspired by coordinator eval tests)."""

    async def send_unify_message(
        self,
        *,
        content: str,
        contact_id: int | str,
        attachment_filepath: str | None = None,
    ) -> dict[str, Any]:
        return {
            "status": "sent",
            "contact_id": contact_id,
            "content": content,
            "attachment_filepath": attachment_filepath,
        }

    async def send_api_response(
        self,
        *,
        content: str,
        contact_id: int | str = 1,
        attachment_filepaths: list[str] | None = None,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        return {
            "status": "sent",
            "contact_id": contact_id,
            "content": content,
            "attachment_filepaths": attachment_filepaths,
            "tags": tags,
        }

    async def wait(self, delay: int | None = None) -> dict[str, Any]:
        return {"status": "waiting", "delay": delay}

    async def act(
        self,
        *,
        query: str,
        requesting_contact_id: int,
        response_format: dict[str, Any] | None = None,
        persist: bool = False,
        include_conversation_context: bool = True,
    ) -> dict[str, Any]:
        return {
            "status": "started",
            "handle_id": "benchmark-action",
            "query": query,
            "requesting_contact_id": requesting_contact_id,
            "response_format": response_format,
            "persist": persist,
            "include_conversation_context": include_conversation_context,
        }

    async def ask_about_contacts(self, *, query: str) -> dict[str, Any]:
        return {"status": "answered", "query": query, "answer": "No matches."}

    async def update_contacts(self, *, query: str) -> dict[str, Any]:
        return {"status": "updated", "query": query}

    async def query_past_transcripts(self, *, query: str) -> dict[str, Any]:
        return {"status": "answered", "query": query, "answer": "No transcripts."}

    async def cm_get_mode(self) -> dict[str, Any]:
        return {"mode": "text"}

    async def cm_get_contact(self, *, contact_id: int) -> dict[str, Any]:
        if contact_id == _BOSS["contact_id"]:
            return _BOSS
        return {"contact_id": contact_id}

    async def cm_list_in_flight_actions(self) -> dict[str, Any]:
        return {"actions": []}

    async def cm_list_notifications(self) -> dict[str, Any]:
        return {"notifications": []}

    def as_tools(self) -> dict[str, Callable[..., Any]]:
        return {
            "send_unify_message": self.send_unify_message,
            "send_api_response": self.send_api_response,
            "wait": self.wait,
            "act": self.act,
            "ask_about_contacts": self.ask_about_contacts,
            "update_contacts": self.update_contacts,
            "query_past_transcripts": self.query_past_transcripts,
            "cm_get_mode": self.cm_get_mode,
            "cm_get_contact": self.cm_get_contact,
            "cm_list_in_flight_actions": self.cm_list_in_flight_actions,
            "cm_list_notifications": self.cm_list_notifications,
        }


@dataclass(frozen=True)
class BenchmarkScenario:
    scenario_id: str
    description: str
    state_prompt: str
    screenshots: tuple[ScreenshotEntry, ...] = ()
    screenshot_paths: tuple[str, ...] = ()


@dataclass
class RunResult:
    scenario_id: str
    model_label: str
    model: str
    image_mode: str
    screenshot_count: int
    image_bytes: int
    state_chars: int
    tool_count: int
    elapsed_s: float
    ok: bool
    tools: list[str] = field(default_factory=list)
    error: str | None = None


def _configure_session() -> None:
    SESSION_DETAILS.reset()
    SESSION_DETAILS.assistant = AssistantDetails(
        agent_id=9001,
        first_name="Alex",
        surname="Ops",
        is_coordinator=False,
    )
    SESSION_DETAILS.user.first_name = _BOSS["first_name"]
    SESSION_DETAILS.user.surname = _BOSS["surname"]
    SESSION_DETAILS.boss_contact_id = _BOSS["contact_id"]
    SESSION_DETAILS.org_id = None
    SESSION_DETAILS.team_summaries = []


class _ContactIndex:
    def get_contact(self, contact_id: int) -> dict[str, Any] | None:
        if contact_id == _BOSS["contact_id"]:
            return _BOSS
        return None


def _make_cm() -> SimpleNamespace:
    call_manager = SimpleNamespace(
        is_ready_for_outbound_call=False,
        has_active_google_meet=False,
        has_active_teams_meet=False,
        _call_channel=None,
    )
    return SimpleNamespace(
        initialized=True,
        contact_index=_ContactIndex(),
        mode=Mode.TEXT,
        get_active_contact=lambda: None,
        in_voice_session=False,
        call_manager=call_manager,
        assistant_job_title="Operations assistant",
        assistant_about="Handles shipment tracking, task triage, and customer updates.",
        computer_fast_path_eligible=False,
        assistant_number="+15557654321",
        assistant_email="assistant@acme.com",
        assistant_whatsapp_number="",
        assistant_discord_bot_id="",
        assistant_slack_bot_user_id="",
        assistant_has_teams=False,
        team_summaries=[],
        coordinator_onboarding_deferred=False,
        coordinator_onboarding_render=None,
        onboarding_clicked_trigger_steps=[],
        onboarding_catalog=None,
    )


def _build_scenarios() -> tuple[BenchmarkScenario, ...]:
    one_shot = _make_ui_screenshot_jpeg_b64(label="Memphis shipment board")
    two_a = _make_ui_screenshot_jpeg_b64(label="Screen 1 — shipment list")
    two_b = _make_ui_screenshot_jpeg_b64(label="Screen 2 — delay details")
    now = datetime.now(timezone.utc)

    return (
        BenchmarkScenario(
            scenario_id="text-light-inbound",
            description="Single new inbound SMS; typical wait-or-reply turn (inspired by CM SMS handlers).",
            state_prompt=_text_light_state(),
        ),
        BenchmarkScenario(
            scenario_id="text-rich-state",
            description="Busier snapshot with notifications, in-flight actions, and tasks (renderer-scale state).",
            state_prompt=_text_rich_state(),
        ),
        BenchmarkScenario(
            scenario_id="vision-one-screenshot",
            description="User screen share with one JPEG screenshot (inspired by test_screenshot_to_act / brain multimodal path).",
            state_prompt=_text_light_state(),
            screenshots=(
                ScreenshotEntry(
                    b64=one_shot,
                    utterance="I'm looking at the shipment board — did Memphis leave yet?",
                    timestamp=now,
                    source="user",
                ),
            ),
            screenshot_paths=("Screenshots/User/20260702T141530Z.jpg",),
        ),
        BenchmarkScenario(
            scenario_id="vision-two-screenshots",
            description="Two chronological user screenshots, matching slow-brain screen-share buffering.",
            state_prompt=_text_rich_state(),
            screenshots=(
                ScreenshotEntry(
                    b64=two_a,
                    utterance="This is the shipment list.",
                    timestamp=now,
                    source="user",
                ),
                ScreenshotEntry(
                    b64=two_b,
                    utterance="And this detail pane shows the delay reason.",
                    timestamp=now,
                    source="user",
                ),
            ),
            screenshot_paths=(
                "Screenshots/User/20260702T141530Z.jpg",
                "Screenshots/User/20260702T141545Z.jpg",
            ),
        ),
    )


def _image_bytes_for(scenario: BenchmarkScenario) -> int:
    total = 0
    for entry in scenario.screenshots:
        total += len(entry.b64) * 3 // 4
    return total


def _build_brain_spec_for_scenario(scenario: BenchmarkScenario):
    _configure_session()
    cm = _make_cm()
    snapshot_state = SimpleNamespace(full_render=scenario.state_prompt)
    return build_brain_spec(
        cm,
        snapshot_state=snapshot_state,
        screenshots=list(scenario.screenshots),
        screenshot_paths=list(scenario.screenshot_paths),
    )


async def _run_once(
    *,
    scenario: BenchmarkScenario,
    model_cfg: dict[str, Any],
) -> RunResult:
    brain_spec = _build_brain_spec_for_scenario(scenario)
    tools = _BenchmarkTools().as_tools()
    client_kwargs = {
        key: model_cfg[key]
        for key in ("model", "reasoning_effort", "service_tier")
        if key in model_cfg
    }
    client = new_llm_client(
        **client_kwargs,
        origin="SlowBrainLatencyBenchmark",
    )
    client.set_system_message(brain_spec.system_prompt.to_list())
    client.set_prompt_caching(["system"])

    started = time.perf_counter()
    try:
        result = await single_shot_tool_decision(
            client,
            [brain_spec.state_message()],
            tools,
            tool_choice="required",
            response_format=brain_spec.response_model,
            exclusive_tools={
                "make_call",
                "make_whatsapp_call",
                "join_google_meet",
                "join_teams_meet",
            },
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return RunResult(
            scenario_id=scenario.scenario_id,
            model_label=model_cfg["label"],
            model=model_cfg["model"],
            image_mode="with_images" if scenario.screenshots else "text_only",
            screenshot_count=len(scenario.screenshots),
            image_bytes=_image_bytes_for(scenario),
            state_chars=len(brain_spec.state_prompt),
            tool_count=len(tools),
            elapsed_s=elapsed,
            ok=False,
            error=f"{type(exc).__name__}: {exc}",
        )

    elapsed = time.perf_counter() - started
    tool_names = [tool.name for tool in result.tools]
    if not tool_names and result.structured_output is not None:
        tool_names = ["<structured_thoughts_only>"]
    return RunResult(
        scenario_id=scenario.scenario_id,
        model_label=model_cfg["label"],
        model=model_cfg["model"],
        image_mode="with_images" if scenario.screenshots else "text_only",
        screenshot_count=len(scenario.screenshots),
        image_bytes=_image_bytes_for(scenario),
        state_chars=len(brain_spec.state_prompt),
        tool_count=len(tools),
        elapsed_s=elapsed,
        ok=True,
        tools=tool_names,
    )


def _print_table(results: list[RunResult]) -> None:
    headers = (
        "scenario",
        "model",
        "mode",
        "shots",
        "state",
        "img_kb",
        "tools",
        "sec",
        "status",
        "called",
    )
    rows: list[list[str]] = []
    for result in results:
        rows.append(
            [
                result.scenario_id,
                result.model_label,
                result.image_mode,
                str(result.screenshot_count),
                str(result.state_chars),
                str(result.image_bytes // 1024),
                str(result.tool_count),
                f"{result.elapsed_s:.2f}",
                "ok" if result.ok else "ERR",
                ",".join(result.tools) if result.tools else (result.error or "-"),
            ],
        )

    widths = [len(header) for header in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    def _fmt(cells: list[str]) -> str:
        return "  ".join(cell.ljust(widths[idx]) for idx, cell in enumerate(cells))

    print(_fmt(list(headers)))
    print(_fmt(["-" * width for width in widths]))
    for row in rows:
        print(_fmt(row))


def _print_summary(results: list[RunResult]) -> None:
    print("\nSummary (median seconds by model / image mode / scenario):")
    groups: dict[tuple[str, str, str], list[float]] = {}
    for result in results:
        if not result.ok:
            continue
        key = (result.model_label, result.image_mode, result.scenario_id)
        groups.setdefault(key, []).append(result.elapsed_s)

    for key in sorted(groups):
        values = groups[key]
        label, mode, scenario = key
        print(
            f"  {label:24} {mode:12} {scenario:24} "
            f"median={statistics.median(values):.2f}s "
            f"mean={statistics.mean(values):.2f}s n={len(values)}",
        )

    print(
        "\nVision overhead vs text-only (median delta, same model + scenario family):",
    )
    text_ids = {"text-light-inbound", "text-rich-state"}
    vision_map = {
        "text-light-inbound": "vision-one-screenshot",
        "text-rich-state": "vision-two-screenshots",
    }
    for model_label in sorted({result.model_label for result in results}):
        for text_id, vision_id in vision_map.items():
            text_vals = [
                result.elapsed_s
                for result in results
                if result.ok
                and result.model_label == model_label
                and result.scenario_id == text_id
            ]
            vision_vals = [
                result.elapsed_s
                for result in results
                if result.ok
                and result.model_label == model_label
                and result.scenario_id == vision_id
            ]
            if not text_vals or not vision_vals:
                continue
            delta = statistics.median(vision_vals) - statistics.median(text_vals)
            print(
                f"  {model_label:24} {text_id} -> {vision_id}: "
                f"+{delta:.2f}s ({delta / statistics.median(text_vals) * 100:.0f}%)",
            )

    failures = [result for result in results if not result.ok]
    if failures:
        print("\nFailures:")
        for result in failures:
            print(
                f"  {result.model_label} {result.scenario_id}: {result.error}",
            )


def _parse_models(raw: list[str]) -> list[dict[str, Any]]:
    if not raw:
        return [dict(model) for model in DEFAULT_MODELS]

    parsed: list[dict[str, Any]] = []
    for item in raw:
        if item.startswith("deepseek"):
            parsed.append(
                {
                    "label": "deepseek-v4-pro (high)",
                    "model": item,
                    "reasoning_effort": "high",
                },
            )
        elif item.startswith("minimax"):
            parsed.append(
                {
                    "label": "MiniMax-M3",
                    "model": item,
                    "reasoning_effort": "high",
                },
            )
        else:
            parsed.append({"label": item, "model": item, "reasoning_effort": "high"})
    return parsed


async def _async_main(args: argparse.Namespace) -> int:
    scenarios = _build_scenarios()
    if args.scenario:
        selected = {scenario.scenario_id for scenario in scenarios}
        missing = set(args.scenario) - selected
        if missing:
            print(f"Unknown scenarios: {', '.join(sorted(missing))}", file=sys.stderr)
            return 2
        scenarios = tuple(s for s in scenarios if s.scenario_id in args.scenario)

    models = _parse_models(args.models)
    if args.dry_run:
        print("Dry run — no API calls.")
        print("Models:")
        for model in models:
            print(f"  - {model['label']} ({model['model']})")
        print("Scenarios:")
        for scenario in scenarios:
            print(
                f"  - {scenario.scenario_id}: {scenario.description} "
                f"(screenshots={len(scenario.screenshots)}, "
                f"state_chars={len(scenario.state_prompt)})",
            )
        return 0

    results: list[RunResult] = []
    for model_cfg in models:
        if args.warmup:
            warmup = scenarios[0]
            print(
                f"Warmup: {model_cfg['label']} on {warmup.scenario_id} ...",
                flush=True,
            )
            await _run_once(scenario=warmup, model_cfg=model_cfg)

        for scenario in scenarios:
            for attempt in range(1, args.repeat + 1):
                label = (
                    f"{model_cfg['label']} | {scenario.scenario_id} "
                    f"| attempt {attempt}/{args.repeat}"
                )
                print(f"Running {label} ...", flush=True)
                result = await _run_once(scenario=scenario, model_cfg=model_cfg)
                results.append(result)
                status = "ok" if result.ok else "FAILED"
                print(
                    f"  {status} in {result.elapsed_s:.2f}s "
                    f"tools={result.tools or result.error}",
                    flush=True,
                )

    _print_table(results)
    _print_summary(results)

    if args.json_out:
        payload = [
            {
                "scenario_id": result.scenario_id,
                "model_label": result.model_label,
                "model": result.model,
                "image_mode": result.image_mode,
                "screenshot_count": result.screenshot_count,
                "image_bytes": result.image_bytes,
                "state_chars": result.state_chars,
                "tool_count": result.tool_count,
                "elapsed_s": result.elapsed_s,
                "ok": result.ok,
                "tools": result.tools,
                "error": result.error,
            }
            for result in results
        ]
        args.json_out.write_text(json.dumps(payload, indent=2) + "\n")
        print(f"\nWrote {args.json_out}")

    return 0 if all(result.ok for result in results) else 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--models",
        nargs="*",
        default=[],
        help="Model endpoints (default: deepseek-v4-max@deepseek and minimax-v3@minimax).",
    )
    parser.add_argument(
        "--scenario",
        nargs="*",
        default=[],
        help="Subset of scenario ids (default: all).",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=2,
        help="Timed repetitions per scenario/model (default: 2).",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="Untimed warmup runs per model (default: 1). Use 0 to disable.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the benchmark matrix without calling providers.",
    )
    parser.add_argument(
        "--json-out",
        type=argparse.FileType("w"),
        default=None,
        help="Optional path to write raw timings as JSON.",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_async_main(args)))


if __name__ == "__main__":
    main()
