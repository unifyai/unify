"""Interactive sandbox for the typed KnowledgeManager claim ledger.

Supports:
• Seeding claims via ``add_knowledge`` (ScenarioBuilder or direct REPL).
• Direct typed CRUD REPL (search / filter / get / add / update / delete /
  invalidate / supersede / reconcile / clear).
• Optional voice I/O via the shared sandbox helpers.
"""

from __future__ import annotations

import asyncio
import ast
import json
import logging
import shlex
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

import unisdk
from sandboxes.scenario_builder import ScenarioBuilder

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from unify.knowledge_manager.knowledge_manager import KnowledgeManager
from sandboxes.utils import (
    record_until_enter as _record_until_enter,
    transcribe_deepgram as _transcribe_deepgram,
    speak as _speak,
    speak_and_wait as _speak_wait,
    build_cli_parser,
    activate_project,
    _wait_for_tts_end as _wait_tts_end,
    configure_sandbox_logging,
)

LG = logging.getLogger("knowledge_sandbox")


def _serialize(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    elif isinstance(value, list) and value and hasattr(value[0], "model_dump"):
        value = [v.model_dump(mode="json") for v in value]
    return json.dumps(value, indent=2, default=str)


def _parse_kwargs(raw: str) -> Dict[str, Any]:
    """Parse ``key=value`` tokens; values are Python literals when possible."""
    if not raw.strip():
        return {}
    kwargs: Dict[str, Any] = {}
    for token in shlex.split(raw):
        if "=" not in token:
            raise ValueError(f"Expected key=value token, got {token!r}")
        key, value = token.split("=", 1)
        try:
            kwargs[key] = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            kwargs[key] = value
    return kwargs


async def _build_scenario(custom: Optional[str] = None) -> None:
    """Seed claims via ``add_knowledge`` / ``search`` using ScenarioBuilder."""
    km = KnowledgeManager()

    description = (
        custom.strip()
        if custom
        else (
            "Generate ~15 diverse typed knowledge claims about electric-vehicle "
            "manufacturers. Cover launch years, battery capacities, warranty "
            "terms and regional sales. Use kind=fact (and a few policy/decision "
            "claims). Attach source_refs with kind=manual. Avoid duplicates by "
            "searching before adding."
        )
    )

    builder = ScenarioBuilder(
        description=description,
        tools={
            "add_knowledge": km.add_knowledge,
            "search": km.search,
            "filter": km.filter,
            "get_knowledge": km.get_knowledge,
        },
    )
    await builder.create()


_COMMANDS_HELP = """
KnowledgeManager sandbox – typed claim ledger REPL. 'quit' to exit.

┌────────────────── accepted commands ──────────────────────────────┐
│ us  {description}   – seed claims via ScenarioBuilder             │
│ usv                 – same, voice description (--voice only)      │
│ search [k=N] [ref=text]                                           │
│ filter [filter="expr"] [offset=N] [limit=N]                       │
│ get knowledge_id=N                                                │
│ add title=... content=... [kind=fact] [topics='["a"]'] ...        │
│ update knowledge_id=N [title=...] [content=...] ...               │
│ delete knowledge_id=N                                             │
│ invalidate knowledge_id=N                                         │
│ supersede old_knowledge_id=N title=... content=...                │
│ reconcile [knowledge_ids='[1,2]']                                 │
│ clear                                                             │
│ save_project | sp   – save project snapshot                       │
│ help | h            – show this help                              │
└───────────────────────────────────────────────────────────────────┘

Examples:
  add title="Battery warranty" content="Eight years" kind=fact topics='["warranty"]'
  search ref="battery warranty" k=5
  filter filter="kind == 'policy'"
  get knowledge_id=1
"""


def _dispatch(km: KnowledgeManager, raw: str) -> Any:
    parts = raw.split(maxsplit=1)
    cmd = parts[0].lower()
    rest = parts[1] if len(parts) > 1 else ""
    kwargs = _parse_kwargs(rest)

    if cmd == "search":
        ref = kwargs.pop("ref", None)
        if ref is not None and "references" not in kwargs:
            kwargs["references"] = {"content": str(ref)}
        return km.search(**kwargs)
    if cmd == "filter":
        return km.filter(**kwargs)
    if cmd in {"get", "get_knowledge"}:
        return km.get_knowledge(**kwargs)
    if cmd in {"add", "add_knowledge"}:
        return km.add_knowledge(**kwargs)
    if cmd in {"update", "update_knowledge"}:
        return km.update_knowledge(**kwargs)
    if cmd in {"delete", "delete_knowledge"}:
        return km.delete_knowledge(**kwargs)
    if cmd in {"invalidate", "invalidate_knowledge"}:
        return km.invalidate_knowledge(**kwargs)
    if cmd in {"supersede", "supersede_knowledge"}:
        return km.supersede_knowledge(**kwargs)
    if cmd in {"reconcile", "reconcile_sources"}:
        return km.reconcile_sources(**kwargs)
    if cmd == "clear":
        km.clear()
        return {"outcome": "cleared"}
    raise ValueError(f"Unknown command {cmd!r}. Type 'help' for the command list.")


async def _main_async() -> None:
    parser = build_cli_parser("KnowledgeManager sandbox")
    args = parser.parse_args()

    activate_project(args.project_name, args.overwrite)

    if args.project_version != -1:
        commits = unisdk.get_project_commits(args.project_name)
        if commits:
            try:
                target = commits[args.project_version]
                unisdk.rollback_project(args.project_name, target["commit_hash"])
                LG.info("[version] Rolled back to commit %s", target["commit_hash"])
            except IndexError:
                LG.warning(
                    "[version] project_version index %s out of range, ignoring",
                    args.project_version,
                )

    configure_sandbox_logging(
        log_in_terminal=args.log_in_terminal,
        log_file=".logs_main.txt",
        tcp_port=args.log_tcp_port,
        http_tcp_port=args.http_log_tcp_port,
        unify_requests_log_file=".logs_unify_requests.txt",
    )
    LG.setLevel(logging.INFO)

    km = KnowledgeManager()

    def _explain_commands() -> None:
        print(_COMMANDS_HELP)

    if args.voice:
        _speak(
            "Sandbox ready. Type typed CRUD commands, or press enter on an empty "
            "line to record. Use u-s-v to seed a scenario vocally.",
        )
        _wait_tts_end()

    while True:
        try:
            print()
            _explain_commands()
            print()

            if args.voice:
                _wait_tts_end()
                raw = input("command ('r' to record)> ").strip()
                if raw.lower() == "r":
                    audio = _record_until_enter()
                    raw = _transcribe_deepgram(audio).strip()
                    if not raw:
                        continue
                    print(f"▶️  {raw}")
            else:
                raw = input("command> ").strip()

            if raw.lower() in {"help", "h", "?"}:
                _explain_commands()
                continue
            if raw.lower() in {"quit", "exit"}:
                break
            if not raw:
                continue

            if raw.lower() in {"save_project", "sp"}:
                commit_hash = unisdk.commit_project(
                    args.project_name,
                    commit_message=f"Sandbox save {datetime.utcnow().isoformat()}",
                ).get("commit_hash")
                print(f"💾 Project saved at commit {commit_hash}")
                if args.voice:
                    _speak("Project saved")
                continue

            parts = raw.split(maxsplit=1)
            cmd_lower = parts[0].lower()

            if cmd_lower in {"us", "update_scenario"}:
                description = parts[1].strip() if len(parts) > 1 else ""
                if not description:
                    description = input(
                        "🧮 Describe the knowledge claims you want to seed > ",
                    ).strip()
                    if not description:
                        print("⚠️  No description provided – cancelled.")
                        continue
                print("[generate] Seeding knowledge claims…")
                try:
                    if args.voice:
                        task = asyncio.create_task(_build_scenario(description))
                        _speak_wait("Got it, seeding your custom claims now.")
                        await task
                        _speak_wait("All done, claims are ready.")
                    else:
                        await _build_scenario(description)
                    print("✓ Scenario seeded.")
                except Exception as exc:
                    LG.error("Scenario generation failed: %s", exc, exc_info=True)
                    print(f"❌  Failed to generate scenario: {exc}")
                continue

            if cmd_lower in {"usv", "update_scenario_vocally"}:
                if not args.voice:
                    print(
                        "⚠️  Voice mode not enabled – restart with --voice or use 'us'.",
                    )
                    continue
                audio = _record_until_enter()
                description = _transcribe_deepgram(audio).strip()
                if not description:
                    print("⚠️  Transcription was empty – please try again.")
                    continue
                print(f"▶️  {description}")
                task = asyncio.create_task(_build_scenario(description))
                _speak_wait("Got it, seeding your custom claims now.")
                try:
                    await task
                    _speak_wait("All done, claims are ready.")
                    print("✓ Scenario seeded.")
                except Exception as exc:
                    LG.error("Scenario generation failed: %s", exc, exc_info=True)
                    print(f"❌  Failed to generate scenario: {exc}")
                continue

            try:
                result = _dispatch(km, raw)
                print(_serialize(result))
                if args.voice:
                    _speak("Done.")
            except Exception as exc:
                print(f"❌  {exc}")
                if args.voice:
                    _speak("That command failed.")
        except (EOFError, KeyboardInterrupt):
            print("\nExiting…")
            break


def main() -> None:
    asyncio.run(_main_async())


if __name__ == "__main__":
    main()
