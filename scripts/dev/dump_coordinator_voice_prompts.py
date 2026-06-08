#!/usr/bin/env python3
"""Dump production Conversation Manager prompts for voice calls.

Prints the exact system prompts that production code builds for:
- slow brain (Main CM Brain) — ``build_brain_spec`` → ``build_system_prompt``
- fast brain (Voice Agent / TTS) — ``call.py`` entrypoint → ``build_voice_agent_prompt``

These are the same builder functions and kwargs wiring used at runtime; only
contact/bio placeholders differ unless you pass real values on the CLI.

Two personas are supported via ``--persona``:
- ``coordinator`` (default) renders the Org Coordinator prompts using the
  live ``COORDINATOR_BIO`` from ``orchestra-coordinator``.
- ``regular`` renders a regular assistant's prompts with ``is_coordinator=False``,
  no authorized-humans roster, and a short generic placeholder bio so the
  surrounding scaffolding is what is visible in the dump.

Usage
-----
    .venv/bin/python scripts/dev/dump_coordinator_voice_prompts.py
    .venv/bin/python scripts/dev/dump_coordinator_voice_prompts.py --persona regular
    .venv/bin/python scripts/dev/dump_coordinator_voice_prompts.py --workspace personal
    .venv/bin/python scripts/dev/dump_coordinator_voice_prompts.py --include-sample-state
    .venv/bin/python scripts/dev/dump_coordinator_voice_prompts.py --write-dir /tmp/prompts
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from textwrap import dedent

# Allow running from repo root without installing the package.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from unity.conversation_manager.domains.contact_index import ContactIndex
from unity.conversation_manager.domains.renderer import Renderer
from unity.conversation_manager.prompt_builders import (
    build_system_prompt,
    build_voice_agent_prompt,
)
from unity.session_details import SESSION_DETAILS, TeamSummary
from unity.settings import SETTINGS

# Source of truth: orchestra/services/coordinator_personas.py (assistants.about at provision time).
_ORCHESTRA_PERSONAS_PATH = (
    _REPO_ROOT.parent
    / "orchestra-coordinator"
    / "orchestra"
    / "services"
    / "coordinator_personas.py"
)


def _load_coordinator_bio() -> str:
    if not _ORCHESTRA_PERSONAS_PATH.is_file():
        raise SystemExit(
            "Cannot load Coordinator bio: expected orchestra-coordinator at "
            f"{_ORCHESTRA_PERSONAS_PATH.parents[2]}\n"
            "Clone https://github.com/unifyai/orchestra-coordinator alongside unity-coordinator.",
        )

    spec = importlib.util.spec_from_file_location(
        "orchestra_coordinator_personas",
        _ORCHESTRA_PERSONAS_PATH,
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"Failed to load {_ORCHESTRA_PERSONAS_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    bio = getattr(module, "COORDINATOR_BIO", None)
    if not isinstance(bio, str) or not bio.strip():
        raise SystemExit(f"{_ORCHESTRA_PERSONAS_PATH} has no COORDINATOR_BIO string")
    return bio


COORDINATOR_BIO = _load_coordinator_bio()

# Sample boss/roster stand-ins — production fills these from contacts and
# CoordinatorManager.get_org_members().
DEFAULT_BOSS = {
    "contact_id": 1,
    "first_name": "Dana",
    "surname": "Owner",
    "phone_number": "+15551234567",
    "email_address": "dana@acme.com",
    "bio": "CEO of Acme Corp.",
}

DEFAULT_AUTHORIZED_HUMANS = [
    {
        "first_name": "Dana",
        "surname": "Owner",
        "email": "dana@acme.com",
        "is_admin": True,
    },
    {
        "first_name": "Francis",
        "surname": "Lead",
        "email": "francis@acme.com",
        "is_admin": False,
    },
]

# Mirror orchestra/services/coordinator_service.py::create_coordinator_assistant defaults.
DEFAULT_ASSISTANT = {
    "name": "Coordinator",
    "bio": COORDINATOR_BIO,
    "job_title": "Coordinator",
    "phone": "+15557654321",
    "email": "coordinator@acme.com",
}

# Placeholder regular-assistant identity — short and generic so the surrounding
# prompt scaffolding is what is visible in the dump. Production assistants
# carry their own bio authored by the user.
REGULAR_ASSISTANT = {
    "name": "Sam",
    "bio": (
        "I am Sam, a remote virtual employee working for Dana at Acme Corp. "
        "I handle day-to-day support the way a competent remote colleague would — "
        "communication, scheduling, research, software setup, and operational "
        "follow-through. I keep notes, follow up, and keep work moving."
    ),
    "job_title": "Operations associate",
    "phone": "+15557654321",
    "email": "sam@acme.com",
}

# Stand-in name for the user's Coordinator on regular-assistant dumps. Lets the
# Team-Coordinator deferral block render with realistic text.
REGULAR_WORKSPACE_COORDINATOR_NAME = "Pat"


def _build_slow_brain_system_prompt(
    *,
    persona: str,
    boss: dict,
    assistant_bio: str,
    assistant_job_title: str,
    assistant_has_phone: bool,
    assistant_has_email: bool,
    is_org_workspace: bool,
    demo_mode: bool,
    authorized_humans: list[dict] | None,
    workspace_coordinator_name: str | None,
    team_summaries: list[TeamSummary],
) -> str:
    """Mirror ``unity/conversation_manager/domains/brain.py::build_brain_spec``."""
    bio_parts: list[str] = []
    job_title = (assistant_job_title or "").strip()
    if job_title:
        bio_parts.append(f"Role / specialization: {job_title}.")
    if assistant_bio:
        bio_parts.append(assistant_bio)

    is_coordinator = persona == "coordinator"

    # runtime_setup_note = deployment_runtime_reconcile_prompt_note(cm) — None at cold start.
    return build_system_prompt(
        bio="\n".join(bio_parts),
        contact_id=boss["contact_id"],
        first_name=boss.get("first_name") or "",
        surname=boss.get("surname") or "",
        phone_number=boss.get("phone_number"),
        email_address=boss.get("email_address"),
        is_voice_call=True,
        is_internal_call=False,
        demo_mode=demo_mode,
        computer_fast_path=False,
        assistant_has_phone=assistant_has_phone,
        assistant_has_email=assistant_has_email,
        assistant_has_whatsapp=False,
        assistant_has_discord=False,
        assistant_has_teams=False,
        user_desktop_control=SETTINGS.conversation.USER_DESKTOP_CONTROL_ENABLED,
        runtime_setup_note=None,
        team_summaries=team_summaries,
        is_coordinator=is_coordinator,
        authorized_humans=(
            authorized_humans if (is_coordinator and is_org_workspace) else None
        ),
        workspace_coordinator_name=workspace_coordinator_name,
        is_org_workspace=is_org_workspace,
    ).flatten()


def _build_fast_brain_system_prompt(
    *,
    persona: str,
    boss: dict,
    assistant_bio: str,
    assistant_name: str,
    is_org_workspace: bool,
    demo_mode: bool,
    channel: str,
) -> str:
    """Mirror ``unity/conversation_manager/medium_scripts/call.py`` entrypoint."""
    is_coordinator = persona == "coordinator"
    return build_voice_agent_prompt(
        bio=assistant_bio,
        assistant_name=assistant_name or None,
        boss_first_name=boss.get("first_name", ""),
        boss_surname=boss.get("surname", ""),
        boss_email_address=boss.get("email_address", ""),
        boss_phone_number=boss.get("phone_number", ""),
        boss_bio=boss.get("bio") or None,
        contact_first_name=boss.get("first_name", ""),
        contact_surname=boss.get("surname", ""),
        contact_phone_number=boss.get("phone_number", ""),
        contact_email=boss.get("email_address", ""),
        contact_bio=boss.get("bio") or None,
        is_boss_user=True,
        contact_rolling_summary="",
        demo_mode=demo_mode,
        channel=channel,
        user_desktop_control=SETTINGS.conversation.USER_DESKTOP_CONTROL_ENABLED,
        is_coordinator=is_coordinator,
        is_org_workspace=is_org_workspace,
    ).flatten()


def _build_sample_slow_brain_state(
    *,
    persona: str,
) -> str:
    """Render an empty-call state snapshot like the slow brain's user message."""
    is_coordinator = persona == "coordinator"
    SESSION_DETAILS.is_coordinator = is_coordinator
    SESSION_DETAILS.org_id = "sample-org-id" if is_coordinator else None

    renderer = Renderer()
    contact_index = ContactIndex()
    snapshot = renderer.render_state(
        contact_index,
        managers_initialized=True,
        vm_ready=True,
        file_sync_complete=True,
    )
    return snapshot.full_render


def _write_or_print(label: str, content: str, write_dir: Path | None) -> None:
    if write_dir is not None:
        path = write_dir / f"{label}.txt"
        path.write_text(content, encoding="utf-8")
        print(f"Wrote {path} ({len(content):,} chars)")
        return

    banner = f"{'=' * 72}\n{label}\n{'=' * 72}"
    print(banner)
    print(f"({len(content):,} chars — use --write-dir to materialize)")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Dump production Coordinator voice-call prompts for slow brain and fast brain."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent(
            """\
            Notes
            -----
            - Slow brain also receives a dynamic state user message each turn
              (see --include-sample-state) plus tool schemas from the async tool loop.
            - Fast brain receives this system prompt as ``instructions=`` on the
              LiveKit Agent; conversation history and [notification] messages are appended
              at runtime.
            - Org roster (authorized_humans) is sample data here; production loads it via
              CoordinatorManager.get_org_members() when org_id is set.
            """,
        ),
    )
    parser.add_argument(
        "--persona",
        choices=("coordinator", "regular"),
        default="coordinator",
        help=(
            "Which assistant persona to render (default: coordinator). "
            "``regular`` sets is_coordinator=False, drops the org roster, "
            "and uses a generic placeholder bio."
        ),
    )
    parser.add_argument(
        "--brain",
        choices=("slow", "fast", "both"),
        default="both",
        help="Which prompt(s) to emit (default: both)",
    )
    parser.add_argument(
        "--workspace",
        choices=("org", "personal"),
        default="org",
        help="Org-scoped vs personal workspace (default: org)",
    )
    parser.add_argument(
        "--channel",
        choices=("phone", "unify_meet", "google_meet", "teams_meet"),
        default="phone",
        help="Voice channel for fast brain (default: phone)",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help=f"Enable demo mode (default: {SETTINGS.DEMO_MODE})",
    )
    parser.add_argument(
        "--no-demo",
        action="store_true",
        help="Force demo mode off",
    )
    parser.add_argument(
        "--include-sample-state",
        action="store_true",
        help="Also render a minimal slow-brain state snapshot (user message)",
    )
    parser.add_argument(
        "--write-dir",
        type=Path,
        help="Write prompt files instead of printing to stdout",
    )
    parser.add_argument(
        "--assistant-bio",
        default=None,
        help=(
            "Override assistant about text "
            "(default: Orchestra COORDINATOR_BIO for --persona coordinator, "
            "a short placeholder for --persona regular)"
        ),
    )
    parser.add_argument(
        "--assistant-name",
        default=None,
        help="Assistant display name for fast brain (default: persona-specific)",
    )
    parser.add_argument(
        "--assistant-job-title",
        default=None,
        help="Job title prepended to slow-brain bio (default: persona-specific)",
    )
    args = parser.parse_args()

    persona = args.persona
    defaults = DEFAULT_ASSISTANT if persona == "coordinator" else REGULAR_ASSISTANT
    default_bio = (
        COORDINATOR_BIO if persona == "coordinator" else REGULAR_ASSISTANT["bio"]
    )
    assistant_bio = args.assistant_bio or default_bio
    assistant_name = args.assistant_name or defaults["name"]
    assistant_job_title = args.assistant_job_title or defaults["job_title"]

    if args.demo and args.no_demo:
        parser.error("Use at most one of --demo and --no-demo")

    demo_mode = SETTINGS.DEMO_MODE
    if args.demo:
        demo_mode = True
    elif args.no_demo:
        demo_mode = False

    is_org_workspace = args.workspace == "org"
    authorized_humans = (
        DEFAULT_AUTHORIZED_HUMANS
        if (persona == "coordinator" and is_org_workspace)
        else None
    )
    workspace_coordinator_name = (
        REGULAR_WORKSPACE_COORDINATOR_NAME if persona == "regular" else None
    )

    if args.write_dir is not None:
        args.write_dir.mkdir(parents=True, exist_ok=True)

    meta = (
        f"persona={persona} workspace={args.workspace} demo_mode={demo_mode} "
        f"channel={args.channel} user_desktop_control="
        f"{SETTINGS.conversation.USER_DESKTOP_CONTROL_ENABLED}"
    )
    print(f"# Voice prompts ({meta})\n")

    label_prefix = persona

    if args.brain in ("slow", "both"):
        slow_prompt = _build_slow_brain_system_prompt(
            persona=persona,
            boss=DEFAULT_BOSS,
            assistant_bio=assistant_bio,
            assistant_job_title=assistant_job_title,
            assistant_has_phone=bool(defaults["phone"]),
            assistant_has_email=bool(defaults["email"]),
            is_org_workspace=is_org_workspace,
            demo_mode=demo_mode,
            authorized_humans=authorized_humans,
            workspace_coordinator_name=workspace_coordinator_name,
            team_summaries=[],
        )
        _write_or_print(
            f"{label_prefix}_slow_brain_system_prompt",
            slow_prompt,
            args.write_dir,
        )

        if args.include_sample_state:
            sample_state = _build_sample_slow_brain_state(
                persona=persona,
            )
            _write_or_print(
                f"{label_prefix}_slow_brain_sample_state_user_message",
                sample_state,
                args.write_dir,
            )

    if args.brain in ("fast", "both"):
        fast_prompt = _build_fast_brain_system_prompt(
            persona=persona,
            boss=DEFAULT_BOSS,
            assistant_bio=assistant_bio,
            assistant_name=assistant_name,
            is_org_workspace=is_org_workspace,
            demo_mode=demo_mode,
            channel=args.channel,
        )
        _write_or_print(
            f"{label_prefix}_fast_brain_system_prompt",
            fast_prompt,
            args.write_dir,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
