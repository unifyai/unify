#!/usr/bin/env python3
"""Generate the OpenClaw / Hermes / Unity comparison-family architecture PNGs.

The three diagrams share a single locked visual grammar (off-white panel, white
rounded boxes, thin gray rounded-orthogonal arrows, locked color semantics) so
that every visual difference between them maps onto a real architectural
difference. Re-run this script to regenerate the PNGs from prompt source.

Run:
    .venv/bin/python scripts/draw_architecture_diagrams.py
    .venv/bin/python scripts/draw_architecture_diagrams.py --only unity
"""

from __future__ import annotations

import argparse
import base64
import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

REPO_ROOT = Path(__file__).resolve().parent.parent
ASSETS_DIR = REPO_ROOT / "assets"

MODEL = "gpt-image-2"
SIZE = "1536x1024"

SHARED_STYLE = """
Render a clean technical architecture diagram as a single landscape image.

Locked visual grammar (use this EXACT styling for every box, arrow, and label
in the diagram, no variations whatsoever):

- Panel background: soft off-white, hex #F3EFE6.
- All boxes: 1.5px dark charcoal border, rounded corners (border-radius
  ~14px), subtle very-light drop shadow.
- Box title: bold sans-serif (Inter), dark charcoal, centered at the top of
  the box.
- Box subtitle: light gray sans-serif (Inter), centered under the title, one
  or two short lines max.
- Arrows: thin (1.5px) medium-gray, rounded orthogonal corners only (no
  diagonals, no curves). Small triangular arrow heads.
- For a bidirectional relationship between two stacked rows (e.g. tools and
  state), use two SEPARATE parallel orthogonal arrows side-by-side — one
  pointing down on the left, one pointing up on the right. NEVER combine
  them into a single double-headed arrow.
- All inline tool-list labels inside a row use a middot separator (·).
- A single bold sans-serif title sits centered at the top of the panel,
  outside any box.
- Layout is a strict top-to-bottom vertical stack. Boxes are horizontally
  centered. Use generous vertical whitespace between rows.

Reserved fill colors (semantic meaning is locked across the OpenClaw,
Hermes Agent, and Unity diagrams in this family — every diagram uses the
same palette, every color means exactly one thing, do not use these colors
for anything else):

- WHITE  = passive structural tier (channels / surfaces / mediums, tools,
           state, dispatcher daemon).
- GREEN  = the agent's tool-calling loop (very soft sage green, hex
           #E4EEDC). Every diagram has exactly one green box: the loop that
           actually calls tools to do work.
- PEACH  = an autonomous wake source — a non-user input that can cause the
           agent to think without a fresh user message (very soft peach, hex
           #F6E2CE). Every diagram has exactly one peach box, sitting at
           the top alongside the user box. The label encodes the
           mechanism (cron + webhooks vs. natural-language scheduled tasks
           etc.) but the color is universal.
- PINK   = a persistent reasoning loop above the agent — a layer that keeps
           reasoning while a dispatched action is in flight (very soft
           dusty pink, hex #F4DCDC). Appears in at most one diagram across
           the family; its presence (or absence) is the headline
           architectural distinction these diagrams are designed to surface.

Do not introduce any other colors. Do not introduce any decorative elements,
legends, logos, or watermarks. No text outside the boxes except the panel
title and the inline edge labels listed in the per-diagram spec below.
""".strip()


OPENCLAW_PROMPT = SHARED_STYLE + "\n\n" + """
Per-diagram spec — panel title: "OpenClaw"

Render the following rows as a top-to-bottom vertical stack using the locked
visual grammar above. The rows, top to bottom, are exactly:

1. Top row contains TWO boxes side-by-side:
   - LEFT: small WHITE box, title "user", no subtitle.
   - RIGHT: PEACH box, title "cron + webhooks", subtitle
     "(automation triggers)".

2. WIDE WHITE box, title "channels", subtitle
   "Telegram · Discord · Slack · SMS · Nodes (devices)".

3. WIDE WHITE box, title "Gateway daemon", subtitle
   "dispatcher — per-session lane (steer = abort + redeliver)".

4. WIDE GREEN box, title "Pi embedded agent", subtitle
   "single tool-calling loop — no supervising loop".

5. WIDE WHITE box, title "tools", subtitle
   "core (web · exec · sessions) · voice-call plugin · mcporter → MCP servers".

6. WIDE WHITE box, title "state", subtitle
   "JSONL sessions · workspace files (SKILL.md · SOUL.md · AGENTS.md) · memory plugin".

Arrows (top to bottom):
- Each of the two top boxes in row (1) has a short down arrow that merges
  into a single orthogonal junction and continues as one down arrow into the
  center-top of (2).
- Single down arrow from (2) to (3).
- Single down arrow from (3) to (4). Label the arrow with small italic gray
  text "start / abort run" to the right of the arrow.
- Single down arrow from (4) to (5).
- Between (5) and (6), use TWO parallel orthogonal arrows side-by-side — one
  pointing down on the left, one pointing up on the right.
"""


HERMES_PROMPT = SHARED_STYLE + "\n\n" + """
Per-diagram spec — panel title: "Hermes Agent"

Render the following rows as a top-to-bottom vertical stack using the locked
visual grammar above. The rows, top to bottom, are exactly:

1. Top row contains TWO small boxes side-by-side:
   - LEFT: WHITE box, title "user", no subtitle.
   - RIGHT: PEACH box, title "cron + webhooks", subtitle
     "(automation triggers)".

2. WIDE WHITE box, title "surfaces", subtitle
   "CLI · TUI · Gateway (Telegram · Discord · Slack · SMS) · ACP (IDE)".

3. WIDE GREEN box, title "AIAgent", subtitle on two short lines:
   line 1 "single ~12k-LOC sync tool-calling loop"
   line 2 "steer() = inject text · interrupt() = thread-scoped abort flag".

4. WIDE WHITE box, title "tools", subtitle
   "native · execute_code · TTS / voice_mode / SMS · delegate_tool · MCP servers".

5. WIDE WHITE box, title "state", subtitle
   "SQLite sessions + FTS5 · MEMORY.md / USER.md · SKILL.md library · memory provider plugin".

Arrows (top to bottom):
- Each of the two top boxes in row (1) has a short down arrow that merges
  into a single orthogonal junction and continues as one down arrow into the
  center-top of (2).
- Single down arrow from (2) to (3).
- Single down arrow from (3) to (4).
- Between (4) and (5), use TWO parallel orthogonal arrows side-by-side — one
  pointing down on the left, one pointing up on the right (IDENTICAL shape
  to the OpenClaw diagram).
"""


UNITY_PROMPT = SHARED_STYLE + "\n\n" + """
Per-diagram spec — panel title: "Unity"

Render the following rows as a top-to-bottom vertical stack using the locked
visual grammar above. The rows, top to bottom, are exactly:

1. Top row contains TWO boxes side-by-side:
   - LEFT: small WHITE box, title "user", no subtitle.
   - RIGHT: PEACH box, title "scheduled tasks + triggers", subtitle
     "(natural-language Tasks)".

2. WIDE WHITE box, title "mediums", subtitle
   "chat · voice · phone · video · screen-share · sms · email".

3. Row containing TWO boxes side-by-side, connected to each other by a
   horizontal double-ended orthogonal arrow labeled with small italic gray
   text "SPEAK / NOTIFY · events / context":
   - LEFT: WHITE box, title "fast brain", subtitle
     "real-time voice + video · sub-second".
   - RIGHT: PINK box, title "ConversationManager (slow brain)", subtitle
     "persistent reasoning loop — always present".

4. WIDE GREEN box, title "CodeActActor", subtitle
   "background reasoner — writes Python plans over typed primitives".

5. WIDE WHITE box, title "primitives", subtitle
   "contacts · knowledge · tasks · transcripts · files · images · web · secrets · functions · guidance".

6. WIDE WHITE box, title "back office (typed state managers)", subtitle on
   two short lines:
   line 1 "ContactManager · KnowledgeManager · TaskScheduler · TranscriptManager · FileManager"
   line 2 "ImageManager · WebSearcher · SecretManager · FunctionManager · GuidanceManager".

Arrows (top to bottom):
- Each of the two top boxes in row (1) has a short down arrow that merges
  into a single orthogonal junction and continues as one down arrow into the
  center-top of (2).
- Two down arrows leaving the bottom of (2): one into the top of the LEFT
  box in row (3) (fast brain) and one into the top of the RIGHT box in row
  (3) (ConversationManager / slow brain).
- Single down arrow from the bottom of the RIGHT box in row (3)
  (ConversationManager / slow brain) into the top of (4). Label this arrow
  with small italic gray text "act(...)" to its right. The LEFT box (fast
  brain) does NOT have an arrow going down into row (4).
- Single down arrow from (4) to (5).
- Between (5) and (6), use TWO parallel orthogonal arrows side-by-side — one
  pointing down on the left, one pointing up on the right (IDENTICAL shape
  to the OpenClaw and Hermes diagrams).
"""


DIAGRAMS = {
    "openclaw": ("openclaw-architecture.png", OPENCLAW_PROMPT),
    "hermes": ("hermes-architecture.png", HERMES_PROMPT),
    "unity": ("unity-architecture.png", UNITY_PROMPT),
}


def generate(name: str, prompt: str, output_path: Path, client: OpenAI) -> None:
    print(f"[{name}] requesting {MODEL} ({SIZE})...", flush=True)
    result = client.images.generate(
        model=MODEL,
        prompt=prompt,
        size=SIZE,
        n=1,
    )
    payload = result.data[0]
    if payload.b64_json is None:
        raise RuntimeError(f"[{name}] no b64_json in response")
    output_path.write_bytes(base64.b64decode(payload.b64_json))
    print(f"[{name}] wrote {output_path.relative_to(REPO_ROOT)}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        choices=sorted(DIAGRAMS.keys()),
        action="append",
        help="restrict generation to one or more diagrams (default: all three)",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    client = OpenAI()

    selected = args.only or list(DIAGRAMS.keys())
    for name in selected:
        filename, prompt = DIAGRAMS[name]
        generate(name, prompt, ASSETS_DIR / filename, client)
    return 0


if __name__ == "__main__":
    sys.exit(main())
