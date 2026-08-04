"""Import the canvas vocabulary corpus as builtin guidance entries.

The canvas authoring vocabulary is shadcn component source the actor inlines
into a canvas module (see ``unify/canvas_manager/base.py``). The corpus itself
is committed in branding (``apps/canvas-host/vocabulary/``), drift-locked
against the upstream registry, and scanned by the runtime host's Tailwind
build — so it is the exact source that renders correctly on the host. This
module turns each corpus component into one builtin guidance entry, giving the
actor discovery-first access to the whole catalogue: search the guidance,
read the entry, inline the source.

Entries land in the same committed snapshot as the skill imports
(``builtins_guidance.json``) under ``canvas/<component>`` keys; the catalogue
seeder is generic over the snapshot, so no consumer changes are involved.

Usage::

    .venv/bin/python -m scripts.skill_migration.vocabulary_to_guidance \
        --branding ~/branding            # regenerate the canvas/ entries
    .venv/bin/python -m scripts.skill_migration.vocabulary_to_guidance \
        --branding ~/branding --check    # fail when the snapshot drifted
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

SNAPSHOT_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "unify/guidance_manager/builtins_guidance.json"
)

KEY_PREFIX = "canvas/"

# Component-specific caveats, appended to the shared authoring rules. Keyed by
# corpus file stem; components absent here get only the shared rules.
_OVERLAY_NOTE = (
    "This component portals its overlay outside the canvas content root. The "
    "runtime includes open overlays in the frame's reported height, so a tall "
    "overlay grows the frame rather than clipping — but prefer modal dialogs "
    "for long content, and let the built-in collision handling place "
    "popovers/menus."
)
_FORM_NOTE = (
    "The frame is sandboxed without `allow-forms`: a native <form> renders "
    "and then silently does nothing on submit. Wire inputs to an action with "
    "`useCanvasAction(canvas, name)` from @unity/canvas-kit, or use the kit's "
    "`ActionForm` to render a whole action's schema."
)
_CHART_NOTE = (
    "Charts compose recharts directly. Never write a literal colour: use "
    "`seriesColor(n)` from @unity/canvas-kit or `var(--chart-N)` for fills "
    "and strokes, and `var(--chart-grid)` for grids."
)

_COMPONENT_NOTES = {
    "alert-dialog": _OVERLAY_NOTE,
    "dialog": _OVERLAY_NOTE,
    "dropdown-menu": _OVERLAY_NOTE,
    "hover-card": _OVERLAY_NOTE,
    "menubar": _OVERLAY_NOTE,
    "navigation-menu": _OVERLAY_NOTE,
    "popover": _OVERLAY_NOTE,
    "select": _OVERLAY_NOTE,
    "sheet": _OVERLAY_NOTE,
    "tooltip": _OVERLAY_NOTE,
    "checkbox": _FORM_NOTE,
    "input": _FORM_NOTE,
    "label": _FORM_NOTE,
    "radio-group": _FORM_NOTE,
    "slider": _FORM_NOTE,
    "switch": _FORM_NOTE,
    "textarea": _FORM_NOTE,
    "toggle": _FORM_NOTE,
    "toggle-group": _FORM_NOTE,
}

_SHARED_RULES = """\
How to use this in a canvas:

- INLINE this source into your canvas TSX module — a canvas is one module and
  cannot import sibling files. Imports are already rewritten for the canvas
  runtime: `cn` comes from `@unity/canvas-kit`, and any `./component` import
  below refers to another vocabulary entry whose source you inline alongside.
- Only the vendored substrate resolves at view time: the `@radix-ui/react-*`
  set, `class-variance-authority`, `clsx`, `tailwind-merge`, `lucide-react`
  and `recharts`, plus `react` and `@unity/canvas-kit`.
- Colour is semantic-token-only (`bg-primary`, `text-muted-foreground`, ...).
  Hex, rgb() and named palettes fail lint, as does any class the shipped
  stylesheet does not contain."""


def build_vocabulary_entries(branding_root: Path) -> Dict[str, Dict[str, str]]:
    """One guidance entry per corpus component, keyed ``canvas/<name>``."""
    corpus = branding_root / "apps/canvas-host/vocabulary"
    lock_path = corpus / "vocabulary.lock.json"
    if not lock_path.is_file():
        raise FileNotFoundError(
            f"no vocabulary corpus at {corpus} — is this a branding checkout "
            f"with the canvas-host vocabulary imported?",
        )
    lock = json.loads(lock_path.read_text(encoding="utf-8"))

    entries: Dict[str, Dict[str, str]] = {}
    for name in sorted(lock.get("files", {})):
        source_path = corpus / f"{name}.tsx"
        source = source_path.read_text(encoding="utf-8").strip()

        kind = "chart composition" if name.startswith("chart-") else "component"
        parts = [
            f"Reference source for the `{name}` canvas vocabulary {kind}, "
            f"imported from the shadcn/ui registry and pinned to the runtime "
            f"host's corpus — this exact source renders correctly on the "
            f"canvas host.",
            _SHARED_RULES,
        ]
        note = _COMPONENT_NOTES.get(name)
        if note:
            parts.append(note)
        if name.startswith("chart-") or name == "chart":
            parts.append(_CHART_NOTE)
        parts.append(f"```tsx\n{source}\n```")

        entries[f"{KEY_PREFIX}{name}"] = {
            "title": f"[canvas] {name} — vocabulary source",
            "content": "\n\n".join(parts),
        }
    return entries


def merge_into_snapshot(
    entries: Dict[str, Dict[str, str]],
    path: Optional[Path] = None,
) -> None:
    """Replace every ``canvas/`` entry in the snapshot, preserving the rest."""
    path = path or SNAPSHOT_PATH
    data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    skills = {
        key: value
        for key, value in data.get("skills", {}).items()
        if not key.startswith(KEY_PREFIX)
    }
    skills.update(entries)
    data["skills"] = {key: skills[key] for key in sorted(skills)}
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def check_drift(
    entries: Dict[str, Dict[str, str]],
    path: Optional[Path] = None,
) -> bool:
    """Whether the snapshot's canvas/ entries match a fresh generation."""
    path = path or SNAPSHOT_PATH
    if not path.exists():
        return False
    stored = {
        key: value
        for key, value in json.loads(path.read_text(encoding="utf-8"))
        .get("skills", {})
        .items()
        if key.startswith(KEY_PREFIX)
    }
    return stored == entries


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--branding", required=True, help="branding checkout path")
    parser.add_argument("--check", action="store_true", help="verify, do not write")
    args = parser.parse_args(argv)

    entries = build_vocabulary_entries(Path(args.branding).expanduser())
    if args.check:
        if not check_drift(entries):
            print("canvas vocabulary guidance drifted from the corpus")
            return 1
        print(f"canvas vocabulary guidance matches the corpus ({len(entries)} entries)")
        return 0

    merge_into_snapshot(entries)
    print(f"merged {len(entries)} canvas vocabulary entries into the snapshot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
