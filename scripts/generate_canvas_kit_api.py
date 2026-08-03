#!/usr/bin/env python3
"""Generate the @unity/canvas-kit API digest the actor authors against.

Without this the actor is guessing component names, and a wrong guess costs a
full build round trip: tsc rejects it, the diagnostic comes back, the plan
revises. Inlining a few kilobytes of reference is much cheaper than that loop,
and it is the difference between the kit being available and the kit being
discoverable.

The digest is generated from the kit's **emitted declarations** rather than its
source, because those are exactly what the typechecker enforces. A digest that
described anything else would document components that then fail to compile.

Following the `builtins_guidance.json` precedent, the output is a committed
snapshot: generation needs a kit, which not every environment has, so drift is
detected on request rather than applied automatically.

    # from an installed toolchain (what the image has)
    scripts/generate_canvas_kit_api.py

    # from a branding checkout
    scripts/generate_canvas_kit_api.py --kit ~/branding/packages/canvas-kit

    # fail if the committed digest is stale
    scripts/generate_canvas_kit_api.py --check
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

OUTPUT = (
    Path(__file__).resolve().parent.parent / "unify/canvas_manager/canvas_kit_api.md"
)

# Declaration files, in the order they should appear in the digest: layout before
# the things that go inside it, primitives before the charts that use them.
_ORDER = (
    "layout/layout.d.ts",
    "display/card.d.ts",
    "display/typography.d.ts",
    "display/indicators.d.ts",
    "display/table.d.ts",
    "display/lists.d.ts",
    "display/states.d.ts",
    "charts/charts.d.ts",
    "interaction/controls.d.ts",
    "interaction/actions.d.ts",
)

_SECTION_TITLES = {
    "layout/layout.d.ts": "Layout",
    "display/card.d.ts": "Cards",
    "display/typography.d.ts": "Typography",
    "display/indicators.d.ts": "Indicators",
    "display/table.d.ts": "Tables",
    "display/lists.d.ts": "Lists",
    "display/states.d.ts": "States",
    "charts/charts.d.ts": "Charts",
    "interaction/controls.d.ts": "Form controls",
    "interaction/actions.d.ts": "Actions",
}

# tsc emits a predictable shape: a JSDoc block, an exported props interface, then
# a `declare const` for the component itself.
_DOC = re.compile(r"/\*\*(?P<body>.*?)\*/", re.DOTALL)

# Interfaces are matched whether or not they are exported: the chart components
# share a private `ChartBase`, and a canvas still needs to know its fields.
_INTERFACE = re.compile(
    r"(?:export )?interface (?P<name>\w+)(?:<[^{]*?>)?(?P<heritage>[^{]*)\{(?P<body>.*?)\n\}",
    re.DOTALL,
)

# Two emitted shapes, because the kit uses both. forwardRef components become a
# `const` whose generic names the props type; plain function components become a
# `function` whose parameter annotation names it.
_CONST_COMPONENT = re.compile(
    r"export declare const (?P<name>\w+): React\.ForwardRefExoticComponent<\s*(?P<props>\w+)",
)
# The optional generic clause matters: `Table` is generic over its row type, and
# without it the component is exported but never documented.
_FUNC_COMPONENT = re.compile(
    r"export declare function (?P<name>\w+)(?:<[^(]*>)?\((?P<params>.*?)\):"
    r"(?P<returns>[^;]*);",
    re.DOTALL,
)
# A props annotation may itself be generic (`TableProps<Row>`); the interface is
# keyed on the bare name.
_PARAM_TYPE = re.compile(
    r"\}\s*:\s*(?P<props>\w+)(?:<[^>]*>)?\s*$"
    r"|^\s*\w+\s*:\s*(?P<bare>\w+)(?:<[^>]*>)?\s*$",
)

_TYPE_ALIAS = re.compile(r"export (?:declare )?type (?P<name>\w+) = (?P<value>[^;]+);")


class Component(NamedTuple):
    name: str
    summary: str
    props: List[str]
    extends: str
    # Hooks are exported alongside components and read identically in the
    # declarations. Rendering one as `<useCanvasAction>` would teach the actor to
    # write a hook as JSX, which fails at the typecheck it cannot see yet.
    is_hook: bool = False


def _clean_doc(raw: str) -> str:
    """Flatten a JSDoc block into one line of prose."""
    lines = []
    for line in raw.splitlines():
        stripped = line.strip().lstrip("*").strip()
        if not stripped or stripped.startswith("@"):
            continue
        lines.append(stripped)
    text = " ".join(lines)
    # The digest is a reference, not the design rationale; one sentence is enough
    # to pick between components, and the full reasoning lives in the source.
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return sentences[0].strip() if sentences else ""


def _props(body: str) -> List[str]:
    """Extract `name?: type` declarations, keeping any inline doc as a suffix."""
    props: List[str] = []
    pending_doc = ""

    for chunk in re.split(r"\n", body):
        line = chunk.strip()
        if not line:
            continue

        inline = re.match(r"/\*\*(.*?)\*/", line)
        if inline:
            pending_doc = inline.group(1).strip()
            line = line[inline.end() :].strip()
            if not line:
                continue
        elif line.startswith(("/**", "*", "*/", "//")):
            cleaned = line.lstrip("/*").rstrip("*/").strip()
            if cleaned:
                pending_doc = f"{pending_doc} {cleaned}".strip()
            continue

        match = re.match(r"(?P<name>\w+)(?P<opt>\??):\s*(?P<type>.+?);?$", line)
        if not match:
            continue

        declared = f"{match.group('name')}{match.group('opt')}: {match.group('type').rstrip(';')}"
        # The signature is code and the note is prose; keeping the note outside
        # the span means a reader (and the model) can tell where the type ends.
        props.append(
            f"`{declared}` — {pending_doc}" if pending_doc else f"`{declared}`",
        )
        pending_doc = ""

    return props


def _doc_before(text: str, position: int) -> str:
    """The JSDoc block immediately preceding a declaration, if any.

    Only immediately: a block further back belongs to the previous declaration,
    and attributing it here would describe the wrong component.
    """
    preceding = text[:position]
    docs = list(_DOC.finditer(preceding))
    if not docs or preceding[docs[-1].end() :].strip():
        return ""
    return _clean_doc(docs[-1].group("body"))


def _heritage(clause: str) -> str:
    """Describe what a props interface inherits, in prose."""
    if "Omit<" in clause:
        omitted = re.findall(r"'(\w+)'", clause)
        if omitted:
            return f"standard element attributes except {', '.join(omitted)}"
    if "HTMLAttributes" in clause:
        return "standard element attributes"
    return ""


def _parse(text: str) -> List[Component]:
    """Pair each exported component with the props interface its signature names.

    Driven by the signature rather than by deriving a props name from the
    component name: several components legitimately share one interface, and the
    name-derived version silently dropped every chart.
    """
    interfaces = {match.group("name"): match for match in _INTERFACE.finditer(text)}

    found: List[tuple[str, str, Optional[str]]] = []  # (name, summary, props type)

    for match in _CONST_COMPONENT.finditer(text):
        found.append(
            (
                match.group("name"),
                (
                    _doc_before(text, interfaces[match.group("props")].start())
                    if match.group("props") in interfaces
                    else _doc_before(text, match.start())
                ),
                match.group("props"),
            ),
        )

    # Hooks are exported as functions too and read identically here. A component
    # returns JSX; anything else does not, which is the only reliable signal in the
    # declarations.
    hooks: set = set()
    for match in _FUNC_COMPONENT.finditer(text):
        params = " ".join(match.group("params").split())
        annotation = _PARAM_TYPE.search(params)
        props_name = None
        if annotation:
            props_name = annotation.group("props") or annotation.group("bare")
        if "JSX.Element" not in match.group("returns"):
            hooks.add(match.group("name"))
        found.append(
            (match.group("name"), _doc_before(text, match.start()), props_name),
        )

    # Components exported without a props interface at all (Card's subparts) are
    # still named, or the actor cannot compose a Card.
    for name in re.findall(r"export declare const (\w+):", text):
        if not any(entry[0] == name for entry in found):
            found.append((name, _doc_before(text, text.index(f"const {name}:")), None))

    components: List[Component] = []
    for name, summary, props_name in found:
        interface = interfaces.get(props_name) if props_name else None
        components.append(
            Component(
                name=name,
                summary=summary,
                props=_props(interface.group("body")) if interface else [],
                extends=_heritage(interface.group("heritage")) if interface else "",
                is_hook=name in hooks,
            ),
        )

    return components


def _exported_components(text: str) -> set:
    """Every component name the declaration file exports.

    Used to prove the digest documents all of them: one the digest omits is one
    the actor cannot discover, which is the failure this generator exists to
    prevent.
    """
    return set(re.findall(r"export declare (?:const|function) (\w+)", text))


def _scales(kit: Path) -> Dict[str, str]:
    """Read the enumerated scales props refer to.

    A prop typed `tone?: Tone` is useless without the values Tone admits, and
    guessing one is a typecheck failure rather than a graceful fallback.
    """
    path = kit / "lib/scales.d.ts"
    if not path.is_file():
        return {}

    text = path.read_text(encoding="utf8")
    scales = {}
    for match in _TYPE_ALIAS.finditer(text):
        value = " ".join(match.group("value").split())
        scales[match.group("name")] = value
    return scales


def _kit_root(explicit: Optional[str]) -> Optional[Path]:
    """Locate emitted declarations, or a source checkout to read instead."""
    if explicit:
        candidate = Path(explicit).expanduser()
        for root in (candidate, candidate / "src"):
            if (root / "index.d.ts").is_file() or (root / "index.ts").is_file():
                return root
        return None

    from unify.canvas_manager.ops.build_ops import _toolchain_root

    toolchain = _toolchain_root()
    if toolchain is None:
        return None
    emitted = toolchain / "node_modules/@unity/canvas-kit"
    return emitted if (emitted / "index.d.ts").is_file() else None


def render(kit: Path) -> str:
    """Build the digest markdown."""
    suffix = ".d.ts" if (kit / "index.d.ts").is_file() else ".tsx"
    out: List[str] = [
        "# @unity/canvas-kit",
        "",
        "The component vocabulary a canvas is written against. Generated from the",
        "kit's type declarations by `scripts/generate_canvas_kit_api.py` — do not",
        "edit by hand.",
        "",
        "Import everything from `@unity/canvas-kit`. Two rules the API enforces:",
        "no component takes a raw colour (only `tone` and chart series indices),",
        "and layout props are enumerated scales rather than class strings.",
        "",
    ]

    scales = _scales(kit)
    if scales:
        out.append("## Scales")
        out.append("")
        for name, value in sorted(scales.items()):
            out.append(f"- `{name}` = {value}")
        out.append("")

    total = 0
    missing: List[str] = []
    for relative in _ORDER:
        path = kit / relative.replace(".d.ts", suffix)
        if not path.is_file():
            continue

        text = path.read_text(encoding="utf8")
        components = _parse(text)
        undocumented = _exported_components(text) - {c.name for c in components}
        if undocumented:
            missing += [f"{relative}: {name}" for name in sorted(undocumented)]
        if not components:
            continue

        out.append(f"## {_SECTION_TITLES[relative]}")
        out.append("")
        for component in components:
            total += 1
            if component.is_hook:
                out.append(f"### `{component.name}(canvas, name)`")
            else:
                out.append(f"### `<{component.name}>`")
            if component.summary:
                out.append(component.summary)
            if component.props:
                out.append("")
                for prop in component.props:
                    out.append(f"- {prop}")
            if component.extends:
                out.append(f"- plus {component.extends}")
            out.append("")

    if total == 0:
        raise SystemExit(
            "Parsed zero components. The declaration format has changed; fix the "
            "generator rather than committing an empty digest.",
        )
    if missing:
        raise SystemExit(
            "These exported components did not reach the digest, so the actor "
            "could not discover them:\n  " + "\n  ".join(missing),
        )

    out.append(f"<!-- {total} components -->")
    return "\n".join(out) + "\n"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kit", help="path to a canvas-kit package or its src/")
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed digest is stale",
    )
    args = parser.parse_args(argv)

    kit = _kit_root(args.kit)
    if kit is None:
        print(
            "No canvas-kit found. Install the toolchain or pass --kit "
            "<path to packages/canvas-kit>.",
            file=sys.stderr,
        )
        return 2

    generated = render(kit)

    if args.check:
        current = OUTPUT.read_text(encoding="utf8") if OUTPUT.is_file() else ""
        if current == generated:
            print(f"{OUTPUT.name} is up to date")
            return 0
        print(
            f"{OUTPUT.name} is stale. Regenerate it with "
            f"scripts/generate_canvas_kit_api.py",
            file=sys.stderr,
        )
        return 1

    OUTPUT.write_text(generated, encoding="utf8")
    print(f"wrote {OUTPUT.relative_to(Path.cwd())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
