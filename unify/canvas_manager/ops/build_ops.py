"""Compile one canvas from TSX to an ES module.

Three gates run in order, cheapest first, and any of them blocks publication:

1. **Lint.** Rejects colour literals and font declarations. This is the only
   place that rule can be applied to a canvas — authored TSX never passes
   through console's lint-staged or its build, so without this a canvas could
   emit an off-palette colour that looks right in one theme and wrong in the
   other.
2. **Typecheck.** ``tsc --noEmit`` against the kit's declarations. esbuild does
   not typecheck, so without this a misused component reaches the browser.
3. **Bundle.** esbuild, with react, react-dom and ``@unity/canvas-kit`` external
   so the runtime host supplies them. That keeps a canvas a few kilobytes rather
   than shipping its own React.

The emitted module is content-addressed by SHA-256 of its bytes. Console
verifies that hash before handing the code to the frame, which is a stronger
integrity guarantee than subresource integrity because we enforce it ourselves.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

from unify.canvas_manager.types.view import BuildReport

# Specifiers a canvas may import. Everything else is absent at view time: the
# frame has no bundler and no network, so an unlisted import is a load failure
# rather than a slow path.
ALLOWED_IMPORTS = frozenset(
    {
        "react",
        "react-dom",
        "react-dom/client",
        "react/jsx-runtime",
        "@unity/canvas-kit",
    },
)

# Colour and font patterns, mirroring console's `scripts/check-colors.ts` and
# `scripts/check-font-classes.ts`. Kept as source-level checks because they must
# reject the author's intent, not just the compiled output.
_HEX_COLOUR = re.compile(r"#[0-9a-fA-F]{3,8}\b")
_FUNCTIONAL_COLOUR = re.compile(r"\b(?:rgba?|hsla?)\s*\(")
_TAILWIND_COLOUR_CLASS = re.compile(
    r"\b(?:bg|text|border|ring|fill|stroke|from|via|to|decoration|outline|shadow|accent|caret|divide)-"
    r"(?:slate|gray|grey|zinc|neutral|stone|red|orange|amber|yellow|lime|green|emerald|teal|cyan|sky|"
    r"blue|indigo|violet|purple|fuchsia|pink|rose|white|black)"
    r"(?:-\d{1,3})?\b",
)
_ARBITRARY_COLOUR_CLASS = re.compile(
    r"\b(?:bg|text|border|ring|fill|stroke)-\[#[0-9a-fA-F]{3,8}\]",
)
_INLINE_FONT_FAMILY = re.compile(r"font-family\s*:", re.IGNORECASE)

# `url(#gradient-id)` and similar are SVG references, not colours.
_SVG_REFERENCE = re.compile(r"url\([^)]*#")

_IMPORT_SPECIFIER = re.compile(
    r"""^\s*(?:import|export)[\s\S]*?from\s*['"]([^'"]+)['"]""",
    re.MULTILINE,
)
_BARE_IMPORT = re.compile(r"""^\s*import\s*['"]([^'"]+)['"]""", re.MULTILINE)


def lint_source(tsx: str) -> List[str]:
    """Return one diagnostic per style violation, empty when the source is clean.

    Checks the author's text rather than the compiled output, so the message can
    name the offending line and the author can act on it.
    """
    problems: List[str] = []

    for lineno, line in enumerate(tsx.splitlines(), start=1):
        stripped = line.strip()
        if (
            stripped.startswith("//")
            or stripped.startswith("*")
            or stripped.startswith("/*")
        ):
            continue

        # An SVG fragment reference is not a colour; strip those before looking.
        scannable = _SVG_REFERENCE.sub("url(", line)

        if _HEX_COLOUR.search(scannable):
            problems.append(
                f"line {lineno}: hex colour is not allowed. Use a `tone` prop "
                f"(muted/success/warning/danger) or a chart series index.",
            )
        if _FUNCTIONAL_COLOUR.search(scannable):
            problems.append(
                f"line {lineno}: rgb()/hsl() colour is not allowed. Use a `tone` prop.",
            )
        if _TAILWIND_COLOUR_CLASS.search(scannable):
            problems.append(
                f"line {lineno}: named colour utility class is not allowed and has no effect — "
                f"the canvas stylesheet ships no colour utilities. Use a `tone` prop.",
            )
        if _ARBITRARY_COLOUR_CLASS.search(scannable):
            problems.append(
                f"line {lineno}: arbitrary colour class is not allowed. Use a `tone` prop.",
            )
        if _INLINE_FONT_FAMILY.search(scannable):
            problems.append(
                f"line {lineno}: inline font-family is not allowed; the kit sets typography.",
            )

    for match in list(_IMPORT_SPECIFIER.finditer(tsx)) + list(
        _BARE_IMPORT.finditer(tsx),
    ):
        specifier = match.group(1)
        if specifier.startswith("."):
            problems.append(
                f"relative import {specifier!r} is not allowed: a canvas is a single module.",
            )
        elif specifier not in ALLOWED_IMPORTS:
            problems.append(
                f"import {specifier!r} is not available at view time. "
                f"Only {', '.join(sorted(ALLOWED_IMPORTS))} are provided.",
            )

    return problems


def _run(command: List[str], cwd: Path, timeout: int = 120) -> Tuple[int, str]:
    """Run a toolchain command, returning its exit code and combined output."""
    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        command,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return completed.returncode, (completed.stdout + completed.stderr).strip()


def _toolchain_root() -> Optional[Path]:
    """Locate the node workspace holding esbuild, typescript and the kit.

    Configured via ``UNITY_CANVAS_TOOLCHAIN_ROOT``; the fallbacks are where the
    assistant image installs it. The toolchain is vendored into the image rather
    than fetched, so authoring needs no network and no separate build service.
    """
    from unify.canvas_manager.settings import CanvasSettings

    configured = CanvasSettings().TOOLCHAIN_ROOT.strip()
    candidates = [Path(configured)] if configured else []
    candidates += [
        Path("/opt/canvas-toolchain"),
        Path.home() / ".unity" / "canvas-toolchain",
    ]

    for candidate in candidates:
        if (candidate / "node_modules").is_dir():
            return candidate
    return None


def build_canvas(tsx: str, *, kit_version: str = "") -> Tuple[BuildReport, str]:
    """Lint, typecheck and bundle one canvas.

    Returns the report and the compiled module text, which is empty whenever the
    report is not ``ok``.
    """
    started = time.monotonic()

    problems = lint_source(tsx)
    if problems:
        return (
            BuildReport(
                ok=False,
                failed_stage="lint",
                diagnostics=problems,
                duration_ms=int((time.monotonic() - started) * 1000),
            ),
            "",
        )

    root = _toolchain_root()
    if root is None:
        return (
            BuildReport(
                ok=False,
                failed_stage="bundle",
                diagnostics=[
                    "Canvas toolchain is unavailable in this environment. "
                    "Expected a node workspace with esbuild, typescript and "
                    "@unity/canvas-kit installed.",
                ],
                duration_ms=int((time.monotonic() - started) * 1000),
            ),
            "",
        )

    with tempfile.TemporaryDirectory(prefix="canvas-build-") as tmp:
        work = Path(tmp)
        entry = work / "canvas.tsx"
        entry.write_text(tsx, encoding="utf8")

        # Typecheck against the kit's declarations. `--noEmit` is the whole
        # point: esbuild will not catch a misused component prop.
        tsconfig = {
            "compilerOptions": {
                "target": "ES2020",
                "lib": ["ES2020", "DOM"],
                "module": "ESNext",
                "moduleResolution": "bundler",
                "jsx": "react-jsx",
                "strict": True,
                "skipLibCheck": True,
                "noEmit": True,
                "esModuleInterop": True,
                "types": [],
                "baseUrl": str(root),
                "paths": {"*": ["node_modules/*"]},
            },
            "include": [str(entry)],
        }
        (work / "tsconfig.json").write_text(json.dumps(tsconfig), encoding="utf8")

        tsc = root / "node_modules" / ".bin" / "tsc"
        if tsc.exists():
            code, output = _run([str(tsc), "-p", str(work / "tsconfig.json")], cwd=root)
            if code != 0:
                return (
                    BuildReport(
                        ok=False,
                        failed_stage="typecheck",
                        kit_version=kit_version,
                        diagnostics=[
                            line for line in output.splitlines() if line.strip()
                        ][:40],
                        duration_ms=int((time.monotonic() - started) * 1000),
                    ),
                    "",
                )

        esbuild = root / "node_modules" / ".bin" / "esbuild"
        if not esbuild.exists():
            return (
                BuildReport(
                    ok=False,
                    failed_stage="bundle",
                    diagnostics=["esbuild is not present in the canvas toolchain."],
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
                "",
            )

        out = work / "canvas.mjs"
        command = [
            str(esbuild),
            str(entry),
            "--bundle",
            "--format=esm",
            "--target=es2020",
            "--jsx=automatic",
            "--platform=browser",
            f"--outfile={out}",
        ]
        command += [f"--external:{name}" for name in sorted(ALLOWED_IMPORTS)]

        code, output = _run(command, cwd=root)
        if code != 0:
            return (
                BuildReport(
                    ok=False,
                    failed_stage="bundle",
                    kit_version=kit_version,
                    diagnostics=[line for line in output.splitlines() if line.strip()][
                        :40
                    ],
                    duration_ms=int((time.monotonic() - started) * 1000),
                ),
                "",
            )

        code_text = out.read_text(encoding="utf8")

    return (
        BuildReport(
            ok=True,
            kit_version=kit_version,
            bundle_sha=hashlib.sha256(code_text.encode("utf8")).hexdigest(),
            bytes=len(code_text.encode("utf8")),
            duration_ms=int((time.monotonic() - started) * 1000),
        ),
        code_text,
    )


def toolchain_available() -> bool:
    """Whether a canvas can be compiled in this environment."""
    root = _toolchain_root()
    return root is not None and (root / "node_modules" / ".bin" / "esbuild").exists()
