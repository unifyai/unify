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

import functools
import hashlib
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import List, Optional, Tuple

from unify.canvas_manager.settings import CanvasSettings
from unify.canvas_manager.types.view import BuildReport

# Specifiers every environment can rely on. The full allowlist is the
# toolchain's canvas-externals.json — emitted from the same specifier list
# that drives the runtime host's import map, so the authoring gate and the
# host cannot disagree about what resolves at view time.
CORE_IMPORTS = frozenset(
    {
        "react",
        "react-dom",
        "react-dom/client",
        "react/jsx-runtime",
        "@unity/canvas-kit",
    },
)


@functools.lru_cache(maxsize=1)
def allowed_imports() -> frozenset:
    """Specifiers a canvas may import.

    Everything else is absent at view time: the frame has no bundler and no
    network, so an unlisted import is a load failure rather than a slow path.
    Read from the installed toolchain because the list is owned by the runtime
    host (branding's canvas-specifiers.mjs); a hand-mirrored copy here is how
    the gate and the host would drift apart.
    """
    root = _toolchain_root()
    if root is None:
        return CORE_IMPORTS
    externals = root / "canvas-externals.json"
    if not externals.is_file():
        # A toolchain predating the vocabulary substrate: the closed core set
        # is exactly what it can resolve.
        return CORE_IMPORTS
    return frozenset(json.loads(externals.read_text(encoding="utf8")))


@functools.lru_cache(maxsize=1)
def class_manifest() -> Optional[frozenset]:
    """Every class the shipped stylesheet contains, or None when unknowable.

    The stylesheet is fixed at build time — Tailwind never runs per canvas —
    so a class outside this set silently styles nothing at view time. The lint
    checks authored class strings against it because that failure is otherwise
    invisible in review and unattributable in production.
    """
    root = _toolchain_root()
    if root is None:
        return None
    manifest = root / "classes.json"
    if not manifest.is_file():
        return None
    return frozenset(json.loads(manifest.read_text(encoding="utf8")))


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
                f"line {lineno}: hex colour is not allowed. Use a semantic token "
                f"utility (bg-primary, text-muted-foreground, bg-destructive, ...) "
                f"or seriesColor(n) / var(--chart-N) for chart fills.",
            )
        if _FUNCTIONAL_COLOUR.search(scannable):
            problems.append(
                f"line {lineno}: rgb()/hsl() colour is not allowed. Use a semantic "
                f"token utility or seriesColor(n) for chart fills.",
            )
        if _TAILWIND_COLOUR_CLASS.search(scannable):
            problems.append(
                f"line {lineno}: named colour utility class is not allowed and has no effect — "
                f"the canvas stylesheet ships no colour palette. Use a semantic token "
                f"utility (bg-primary, text-muted-foreground, bg-destructive, ...).",
            )
        if _ARBITRARY_COLOUR_CLASS.search(scannable):
            problems.append(
                f"line {lineno}: arbitrary colour class is not allowed. Use a "
                f"semantic token utility.",
            )
        if _INLINE_FONT_FAMILY.search(scannable):
            problems.append(
                f"line {lineno}: inline font-family is not allowed; the host "
                f"stylesheet sets typography.",
            )

    imports = allowed_imports()
    for match in list(_IMPORT_SPECIFIER.finditer(tsx)) + list(
        _BARE_IMPORT.finditer(tsx),
    ):
        specifier = match.group(1)
        if specifier.startswith("."):
            problems.append(
                f"relative import {specifier!r} is not allowed: a canvas is a "
                f"single module — inline the component source instead.",
            )
        elif specifier not in imports:
            problems.append(
                f"import {specifier!r} is not available at view time. "
                f"Only {', '.join(sorted(imports))} are provided.",
            )

    problems.extend(_lint_class_strings(tsx))

    return problems


# Class strings inside these constructs are utility classes by definition,
# which is what makes checking them against the manifest sound: prose and
# test-ids never appear here, so a flagged token is a real no-op class.
_CLASS_ATTRIBUTES = re.compile(
    r"""className\s*=\s*(?:"([^"]*)"|\{\s*`([^`]*)`\s*\})""",
)
_CN_ARGS = re.compile(r"""\bcn\(([^)]*)\)""", re.DOTALL)
_QUOTED = re.compile(r"""["'`]([^"'`]*)["'`]""")


def _lint_class_strings(tsx: str) -> List[str]:
    """Flag utility classes the shipped stylesheet does not contain.

    Tailwind never runs per canvas, so such a class silently styles nothing at
    view time — invisible in review, unattributable in production. Template
    interpolations are skipped (their value is unknowable statically); the
    render review remains the backstop for what this cannot see.
    """
    manifest = class_manifest()
    if manifest is None:
        return []

    candidates: set = set()
    for match in _CLASS_ATTRIBUTES.finditer(tsx):
        candidates.update((match.group(1) or match.group(2) or "").split())
    for match in _CN_ARGS.finditer(tsx):
        for quoted in _QUOTED.finditer(match.group(1)):
            candidates.update(quoted.group(1).split())

    problems: List[str] = []
    for token in sorted(candidates):
        if not token or "${" in token or token in manifest:
            continue
        # Colour classes already carry a line-anchored diagnostic with the
        # colour-specific remedy; a second report for the same token is noise.
        if _TAILWIND_COLOUR_CLASS.search(token) or _ARBITRARY_COLOUR_CLASS.search(
            token,
        ):
            continue
        problems.append(
            f"class {token!r} is not in the shipped stylesheet and will "
            f"silently style nothing. Use classes the vocabulary corpus uses, "
            f"or a semantic-token utility the manifest lists.",
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

    Configured via ``UNIFY_CANVAS_TOOLCHAIN_ROOT``; the fallbacks are where the
    assistant image installs it. The toolchain is vendored into the image rather
    than fetched, so authoring needs no network and no separate build service.
    """
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

    # Checked before lint: without a toolchain the allowlist and the class
    # manifest degrade to floors, and a vocabulary import would be rejected
    # with a misleading lint message instead of the real, actionable problem.
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

    # Built inside the toolchain so ordinary node resolution walks up into its
    # node_modules. Path mapping would also work but silently resolves nothing
    # when the toolchain layout changes, which surfaces as a confusing
    # "cannot find module 'react'" rather than a missing toolchain.
    builds = root / ".builds"
    builds.mkdir(exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="canvas-", dir=str(builds)) as tmp:
        work = Path(tmp)
        entry = work / "canvas.tsx"
        entry.write_text(tsx, encoding="utf8")

        # Typecheck against the kit's declarations. `--noEmit` is the whole
        # point: esbuild will not catch a misused component prop.
        tsconfig = {
            "compilerOptions": {
                "target": "ES2020",
                "lib": ["ES2020", "DOM", "DOM.Iterable"],
                "module": "ESNext",
                "moduleResolution": "bundler",
                "jsx": "react-jsx",
                "strict": True,
                "skipLibCheck": True,
                "noEmit": True,
                "esModuleInterop": True,
                # Nothing is auto-included as a global; the canvas gets exactly
                # the types it imports.
                "types": [],
            },
            "include": [str(entry)],
        }
        (work / "tsconfig.json").write_text(json.dumps(tsconfig), encoding="utf8")

        # Both tools run with the build directory as cwd. esbuild writes the
        # entry's cwd-relative path into the output as a comment, so running from
        # anywhere else would put the temp directory name in the bundle and give
        # identical source a different sha on every build. It also keeps tsc
        # diagnostics reading `canvas.tsx(5,3)` rather than a temp path the
        # author cannot act on.
        tsc = root / "node_modules" / ".bin" / "tsc"
        if tsc.exists():
            code, output = _run([str(tsc), "-p", str(work / "tsconfig.json")], cwd=work)
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
            entry.name,
            "--bundle",
            "--format=esm",
            "--target=es2020",
            "--jsx=automatic",
            "--platform=browser",
            f"--outfile={out.name}",
        ]
        command += [f"--external:{name}" for name in sorted(allowed_imports())]

        code, output = _run(command, cwd=work)
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

    encoded = code_text.encode("utf8")
    ceiling = CanvasSettings().MAX_BUNDLE_BYTES
    if len(encoded) > ceiling:
        # The bundle is stored on the canvas row, so this bounds row size. A
        # canvas this large is almost always inlining a dataset that belongs in
        # a binding, which is also why the remedy is named rather than implied.
        return (
            BuildReport(
                ok=False,
                failed_stage="bundle",
                kit_version=kit_version,
                diagnostics=[
                    f"Compiled canvas is {len(encoded)} bytes, over the "
                    f"{ceiling}-byte limit. Move inlined data into a binding "
                    f"rather than embedding it in the source.",
                ],
                duration_ms=int((time.monotonic() - started) * 1000),
            ),
            "",
        )

    return (
        BuildReport(
            ok=True,
            kit_version=kit_version,
            bundle_sha=hashlib.sha256(encoded).hexdigest(),
            bytes=len(encoded),
            duration_ms=int((time.monotonic() - started) * 1000),
        ),
        code_text,
    )


def toolchain_available() -> bool:
    """Whether a canvas can be compiled in this environment."""
    root = _toolchain_root()
    return root is not None and (root / "node_modules" / ".bin" / "esbuild").exists()
