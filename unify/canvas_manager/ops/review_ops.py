"""Render a compiled canvas headlessly and look at the result.

This runs locally in the assistant pod, offline. It deliberately does not use
``primitives.computer.web``: that routes through agent-service on the assistant
desktop VM, a different machine, and console's canvas routes are session-authed,
so the assistant has no session to render with. Making it work would mean
minting an unauthenticated preview URL -- reintroducing exactly the
token-guessable render path the rest of the design removes.

Rendering locally instead needs no authentication at all, because nothing is
remote. It is also the only gate that catches a canvas which compiles cleanly and
then throws on mount, which neither the linter nor the typechecker can see.
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from unify.canvas_manager.types.view import ReviewReport

logger = logging.getLogger(__name__)

# Where the assistant image vendors the runtime host, so a preview renders
# against byte-identical assets to the ones console serves.
HOST_ROOTS = (
    Path("/opt/canvas-host"),
    Path.home() / ".unity" / "canvas-host",
)


def _host_root() -> Optional[Path]:
    for candidate in HOST_ROOTS:
        if (candidate / "index.html").is_file():
            return candidate
    return None


def render_and_review(
    *,
    token: str,
    bundle: str,
    props: Dict[str, Any],
    rows: Optional[Dict[str, Any]] = None,
) -> ReviewReport:
    """Render one canvas in both themes and report what happened.

    ``rendered`` is a hard gate. ``verdict`` and ``issues`` are advisory: they go
    back to the actor, which decides whether to revise.
    """
    host = _host_root()
    if host is None:
        # Not a failure: environments without the vendored host (local dev,
        # some CI lanes) skip the visual gate rather than block authoring.
        return ReviewReport(rendered=True, verdict="skipped: no canvas host available")

    script = host / "scripts" / "render_canvas.mjs"
    if not script.is_file():
        return ReviewReport(rendered=True, verdict="skipped: renderer not installed")

    node = shutil.which("node")
    if node is None:
        return ReviewReport(rendered=True, verdict="skipped: node unavailable")

    with tempfile.TemporaryDirectory(prefix="canvas-review-") as tmp:
        work = Path(tmp)
        (work / "bundle.mjs").write_text(bundle, encoding="utf8")
        (work / "props.json").write_text(
            json.dumps({"props": props, "data": rows or {}}),
            encoding="utf8",
        )

        try:
            completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
                [node, str(script), "--work", str(work), "--token", token],
                cwd=str(host),
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            return ReviewReport(
                rendered=False,
                error="Canvas render timed out after 120s, which usually means an "
                "effect loops on mount.",
            )

        if completed.returncode != 0:
            return ReviewReport(
                rendered=False,
                error=(completed.stdout + completed.stderr).strip()[:2000],
            )

        shots = sorted(str(path) for path in work.glob("*.png"))

    return ReviewReport(rendered=True, screenshots=shots, verdict="rendered")
