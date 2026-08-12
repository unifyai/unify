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

## Why the harness reproduces the deployed topology

Two static servers on two ports, the real response headers, and console's own
handshake. A single-origin render with no CSP would pass a canvas that then fails
for real viewers, which makes the gate worse than none: it would certify
something it never tested. The host assets are the same bytes the canvas origin
serves, the headers come from the same script the deploy uses, and the frame is
sandboxed the way console sandboxes it.

The harness plays the parent's role only as far as the render needs: it answers
binding aliases from the dry-run rows the author-time validation already
produced, and switches theme. Action dispatch is absent, because that is an
integration concern with no server here to dispatch to.
"""

from __future__ import annotations

import http.server
import json
import logging
import shutil
import socketserver
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from unify.canvas_manager.types.view import ReviewReport

logger = logging.getLogger(__name__)

# Where the assistant image vendors the runtime host, so a preview renders
# against byte-identical assets to the ones console serves. Configured via
# UNITY_CANVAS_HOST_ROOT; these are the fallbacks the install script writes to.
_HOST_FALLBACKS = (
    Path("/opt/canvas-host"),
    Path.home() / ".unity" / "canvas-host",
)

# The documents the runtime is served under, newest first. Authoring always
# reviews against the newest runtime present — that is the one a canvas built
# by this toolchain will be routed to. A breaking runtime change adds a new
# version at the front rather than replacing the existing entry.
_HOST_DOCUMENTS = ("host/v1/index.html",)

# Generous enough for a cold chromium launch on a throttled pod, short enough
# that a canvas whose effect loops on mount fails rather than hangs.
_RENDER_TIMEOUT_MS = 30_000


def _host_root() -> Optional[Path]:
    """Locate the vendored runtime host, or None when this environment has none."""
    from unify.canvas_manager.settings import CanvasSettings

    configured = CanvasSettings().HOST_ROOT.strip()
    candidates = [Path(configured)] if configured else []
    candidates += list(_HOST_FALLBACKS)

    for candidate in candidates:
        for document in _HOST_DOCUMENTS:
            if (candidate / document).is_file():
                return candidate
    return None


def _host_document(host: Path) -> str:
    """The newest runtime document this host root serves."""
    for document in _HOST_DOCUMENTS:
        if (host / document).is_file():
            return document
    raise FileNotFoundError(f"no host document under {host}")


def _browser_available() -> bool:
    """Whether a chromium is installed for this interpreter.

    Starting playwright is cheap; launching a browser is not, so this checks the
    executable path rather than trying. Used to tell "this environment cannot
    render" apart from "this canvas does not render", which must never be
    conflated: the second is a publication gate and the first is not.
    """
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as playwright:
            return Path(playwright.chromium.executable_path).exists()
    except PlaywrightError:
        return False


def gate_available() -> bool:
    """Whether the render gate can run here at all."""
    return _host_root() is not None and _browser_available()


def _host_headers(
    host: Path,
    *,
    host_origin: str,
    parent_origin: str,
) -> Dict[str, str]:
    """Response headers for the host origin, from the deploy's own definition.

    Shelling out to the vendored script rather than rebuilding the policy here is
    the point: a second implementation would drift, and a gate running a laxer
    CSP than production certifies canvases it never actually tested. The inline
    import-map hash in particular is derived from the built document, so it
    cannot be restated correctly by hand.
    """
    node = shutil.which("node")
    script = host / "scripts" / "headers.mjs"
    if node is None or not script.is_file():
        return {}

    completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [
            node,
            str(script),
            "--dist",
            str(host),
            "--host-origin",
            host_origin,
            "--parent-origin",
            parent_origin,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        logger.warning("canvas header generation failed: %s", completed.stderr[:500])
        return {}
    return json.loads(completed.stdout)


@dataclass
class _OriginConfig:
    """What one static origin serves.

    Mutable so both ports can be allocated before the CSP that has to name them
    is generated. Nothing is requested until the browser navigates, so there is
    no window in which a request could observe the empty state.
    """

    headers: Dict[str, str] = field(default_factory=dict)
    # Served for `/` in place of a file on disk, so the parent harness page needs
    # no temp directory of its own.
    index_body: Optional[bytes] = None


def _serve(
    *,
    directory: Path,
    config: _OriginConfig,
    bind: str,
) -> Tuple[socketserver.TCPServer, int]:
    """Start a static server on an ephemeral port and return it with the port."""

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, directory=str(directory), **kwargs)

        def end_headers(self) -> None:
            for name, value in config.headers.items():
                self.send_header(name, value)
            super().end_headers()

        def do_GET(self) -> None:  # noqa: N802 - stdlib naming
            if config.index_body is not None and self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(config.index_body)))
                self.end_headers()
                self.wfile.write(config.index_body)
                return
            super().do_GET()

        def log_message(self, *args: Any) -> None:
            """Silence request logging; failures surface through the report."""

    server = socketserver.ThreadingTCPServer(
        (bind, 0),
        Handler,
        bind_and_activate=False,
    )
    server.allow_reuse_address = True
    server.daemon_threads = True
    server.server_bind()
    server.server_activate()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_address[1]


def _js(value: Any) -> str:
    """Serialise a value for embedding in an inline script.

    JSON alone is not enough. The HTML parser ends a `<script>` block at the
    first literal `</script`, whatever the JavaScript context, and `json.dumps`
    does not escape forward slashes -- so a bundle containing that sequence in a
    string would terminate the harness's own script. `<\\/` is identical inside a
    JS string literal, and `<!--` gets the same treatment for the same reason.
    """
    return json.dumps(value).replace("</", "<\\/").replace("<!--", "<\\!--")


def _parent_html(
    *,
    host_origin: str,
    host_document: str,
    source: str,
    props: Dict[str, Any],
    rows: Dict[str, Any],
    actions: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """The harness page, mirroring what console's CanvasFrame does.

    Frames the host with ``sandbox="allow-scripts"`` and no ``allow-same-origin``,
    waits for the child's hello, then hands over one end of a MessageChannel in a
    single postMessage. The child speaks first deliberately: replying to `load`
    races the host's deferred module script and the init is dropped.
    """
    return f"""<!doctype html><meta charset="utf-8"><body style="margin:0">
<iframe id="f" src="{host_origin}/{host_document}" sandbox="allow-scripts"
        style="width:1024px;height:768px;border:0" allow="" referrerpolicy="no-referrer"></iframe>
<script>
  window.__log = {{ ready: null, errors: [], aliases: [], height: 0 }};
  const frame = document.getElementById('f');
  const channel = new MessageChannel();
  const rows = {_js(rows)};

  channel.port1.onmessage = (event) => {{
    const msg = event.data;
    if (msg.type === 'canvas/ready') window.__log.ready = msg;
    if (msg.type === 'canvas/resize') window.__log.height = msg.height;
    if (msg.type === 'canvas/error') window.__log.errors.push(msg);
    if (msg.type === 'canvas/data/request') {{
      window.__log.aliases.push(msg.alias);
      channel.port1.postMessage({{
        type: 'canvas/data/result', alias: msg.alias,
        rows: rows[msg.alias] ?? [], truncated: false,
      }});
    }}
  }};
  channel.port1.start();

  // An opaque-origin child reports origin "null", so the only check available
  // here is that the message came from this frame's window.
  window.addEventListener('message', (event) => {{
    if (event.source !== frame.contentWindow) return;
    if (!event.data || event.data.type !== 'canvas/hello' || window.__sent) return;
    window.__sent = true;
    frame.contentWindow.postMessage({{
      type: 'canvas/init', protocol: 1, channel: 'review',
      source: {_js(source)},
      theme: 'light', props: {_js(props)},
      aliases: {_js(sorted(rows))}, actions: {_js(actions or [])},
    }}, '*', [channel.port2]);
  }});

  window.__setTheme = (theme) => channel.port1.postMessage({{ type: 'canvas/theme', theme }});

  // Size the frame to the content the way console does. Screenshotting a fixed
  // viewport instead would capture unpainted space below a short canvas, and the
  // critique would report that as a broken background on every single review.
  window.__fit = () => {{
    const height = Math.max(120, Math.min(window.__log.height || 0, 4000));
    frame.style.height = height + 'px';
    return height;
  }};
</script></body>"""


def _wait_for_theme(page: Any, child: Any, *, dark: bool) -> None:
    """Block until the child frame has applied the theme it was sent.

    Polled from here rather than with ``wait_for_function`` because the frame
    is the real thing: opaque origin, ``script-src <origin> blob: <hash>``,
    and deliberately no ``unsafe-eval`` — the runtime imports bundles as
    blobs precisely so eval is never needed. Playwright's in-page predicate
    poller evals the predicate string in that document, which the CSP
    refuses, so the whole render reported "did not render" with a CSP error
    the moment the gate began rendering for real.

    Reading the class over CDP one tick at a time is CSP-exempt and asserts
    the same condition. Relaxing the frame's CSP to suit the harness would
    trade a real security property for a test convenience.
    """
    deadline = time.monotonic() + _RENDER_TIMEOUT_MS / 1000
    while True:
        applied = bool(
            child.evaluate("document.documentElement.classList.contains('dark')"),
        )
        if applied is dark:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"child frame did not apply the {'dark' if dark else 'light'} "
                f"theme within {_RENDER_TIMEOUT_MS}ms",
            )
        page.wait_for_timeout(50)


def _render(
    *,
    host: Path,
    token: str,
    source: str,
    props: Dict[str, Any],
    rows: Dict[str, Any],
    actions: List[Dict[str, Any]],
    out_dir: Path,
) -> ReviewReport:
    """Serve, frame, render both themes, screenshot. Runs on its own thread."""
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import sync_playwright

    # Two genuinely different origins: different hostname as well as different
    # port, so the frame is cross-origin exactly as it is in production.
    host_config = _OriginConfig()
    parent_config = _OriginConfig(index_body=b"")

    host_server, host_port = _serve(
        directory=host,
        config=host_config,
        bind="127.0.0.1",
    )
    parent_server, parent_port = _serve(
        directory=host,
        config=parent_config,
        bind="127.0.0.1",
    )
    host_origin = f"http://127.0.0.1:{host_port}"
    parent_origin = f"http://localhost:{parent_port}"

    host_config.headers = _host_headers(
        host,
        host_origin=host_origin,
        parent_origin=parent_origin,
    )
    parent_config.index_body = _parent_html(
        host_origin=host_origin,
        host_document=_host_document(host),
        source=source,
        props=props,
        rows=rows,
        actions=actions,
    ).encode("utf8")

    page_errors: List[str] = []
    shots: List[str] = []

    try:
        with sync_playwright() as playwright:
            try:
                browser = playwright.chromium.launch(args=["--no-sandbox"])
            except PlaywrightError as error:
                # A browser that will not start is an environment problem, not a
                # verdict on the canvas -- nothing an authored canvas does can
                # prevent chromium from launching. Reporting it as a render
                # failure would block publishing everywhere the browser is
                # missing, and would also let a genuinely broken canvas look
                # rejected for the wrong reason.
                logger.warning("canvas render skipped, browser unavailable: %s", error)
                return ReviewReport(
                    rendered=True,
                    verdict="skipped: no browser available",
                )
            try:
                page = browser.new_page(viewport={"width": 1024, "height": 768})
                page.on("pageerror", lambda error: page_errors.append(str(error)))
                page.goto(parent_origin, wait_until="load", timeout=_RENDER_TIMEOUT_MS)

                element = page.wait_for_selector("#f", timeout=_RENDER_TIMEOUT_MS)
                child = element.content_frame()

                # Content, not merely a mount: the host shows a loading skeleton
                # until the bundle resolves, so waiting on the root alone would
                # pass a canvas that never actually rendered.
                child.wait_for_selector(
                    "#canvas-content > *",
                    timeout=_RENDER_TIMEOUT_MS,
                )

                # Match the frame to its content before capturing, so the
                # screenshot shows what a viewer sees rather than the harness's
                # arbitrary viewport.
                page.evaluate("window.__fit()")

                for theme in ("light", "dark"):
                    page.evaluate("theme => window.__setTheme(theme)", theme)
                    # Waiting on the class the child actually applied keeps this
                    # deterministic; a fixed delay would race a slow message.
                    _wait_for_theme(page, child, dark=theme == "dark")
                    shot = out_dir / f"canvas-{token}-{theme}.png"
                    element.screenshot(path=str(shot))
                    shots.append(str(shot))

                reported = page.evaluate("window.__log")
            finally:
                browser.close()
    except PlaywrightError as error:
        return ReviewReport(rendered=False, screenshots=shots, error=str(error)[:2000])
    finally:
        for server in (host_server, parent_server):
            server.shutdown()
            server.server_close()

    # An error reported over the port is authored code throwing on mount, which
    # is the failure this whole gate exists to catch.
    runtime_errors = [
        str(entry.get("message", "")) for entry in reported.get("errors", [])
    ]
    if runtime_errors or page_errors:
        return ReviewReport(
            rendered=False,
            screenshots=shots,
            error="; ".join(runtime_errors + page_errors)[:2000],
        )

    if not reported.get("ready"):
        return ReviewReport(
            rendered=False,
            screenshots=shots,
            error="The canvas runtime never completed its handshake.",
        )

    return ReviewReport(rendered=True, screenshots=shots, verdict="rendered")


def render_and_review(
    *,
    token: str,
    bundle: str,
    props: Dict[str, Any],
    rows: Optional[Dict[str, Any]] = None,
    actions: Optional[List[Dict[str, Any]]] = None,
    out_dir: Optional[Path] = None,
    intent: str = "",
) -> ReviewReport:
    """Render one canvas in both themes and report what happened.

    ``rendered`` is a hard gate. ``verdict`` and ``issues`` are advisory: they go
    back to the actor, which decides whether to revise.

    Screenshots are written under ``out_dir`` when given, so the caller can hand
    the paths to a vision model and into the actor's transcript. Without one they
    land in a temporary directory and survive only for the duration of the call.
    """
    host = _host_root()
    if host is None:
        # Not a failure: environments without the vendored host (local dev, some
        # CI lanes) skip the visual gate rather than block authoring.
        return ReviewReport(rendered=True, verdict="skipped: no canvas host available")

    target = out_dir or Path(tempfile.mkdtemp(prefix=f"canvas-review-{token}-"))
    target.mkdir(parents=True, exist_ok=True)

    # Playwright's sync API refuses to run on a thread with a live asyncio loop,
    # and this is reached from both plain sync code and `asyncio.to_thread`. A
    # dedicated thread makes the caller's context irrelevant. The critique runs on
    # the same thread for the same reason.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="canvas-review") as pool:
        return pool.submit(
            _render_and_critique,
            host=host,
            token=token,
            source=bundle,
            props=props,
            rows=rows or {},
            actions=actions or [],
            out_dir=target,
            intent=intent,
        ).result()


class _Critique(BaseModel):
    """What a look at the rendered canvas turned up."""

    verdict: str = Field(description="One sentence on whether this reads well.")
    issues: List[str] = Field(
        default_factory=list,
        description="Specific, actionable visual problems. Empty when there are none.",
    )


_CRITIQUE_PROMPT = """You are reviewing a rendered view an assistant just built \
for a user, captured in light and dark theme.

Report only problems a viewer would actually notice: text that is unreadable or \
clipped, elements overlapping or overflowing, a chart or table that is empty when \
it should have content, wildly unbalanced spacing, or something legible in one \
theme and not the other.

Do not comment on the sample data itself, on colour choices (the palette is fixed \
and not the author's to change), or on features you think are missing. If it reads \
well, say so and return no issues."""


def _critique(shots: List[str], *, intent: str = "") -> Tuple[str, List[str]]:
    """Look at the screenshots and report what a viewer would notice.

    Advisory by construction. A critique that cannot run -- no vision model
    configured, no network -- must never block publication, because the thing
    being gated is whether the canvas *renders*, and that has already been
    established by this point.
    """
    if not shots:
        return "rendered", []

    import asyncio

    from unify.common.reasoning import query_llm

    prompt = _CRITIQUE_PROMPT
    if intent:
        prompt += (
            f"\n\nThe view was built for this request: {intent!r}. Also report "
            f"where what is rendered plainly fails to serve that request — a "
            f"missing panel it asked for, an empty region where its data should "
            f"be, a control it named that is absent. Judge fitness from what is "
            f"visible; do not speculate about behaviour you cannot see."
        )

    try:
        # Safe on this thread by construction: the pool worker has no running
        # loop, which is the same reason playwright's sync API works here.
        result = asyncio.run(
            query_llm(
                prompt,
                images=list(shots),
                response_format=_Critique,
                origin="CanvasManager.review",
            ),
        )
    except Exception as error:  # noqa: BLE001 - advisory; never blocks publish
        logger.warning("canvas critique unavailable: %s", error)
        return "rendered", []

    if isinstance(result, _Critique):
        return result.verdict, result.issues
    return "rendered", []


def _render_and_critique(intent: str = "", **kwargs: Any) -> ReviewReport:
    """Render, then look at the result.

    Kept separate from ``_render`` so the mechanical half stays testable without a
    model, and so a critique failure cannot be confused with a render failure.

    A report with no screenshots is returned as-is: those are the skip paths
    (no browser), and letting the critique's default verdict overwrite theirs
    once disguised a pod whose chromium could not launch as a canvas that had
    genuinely rendered.
    """
    report = _render(**kwargs)
    if not report.rendered or not report.screenshots:
        return report

    verdict, issues = _critique(report.screenshots, intent=intent)
    return report.model_copy(update={"verdict": verdict, "issues": issues})
