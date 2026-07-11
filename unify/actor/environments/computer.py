from __future__ import annotations

import inspect
from typing import Any, Dict, Optional
import asyncio

from unify.actor.environments.base import (
    BaseEnvironment,
    ToolMetadata,
    build_filtered_method_docs,
)
from unify.function_manager.primitives import ComputerPrimitives, get_registry


class ComputerEnvironment(BaseEnvironment):
    """Computer control environment backed by ``ComputerPrimitives``.

    Exposes two interfaces for generated plan code:

    - ``primitives.computer.desktop.*``  -- singleton desktop control (mouse/keyboard)
    - ``primitives.computer.web.new_session(visible=...)``  -- factory for browser sessions

    Lives under the unified ``primitives`` namespace alongside state managers
    and actor delegation.
    """

    NAMESPACE = "primitives"
    MANAGER_ALIAS = "computer"

    def __init__(
        self,
        computer_primitives: ComputerPrimitives,
        *,
        allowed_methods: Optional[set[str]] = None,
        clarification_up_q: Optional[asyncio.Queue[str]] = None,
        clarification_down_q: Optional[asyncio.Queue[str]] = None,
    ):
        from unify.function_manager.primitives import Primitives, PrimitiveScope

        self._computer_primitives = computer_primitives
        self._allowed_methods = frozenset(allowed_methods) if allowed_methods else None
        primitives = Primitives(
            primitive_scope=PrimitiveScope(
                scoped_managers=frozenset({self.MANAGER_ALIAS}),
            ),
        )
        primitives._managers[self.MANAGER_ALIAS] = computer_primitives
        super().__init__(
            instance=primitives,
            namespace=self.NAMESPACE,
            clarification_up_q=clarification_up_q,
            clarification_down_q=clarification_down_q,
        )

    @property
    def namespace(self) -> str:
        return self.NAMESPACE

    def get_instance(self) -> Any:
        return self._instance

    def get_tools(self) -> Dict[str, ToolMetadata]:
        impure = {"navigate", "act", "new_session"}
        desktop_tool_names = [
            "navigate",
            "act",
            "observe",
            "query",
            "get_links",
            "get_screenshot",
        ]

        registry = get_registry()
        tools: Dict[str, ToolMetadata] = {}

        # Desktop namespace -- singleton, full method set
        desktop_ns = self._computer_primitives.desktop
        for name in desktop_tool_names:
            fq_name = f"{self.NAMESPACE}.{self.MANAGER_ALIAS}.desktop.{name}"
            if (
                self._allowed_methods is not None
                and fq_name not in self._allowed_methods
            ):
                continue
            fn = getattr(desktop_ns, name, None)
            if fn is None or not callable(fn):
                continue
            try:
                signature = str(inspect.signature(fn))
            except Exception:
                signature = None
            tools[fq_name] = ToolMetadata(
                name=fq_name,
                is_impure=name in impure,
                is_steerable=False,
                docstring=getattr(fn, "__doc__", None),
                signature=signature,
                function_id=registry.get_function_id(self.MANAGER_ALIAS, name),
                function_context="primitive",
            )

        # Web factory -- new_session(), list_sessions(), and get_session()
        web_factory = self._computer_primitives.web
        for factory_method_name in ("new_session", "list_sessions", "get_session"):
            fq_name = f"{self.NAMESPACE}.{self.MANAGER_ALIAS}.web.{factory_method_name}"
            if (
                self._allowed_methods is not None
                and fq_name not in self._allowed_methods
            ):
                continue
            fn = getattr(web_factory, factory_method_name, None)
            if fn is None or not callable(fn):
                continue
            try:
                signature = str(inspect.signature(fn))
            except Exception:
                signature = None
            tools[fq_name] = ToolMetadata(
                name=fq_name,
                is_impure=factory_method_name == "new_session",
                is_steerable=False,
                docstring=getattr(fn, "__doc__", None),
                signature=signature,
                function_id=registry.get_function_id(
                    self.MANAGER_ALIAS,
                    factory_method_name,
                ),
                function_context="primitive",
            )

        # User-desktop factory -- session() and list_linked()
        user_desktop_factory = self._computer_primitives.user_desktop
        for factory_method_name in ("session", "list_linked"):
            fq_name = (
                f"{self.NAMESPACE}.{self.MANAGER_ALIAS}.user_desktop."
                f"{factory_method_name}"
            )
            if (
                self._allowed_methods is not None
                and fq_name not in self._allowed_methods
            ):
                continue
            fn = getattr(user_desktop_factory, factory_method_name, None)
            if fn is None or not callable(fn):
                continue
            try:
                signature = str(inspect.signature(fn))
            except Exception:
                signature = None
            tools[fq_name] = ToolMetadata(
                name=fq_name,
                is_impure=False,
                is_steerable=False,
                docstring=getattr(fn, "__doc__", None),
                signature=signature,
                function_id=registry.get_function_id(
                    self.MANAGER_ALIAS,
                    factory_method_name,
                ),
                function_context="primitive",
            )

        return tools

    def get_prompt_context(self) -> str:
        """Generate prompt context with desktop + web factory guidance."""
        parts: list[str] = []

        parts.append(
            "### Computer Control\n\n"
            "The VM desktop runs on X11 display `:99` (1920×1080 physical).  "
            "Desktop and visible-web interactions use native input via "
            "DisplayHarness: xdotool for mouse/keyboard, scrot for screenshots.  "
            "`observe()` on the desktop performs vision extraction over a "
            "full-display screenshot -- the correct way to verify what is "
            "currently visible.  `get_content()` is intentionally not exposed "
            "on the desktop namespace because there is no DOM to query.\n\n"
            "Two interfaces for controlling the desktop:\n\n"
            "#### `primitives.computer.desktop` -- Desktop Control (singleton)\n\n"
            "Sends mouse and keyboard actions to the VM desktop via native X11 "
            "input (xdotool).  There is exactly one desktop session -- it "
            "persists for the lifetime of the assistant.  Use this for native "
            "desktop apps, terminal operations, file managers, and any "
            "interaction with windows already visible on the desktop.\n\n"
            "```python\n"
            "await primitives.computer.desktop.act('Open the Terminal app')\n"
            "display(await primitives.computer.desktop.get_screenshot())\n"
            "```\n\n"
            "#### `primitives.computer.web` -- Web Sessions (factory + registry)\n\n"
            "Creates independent browser sessions and lets you reattach to "
            "existing ones.  Each session is an isolated Chromium process with "
            "its own cookies, storage, and browsing context.  Multiple sessions "
            "can run in parallel.  Always call `stop()` when done.\n\n"
            "```python\n"
            "session = await primitives.computer.web.new_session()  # visible=True by default\n"
            "await session.navigate('https://example.com')\n"
            "data = await session.observe('Extract the main heading')\n"
            "display(await session.get_screenshot())\n"
            "await session.stop()\n"
            "```\n\n"
            "To continue working in a browser that already exists, reattach by "
            "session ID instead of opening a duplicate browser:\n\n"
            "```python\n"
            "session = primitives.computer.web.get_session(0)\n"
            "display(await session.get_screenshot())\n"
            "await session.act('Click the Continue button')\n"
            "```\n\n"
            "Use `primitives.computer.web.list_sessions(visible_only=True, active_only=True)` "
            "when you need to discover reusable sessions programmatically.  In "
            "voice/screen-share flows, visible session IDs are often surfaced in "
            "`<active_web_sessions>` for direct reuse.\n\n"
            "The `visible` parameter controls where and how the browser runs:\n"
            "- `visible=True` (default): browser window appears on the VM "
            "desktop (user can see it via screen sharing).  Mouse/keyboard "
            "actions route through the native display harness (xdotool); "
            "Playwright/CDP is retained for `navigate`, `get_content`, "
            "`get_links`, and captcha handling.\n"
            "- `visible=False`: headless Playwright-only browser.  Faster and "
            "invisible to the user; viewport screenshots and CDP control "
            "exclusively.\n\n"
            "`new_session()` creates, `get_session()` reattaches by ID, and "
            "`list_sessions()` enumerates existing handles.\n\n"
            "Session handles expose: "
            "`act`, `observe`, `query`, `navigate`, `get_links`, `get_content`, "
            "`get_screenshot`, plus `stop()`.\n\n"
            "#### When to Use Each\n\n"
            "- **Any task involving a web browser** -- "
            "`web.new_session(visible=True)`.  This is the default for all "
            "browser work and the only way to get a browser window visible on "
            "the desktop and in screenshots.  If the task involves a browser "
            "in any way, always use `web.new_session()`, not `desktop`.\n"
            "- **Background web lookup the user doesn't need to see** -- "
            "`web.new_session(visible=False)` for speed.\n"
            "- **Native desktop apps, terminal, file operations** -- "
            "`primitives.computer.desktop`.  Only for non-browser interactions.\n\n"
            "Note: mouse clicks on native-input sessions (desktop and "
            "visible web) hit the topmost window at the given coordinates.  "
            "If the target window is behind another, bring it to the front "
            "first or use `act()` with a natural-language instruction that "
            "includes focusing the correct window.\n\n"
            "#### Managed desktop filesystem\n\n"
            "The managed VM desktop is a separate filesystem from the local "
            "`execute_code` workspace.  Layout:\n"
            "- Desktop login name: `unityuser`.  Home is **`HOME=/Unity`** -- "
            "not `/home/unityuser` (that is not the desktop home).\n"
            "- Browser / XDG Downloads: **`/Unity/Downloads`**.\n"
            "- Synced workspace on the VM: **`/Unity/Local`** (bisync with the "
            "local pod workspace under Filesystem Context).\n"
            "- Other XDG dirs sit beside Downloads under `/Unity/` "
            "(Desktop, Documents, Pictures, ...).\n\n"
            "Path reading rules (on this VM desktop only):\n"
            "- A file-dialog or Thunar breadcrumb like `Unity > Downloads` "
            "means absolute **`/Unity/Downloads`** -- the home folder itself is "
            "named `Unity`.  Do not expand that breadcrumb as "
            "`~/Unity/Downloads` or `/home/unityuser/Unity/Downloads`.\n"
            "- Do not invent `/home/unityuser/...` from the panel username.\n"
            "- When reporting a saved or opened desktop path, use the absolute "
            "`/Unity/...` path you actually used.",
        )

        parts.append(
            "### Your Desktop vs. a User's Desktop\n\n"
            "`primitives.computer.desktop` and `primitives.computer.web` always "
            "drive **your own** managed desktop -- the machine shown in the "
            "Console live view.  This is your default workspace for every task; "
            "reach for it unless the user has explicitly asked you to act on "
            "*their* computer.\n\n"
            "`primitives.computer.user_desktop` drives a **user's own physical "
            "machine**, which they have linked to you in the Console and exposed "
            "over a secure tunnel.  It is never shown in the live view and it is "
            "their personal computer, so treat it with care.\n\n"
            "Rules of engagement for `user_desktop`:\n"
            "- **Only on explicit request.** Do not touch a user's machine "
            "unless that user has clearly asked you to do something on *their* "
            'computer (e.g. "open the file on my laptop", "click submit on my '
            'screen").  For everything else, use your own desktop.\n'
            "- **Clarify when unsure.** If the request is ambiguous -- which "
            "machine, which file or target, or whether they mean *their* "
            "computer rather than yours -- ask a brief clarifying question "
            "before acting rather than guessing.\n"
            "- **Confirm before consequential actions.** Their machine may hold "
            "personal data and live sessions.  Before anything destructive, "
            "irreversible, or that sends/deletes/purchases on their behalf, "
            "state what you are about to do and get a clear go-ahead.\n"
            "- **Respect revocation.** If a call raises `PermissionError`, the "
            "user has revoked control mid-session -- stop immediately, do not "
            "retry, and continue only on your own desktop.\n"
            "- **Never modify their machine to work around an error.** It is "
            "their personal computer, not a sandbox to repair.  If a session, "
            "screenshot, or file call fails because something is missing or "
            "broken on their side (e.g. a missing browser/driver, an unstarted "
            "service, a tunnel that is down), do **not** try to fix it by "
            "installing software, downloading binaries, running package "
            "managers (`npm`/`npx`/`pip`/`brew`/`choco`), or creating "
            "files/links to patch it.  That is an infrastructure gap, not your "
            "task -- report the failure plainly (what you tried, what error came "
            "back) and stop.  Only ever install or change things on their "
            "machine if the user explicitly asked you to.\n"
            "- **Discover before assuming.** Use "
            "`primitives.computer.user_desktop.list_linked()` to see whose "
            "machines are available; `session(user_id=...)` selects a specific "
            "one when more than one user has linked a desktop.\n\n"
            "```python\n"
            "# Act on the requesting user's own machine, on explicit request:\n"
            "ud = primitives.computer.user_desktop.session()\n"
            "display(await ud.get_screenshot())\n"
            "await ud.act('Open the Downloads folder')\n"
            "```\n\n"
            "For a pure 'look and tell me what's on my screen' request, capture "
            "the screenshot and read it with "
            "`query_llm(prompt, images=[png_bytes])`.  You do not need "
            "`observe()` or `act()` for perception.\n\n"
            "**Their files: use `user_desktop.files`, never shell.**  "
            "`primitives.computer.user_desktop.files` is the canonical way to "
            "read, fetch, or sync a user's files.  It reads/writes their home "
            "over SFTP and mirrors what you pull into `~/Unity/Remote/"
            "<user_id>/`, returning local paths you can parse -- this is a "
            "*separate* capability from the screen-control session above, and "
            "is what you reach for whenever the task is about file *content* "
            "rather than driving their screen:\n"
            "The model is mounting-like: browse cheaply, then fetch only what "
            "you need.\n"
            "- `files.list(path='', user_id=...)` -- browse their home tree with "
            "no copy (``''`` lists the home root; directories carry a trailing "
            "``/``). Start here; it shows the full tree.\n"
            "- `files.pull(path, user_id=...)` -- stage one home-relative file "
            "into the mirror when you need its contents or want to edit it; "
            "returns its absolute local path. This is your default fetch. "
            "Copies skip noise (caches, deps, VCS metadata) and credential dirs "
            "(`.ssh`/`.gnupg`/`.aws`/…), so they're never exfiltrated.\n"
            "- `files.sync(path='', user_id=...)` -- bulk-mirror a whole subtree "
            "at once. Use only when you genuinely need *everything* under it; "
            "scope to a subtree (e.g. ``'Documents'``) since ``''`` (entire "
            "home) is heavy and slow. Prefer `list`+`pull` otherwise.\n"
            "- `files.push(local_path, dest_path, user_id=...)` -- write a file "
            "back (saved as a timestamped copy; never overwrites their "
            "original).\n\n"
            "```python\n"
            "# Read the user's files lazily (NOT via shell): browse, then pull.\n"
            "names = await primitives.computer.user_desktop.files.list('Documents')\n"
            "path = await primitives.computer.user_desktop.files.pull('Documents/report.pdf')\n"
            "display(await primitives.files.parse(path))  # work from the staged mirror\n"
            "```\n\n"
            'Requests like "sync my filesystem", "pull my desktop files", '
            '"back up my home folder", or "grab the files off my machine" '
            'are first-class file requests -- not ambiguous "sync to where?" '
            "questions.  Default to `files.list` then `files.pull` for the "
            "specific paths in play; reserve `files.sync` for an explicit "
            "request to copy an entire folder. The destination is always your "
            "local mirror, so never ask the user where to sync to.  **Never** "
            "harvest their files by running shell "
            "`find`/`cat`/`tar`/`base64`/`cp`/`scp`/`rclone` on "
            '`surface="user_desktop"` -- that surface is only for commands the '
            "user explicitly wants executed on their machine, not for retrieving "
            "file content.",
        )

        parts.append(
            "### Locked macOS desktops show a screensaver -- recognise it and "
            "unlock\n\n"
            "A linked Mac that has been left alone is **locked**, and the live "
            "capture then shows its **screensaver**, not a desktop.  You know a "
            "linked machine is a Mac from "
            "`primitives.computer.user_desktop.list_linked()[...].os == 'macos'` "
            "-- that field is **authoritative**; never identify the OS from a "
            "screenshot or an LLM's guess.\n\n"
            "**Recognise the locked state.**  On a Mac, a full-screen "
            "moving/abstract pattern with **no menu bar, no Dock, and no window "
            "chrome** is the **screensaver**, which means the machine is locked "
            "or idle -- it is *not* artwork, a wallpaper, a browser, or an app "
            "the user opened.  The explicit lock screen (a centred clock + date, "
            "a user avatar, and an `Enter Password` / `Touch ID or Enter "
            "Password` field) is also the locked state.  In either case, treat "
            "the Mac as **locked** -- do **not** describe the pattern as on-screen "
            "content, and do **not** ask the user what 'unlock' means; "
            "recognising and clearing the lock is your job.\n\n"
            "**Unlock with the saved password (low-level, deterministic).**  The "
            "user's login password is stored as the secret "
            "`MACOS_USER_DESKTOP_PASSWORD`.  Use the placeholder form "
            "`${MACOS_USER_DESKTOP_PASSWORD}` -- it is resolved to the real value "
            "*on their machine* at the call boundary, so the plaintext never "
            "enters your context.\n\n"
            "**Judge each screen with `query_llm(prompt, images=[png_bytes])`** "
            "-- pass the screenshot to the LLM to decide what state the "
            "Mac is in.  Drive the *actions* with the **low-level** session "
            "methods (`press_key`, `get_screenshot`, `type_text`, "
            "`press_enter`), never `act()`/`observe()`, and read every "
            "screenshot back through `query_llm(images=...)`:\n"
            "```python\n"
            "import io\n"
            "ud = primitives.computer.user_desktop.session()\n"
            "def _png(img):\n"
            "    buf = io.BytesIO(); img.save(buf, 'PNG'); return buf.getvalue()\n"
            "shot = _png(await ud.get_screenshot())\n"
            "state = await query_llm(\n"
            "    'Is this a macOS lock screen or screensaver (a clock/avatar, a '\n"
            "    'password field, or a full-screen moving pattern)? Answer locked '\n"
            "    'or unlocked.', images=[shot])\n"
            "# If locked / screensaver:\n"
            "await ud.press_key('esc')                    # wake the display\n"
            "shot = _png(await ud.get_screenshot())\n"
            "ready = await query_llm('Is a focused macOS password field visible '\n"
            "    'and ready for input? Answer yes or no.', images=[shot])\n"
            "# Only once the field is confirmed present and focused:\n"
            "await ud.type_text('${MACOS_USER_DESKTOP_PASSWORD}')  # placeholder; resolved on-device\n"
            "await ud.press_enter()\n"
            "shot = _png(await ud.get_screenshot())\n"
            "done = await query_llm('Is this an unlocked desktop now (no lock '\n"
            "    'screen)? Answer yes or no, then briefly describe it.', images=[shot])\n"
            "```\n"
            "**Confirm before you type.**  Only `type_text` the password after a "
            "`query_llm(images=...)` check confirms the macOS password field is "
            "on screen and focused.  Never type it blind into a screensaver or a "
            "focused app -- that could mis-send the secret or surface it on "
            "screen.\n"
            "**Never** read, print, log, or echo the password value, and never "
            "ask the user to paste their password into chat -- only ever reference "
            "`${MACOS_USER_DESKTOP_PASSWORD}`.  If the placeholder stays literal "
            "or the field never accepts it, the secret is probably unset: tell the "
            "user to save it in the Console ('Save User Password').\n\n"
            "**Reporting.**  If you are asked what's on the screen while it's the "
            "screensaver, say the Mac is locked and offer to unlock it (or just "
            "unlock it if they already asked you to look) -- do not describe the "
            "screensaver pattern as content.  **Fail honestly:** if waking never "
            "reveals a login field, or the password is not accepted, report that "
            "the Mac is locked and you could not drive the login screen, and ask "
            "them to unlock it locally.  Do not retry endlessly or invent a "
            "description of a screen you cannot see.",
        )

        parts.append(
            "### Viewing Computer State\n\n"
            "`get_screenshot()` returns a PIL Image.  `display()` renders it as "
            "rich output for the **user** to see.\n\n"
            "```python\n"
            "# Show the user a screenshot:\n"
            "display(await primitives.computer.desktop.get_screenshot())\n"
            "```\n\n"
            "**Observation space.**  Screenshots from native sessions (desktop "
            "and visible web) may be downscaled to a model-aware observation "
            "space before being returned.  `act()` uses the same observation "
            "space internally and scales coordinates back up before dispatching "
            "to xdotool.  If you call low-level `click(x, y)`, the coordinates "
            "must match the pixel space of the screenshot returned by "
            "`get_screenshot()` on that same session.\n\n"
            "**To interpret a screen, send it to the LLM with "
            "`query_llm(prompt, images=[...])`**, which accepts a local path or "
            "PNG bytes and returns text.  `display()` and `query_llm(images=...)` "
            "both fit images to observation space automatically:\n"
            "```python\n"
            "import io\n"
            "img = await primitives.computer.desktop.get_screenshot()\n"
            "buf = io.BytesIO(); img.save(buf, 'PNG')\n"
            "answer = await query_llm('Describe what is on this screen.', images=[buf.getvalue()])\n"
            "```\n"
            "Do not relay a weaker side model's caption, and do not call "
            "`observe()`/`act()` just to get a description -- ask the vision "
            "model directly with `query_llm(images=...)`.  Take the OS from "
            "`primitives.computer.user_desktop.list_linked()[...].os`, never "
            "from what a model infers off the pixels.\n\n"
            "**Coordinate spaces are NOT interchangeable.**\n\n"
            "- **Desktop** and **visible web** (web-vm): screenshots capture "
            "the full physical display in observation space (including browser "
            "chrome, address bar, taskbar, window decorations).  Both share the "
            "same coordinate system.\n"
            "- **Headless web** (`visible=False`): screenshots capture only the "
            "page viewport; coordinates are viewport-relative.\n\n"
            "The same element appears at different pixel coordinates in each "
            "space.\n\n"
            "- To `session.click(x, y)` on a **headless** web session, read "
            "coordinates from `session.get_screenshot()` -- never from "
            "`primitives.computer.desktop.get_screenshot()`.\n"
            "- To `primitives.computer.desktop.click(x, y)` or click in a "
            "**visible** web session, read coordinates from the desktop or "
            "visible-session screenshot (same display space).\n\n"
            "Mixing coordinate spaces causes systematic misclicks.",
        )

        parts.append(
            "### Progress Notifications for Computer Actions\n\n"
            "Notify once per **logical task**, not per API call.  A task like "
            '"search Google for X" is one notification at the start and one at '
            "completion -- the sub-steps (open browser, navigate, type query, "
            "press Enter) are implementation details the user does not need to "
            "hear individually.\n\n"
            "Reserve intermediate notifications for genuinely long workflows "
            "that span multiple unrelated sites or take more than ~30 seconds "
            "(e.g., comparing prices across five stores).  For a single-site "
            "interaction that completes in a few seconds, one kickoff + one "
            "completion is sufficient.",
        )

        parts.append(
            "### Latency: Act and Observe Concurrently\n\n"
            "Computer actions are the single biggest latency bottleneck — "
            "especially during interactive sessions where the user is waiting "
            "in real time.  **Never** follow a sequential observe → act → "
            "observe pattern (three separate round trips).  Instead:\n\n"
            "1. **Act immediately** based on known or assumed state.  If the "
            "user says \"open Chrome\", just call `act('Open Chrome')` — do "
            "not take a screenshot first to confirm the desktop is visible.\n"
            "2. **Observe after acting** to verify the outcome.  If the state "
            "is not what you expected, course-correct with a follow-up action.\n"
            "3. **Combine observe + act in one turn** when possible.  If you "
            "need both a screenshot and an action, issue them concurrently "
            "rather than waiting for the screenshot before deciding to act.\n\n"
            "The principle: **assume the likely state and act on it; verify "
            "and correct afterwards.**  One optimistic action + one "
            "verification is almost always faster than observe → plan → act → "
            "verify, and the cost of an occasional correction is far less "
            "than the cost of an extra round trip on every single interaction.\n\n"
            "**Multi-step automated workflows are different.**  The above "
            "applies to individual interactive actions where latency matters "
            "(user is watching).  For multi-step automated work — loops, "
            "sequential data extraction, form-filling pipelines — work "
            "incrementally: execute one iteration, verify the result, then "
            "proceed.  The cost of one wrong action is small; the cost of "
            "repeating it in an unverified loop is not.",
        )

        if self._allowed_methods is not None:
            filtered_docs = build_filtered_method_docs(
                self._allowed_methods,
                self.NAMESPACE,
            )
            if filtered_docs:
                parts.append(filtered_docs)
        else:
            registry_ctx = get_registry().computer_prompt_context()
            if registry_ctx:
                parts.append(registry_ctx)

            from unify.actor.prompt_examples import get_computer_examples

            examples = get_computer_examples()
            if examples:
                parts.append(f"### Computer Examples\n\n{examples}")

        return "\n\n".join(p for p in parts if p and p.strip())

    async def capture_state(self) -> Dict[str, Any]:
        """Captures visual computer state from the desktop session."""
        try:
            session = await self._computer_primitives.backend.get_session("desktop")
            screenshot = await session.get_screenshot()
            url = await session.get_current_url()
            return {
                "type": "visual",
                "screenshot": screenshot,
                "url": url,
            }
        except Exception as e:
            return {
                "type": "visual",
                "error": str(e),
            }
