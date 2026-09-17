"""
unify/logger.py
===============

Unity's runtime logging and OpenTelemetry tracing configuration.

File-based logging:
    When UNIFY_LOG_DIR is set (via env var or configure_log_dir()),
    Unity's LOGGER output is written to two files:
      - {UNIFY_LOG_DIR}/unity.log           (DEBUG + INFO)
      - {UNIFY_LOG_DIR}/unity_info_only.log (INFO only)
    This captures async tool loop events, manager operations, etc.

OpenTelemetry tracing:
    When UNIFY_OTEL is enabled, manager operations and async tool loops
    create OTel spans that propagate trace context to downstream libraries.

    - UNIFY_OTEL: Master switch (default: false)
    - UNIFY_OTEL_ENDPOINT: OTLP endpoint for trace export (optional)
    - UNIFY_OTEL_LOG_DIR: Directory for file-based span export (optional)

    Unity acts as the root TracerProvider when enabled. Child libraries
    (unillm, unify) will detect the existing provider and create child spans.

File-based span export:
    When UNIFY_OTEL_LOG_DIR is set, spans are written to JSONL files keyed
    by trace_id: {UNIFY_OTEL_LOG_DIR}/{trace_id}.jsonl

    This enables full-stack trace correlation across processes. Orchestra
    (running in a separate FastAPI process) receives the traceparent header
    from Unify HTTP calls and can write its spans to the same directory.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from unify.settings import SETTINGS

# ─────────────────────────────────────────────────────────────────────────────
# Cgroup Memory Reporting
#
# Reads container memory from cgroup v1/v2 pseudo-files and formats a compact
# tag like ``[4821/8192 MiB]`` for inclusion in every log line.  Cached with
# a short TTL to avoid per-record syscall overhead.  Returns an empty string
# when cgroup files are absent (local dev, macOS, tests).
# ─────────────────────────────────────────────────────────────────────────────

_CGROUP_MEM_FILE: str | None = None  # path to current-usage pseudo-file
_CGROUP_MEM_MAX: int | None = None  # limit in bytes (None = no limit)
_CGROUP_CACHE: tuple[float, int] = (0.0, 0)  # (monotonic_ts, usage_bytes)
_CGROUP_CACHE_TTL = 1.0  # seconds


def _init_cgroup_paths() -> None:
    """Detect cgroup v1/v2 memory files (called once, lazily)."""
    global _CGROUP_MEM_FILE, _CGROUP_MEM_MAX
    if _CGROUP_MEM_FILE is not None:
        return

    # cgroup v2
    if os.path.isfile("/sys/fs/cgroup/memory.current"):
        _CGROUP_MEM_FILE = "/sys/fs/cgroup/memory.current"
        try:
            with open("/sys/fs/cgroup/memory.max") as f:
                val = f.read().strip()
                _CGROUP_MEM_MAX = None if val == "max" else int(val)
        except (OSError, ValueError):
            _CGROUP_MEM_MAX = None
    # cgroup v1
    elif os.path.isfile("/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        _CGROUP_MEM_FILE = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
                _CGROUP_MEM_MAX = int(f.read().strip())
        except (OSError, ValueError):
            _CGROUP_MEM_MAX = None
    else:
        _CGROUP_MEM_FILE = ""  # sentinel: no cgroup available


def _get_memory_tag() -> str:
    """Return e.g. ``[4821/8192 MiB]`` or ``""`` if cgroup unavailable."""
    global _CGROUP_CACHE

    _init_cgroup_paths()
    if not _CGROUP_MEM_FILE:
        return ""

    now = time.monotonic()
    cached_ts, cached_bytes = _CGROUP_CACHE
    if now - cached_ts < _CGROUP_CACHE_TTL:
        current = cached_bytes
    else:
        try:
            with open(_CGROUP_MEM_FILE) as f:
                current = int(f.read().strip())
            _CGROUP_CACHE = (now, current)
        except (OSError, ValueError):
            return ""

    mib = current >> 20  # // 1048576
    if _CGROUP_MEM_MAX:
        max_mib = _CGROUP_MEM_MAX >> 20
        pct = current * 100 // _CGROUP_MEM_MAX
        return f"[{mib}/{max_mib} MiB ({pct}%)]"
    return f"[{mib} MiB]"


# ─────────────────────────────────────────────────────────────────────────────
# Logger Instance
# ─────────────────────────────────────────────────────────────────────────────

LOGGER = logging.getLogger("unify")

# Unique identifier for this process lifetime (used for log correlation)
SESSION_ID = datetime.now(timezone.utc).isoformat()

# File handler state (managed by configure_log_dir)
_FILE_HANDLER: Optional[logging.FileHandler] = None
_INFO_FILE_HANDLER: Optional[logging.FileHandler] = None
_LOG_DIR: Optional[Path] = None

# ─────────────────────────────────────────────────────────────────────────────
# Console (Terminal) Logging
#
# This is the single authority for all Unity log output.  No other module
# should call logging.basicConfig(), add handlers, or filter the root logger.
# ─────────────────────────────────────────────────────────────────────────────

# Prevent unity records from propagating to the root logger.  This eliminates
# duplicate output from any root-level handler (e.g. logging.basicConfig())
# that third-party code may install.
LOGGER.propagate = False


from unify.syntax_highlight import highlight_code_blocks  # noqa: E402


class _MillisFormatter(logging.Formatter):
    """Formatter that prepends ``HH:MM:SS.mmm`` to each log line.

    Messages that don't already start with a non-ASCII character (i.e. an
    emoji icon from the hierarchical logger) are auto-prefixed with ``⬥``
    so every terminal line has a consistent visual anchor.

    When *stream* is a TTY, markdown-fenced code blocks are syntax-
    highlighted via Pygments.
    """

    _DEFAULT_ICON = "⬥"

    def __init__(self, *args, stream=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_tty = getattr(stream, "isatty", lambda: False)()

    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone()
        ts = dt.strftime("%H:%M:%S") + f".{int(dt.microsecond / 1000):03d}"
        msg = record.getMessage()
        if msg and ord(msg[0]) < 128:
            msg = f"{self._DEFAULT_ICON} {msg}"
        if self._is_tty:
            msg = highlight_code_blocks(msg)
        return f"{ts} {msg}"


LOGGER.setLevel(logging.DEBUG)

if SETTINGS.UNIFY_TERMINAL_LOG:
    import sys

    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(_MillisFormatter(stream=sys.stdout))
    _handler.setLevel(getattr(logging, SETTINGS.UNIFY_TERMINAL_LOG_LEVEL, logging.INFO))

    _already_configured = any(
        isinstance(h, logging.StreamHandler) and getattr(h, "_unity_terminal", False)
        for h in LOGGER.handlers
    )

    if not _already_configured:
        _handler._unity_terminal = True  # type: ignore[attr-defined]
        LOGGER.addHandler(_handler)

# Mute noisy third-party loggers so only unify.* output reaches the terminal.
for _lib in (
    "httpx",
    "urllib3",
    "openai",
    "LiteLLM",
    "LiteLLM Proxy",
    "LiteLLM Router",
):
    logging.getLogger(_lib).setLevel(logging.WARNING)

# RapidOCR reconfigures its own logger on import (unconditionally calling
# setLevel(INFO)), so setLevel here would be clobbered.  A filter survives.
logging.getLogger("RapidOCR").addFilter(
    lambda record: record.levelno >= logging.WARNING,
)

# File-only loggers: cut propagation so nothing reaches the root/terminal
# handlers, and attach the DEBUG file handler in configure_log_dir() so the
# output still lands in unity.log.  NOT wired to unity_info_only.log — that
# file mirrors the terminal exactly (Unity INFO+ only).
#
# "py.warnings" captures Python warnings (e.g. unawaited coroutine
# RuntimeWarnings from LiteLLM's async bridge during task cancellation).
# They are harmless but noisy on the terminal; routing through the logging
# system keeps them in unity.log for debugging without cluttering stdout.
_FILE_ONLY_LOGGERS = [
    logging.getLogger(name)
    for name in (
        "livekit",
        "livekit.agents",
        "livekit.plugins",
        "PIL",
        "py.warnings",
    )
]
for _fo in _FILE_ONLY_LOGGERS:
    _fo.setLevel(logging.DEBUG)
    _fo.propagate = False

logging.captureWarnings(True)

# ─────────────────────────────────────────────────────────────────────────────
# File-based Logging Configuration
# ─────────────────────────────────────────────────────────────────────────────


class _MemoryFileFormatter(logging.Formatter):
    """File formatter that prepends cgroup memory usage to each line."""

    def format(self, record: logging.LogRecord) -> str:
        mem = _get_memory_tag()
        base = super().format(record)
        if record.name.startswith("livekit.plugins.elevenlabs"):
            elevenlabs_extra = _format_elevenlabs_extra(record)
            if elevenlabs_extra:
                base = f"{base} {elevenlabs_extra}"
        if mem:
            # Insert memory tag after the log-level field
            return f"{base} {mem}"
        return base


def _format_elevenlabs_extra(record: logging.LogRecord) -> str:
    """Render ElevenLabs provider error metadata hidden in logging ``extra``."""

    fields: dict[str, Any] = {}
    for key in ("context_id", "error"):
        value = getattr(record, key, None)
        if value not in (None, ""):
            fields[key] = value
    data = getattr(record, "data", None)
    if isinstance(data, dict):
        safe_data = {
            key: value
            for key, value in data.items()
            if key
            not in {
                "audio",
                "alignment",
                "normalizedAlignment",
                "normalized_alignment",
            }
        }
        if safe_data:
            fields["data"] = safe_data
    if not fields:
        return ""
    try:
        return json.dumps({"elevenlabs": fields}, ensure_ascii=False, default=str)
    except TypeError:
        return f"elevenlabs={fields!r}"


def _append_elevenlabs_extra(record: logging.LogRecord) -> bool:
    """Attach ElevenLabs provider metadata before any handler formats the record."""

    extra = _format_elevenlabs_extra(record)
    if extra and extra not in str(record.msg):
        record.msg = f"{record.getMessage()} {extra}"
        record.args = ()
    return True


logging.getLogger("livekit.plugins.elevenlabs").addFilter(_append_elevenlabs_extra)


def configure_log_dir(log_dir: Optional[str] = None) -> Optional[Path]:
    """Configure or reconfigure the Unity LOGGER file output directory.

    When configured, LOGGER output is written to two files:
      - {log_dir}/unity.log           (everything: Unity DEBUG+, plus
                                        third-party file-only loggers)
      - {log_dir}/unity_info_only.log (Unity INFO+ only — mirrors the
                                        terminal exactly)

    This captures async tool loop events, manager operations, hierarchical
    session logs, and any other code using LOGGER.

    Call this after setting UNIFY_LOG_DIR if the env var was set
    after this module was imported.

    Args:
        log_dir: Explicit log directory path. If None, reads from
                 UNIFY_LOG_DIR env var (or SETTINGS.UNIFY_LOG_DIR).

    Returns:
        The configured log directory Path, or None if disabled.
    """
    global _FILE_HANDLER, _INFO_FILE_HANDLER, _LOG_DIR

    # Remove existing file handlers if any
    if _FILE_HANDLER is not None:
        LOGGER.removeHandler(_FILE_HANDLER)
        for _fo in _FILE_ONLY_LOGGERS:
            _fo.removeHandler(_FILE_HANDLER)
        _FILE_HANDLER.close()
        _FILE_HANDLER = None
    if _INFO_FILE_HANDLER is not None:
        LOGGER.removeHandler(_INFO_FILE_HANDLER)
        _INFO_FILE_HANDLER.close()
        _INFO_FILE_HANDLER = None
    _LOG_DIR = None

    # Determine log directory
    if log_dir is not None:
        os.environ["UNIFY_LOG_DIR"] = log_dir
        dir_path = log_dir
    else:
        dir_path = os.environ.get("UNIFY_LOG_DIR", "").strip() or SETTINGS.UNIFY_LOG_DIR

    if not dir_path:
        return None

    try:
        log_path = Path(dir_path)
        log_path.mkdir(parents=True, exist_ok=True)

        fmt = "%(asctime)s %(levelname)7s %(message)s"

        log_file = log_path / "unity.log"
        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        handler.setFormatter(_MemoryFileFormatter(fmt))
        handler.setLevel(logging.DEBUG)
        handler._unity_file_handler = True  # type: ignore[attr-defined]
        LOGGER.addHandler(handler)
        for _fo in _FILE_ONLY_LOGGERS:
            _fo.addHandler(handler)
        _FILE_HANDLER = handler

        info_log_file = log_path / "unity_info_only.log"
        info_handler = logging.FileHandler(info_log_file, mode="a", encoding="utf-8")
        info_handler.setFormatter(logging.Formatter(fmt))
        info_handler.setLevel(logging.INFO)
        info_handler._unity_file_handler = True  # type: ignore[attr-defined]
        LOGGER.addHandler(info_handler)
        _INFO_FILE_HANDLER = info_handler

        _LOG_DIR = log_path

        LOGGER.debug(f"Unity file logging enabled: {log_file}")
        return log_path

    except Exception as e:
        # Best-effort: log to console if file logging fails
        logging.warning(f"Failed to configure Unity log directory {dir_path}: {e}")
        return None


def get_log_dir() -> Optional[Path]:
    """Get the current Unity log directory, if configured."""
    return _LOG_DIR


# Auto-configure from settings on module load (if UNIFY_LOG_DIR is set)
if SETTINGS.UNIFY_LOG_DIR:
    configure_log_dir(SETTINGS.UNIFY_LOG_DIR)
