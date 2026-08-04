#!/usr/bin/env python3
"""
Standalone runner script for executing functions in isolated virtual environments.

This script is designed to be copied into custom venvs and executed as a subprocess.
It has NO dependencies on the unity package - it's completely standalone.

Communication Protocol (Bidirectional JSON-RPC):

Initial request from main process:
    {"type": "execute", "implementation": str, "call_kwargs": dict, "is_async": bool}

During execution, subprocess can send RPC requests:
    {"type": "rpc_call", "id": str, "path": str, "kwargs": dict}

Main process responds with:
    {"type": "rpc_result", "id": str, "result": Any}
    {"type": "rpc_error", "id": str, "error": str}
    {"type": "rpc_interrupt", "id": str, "reason": str}

The main process can also push a control directive at any time, outside the
request/response rhythm. A directive is not a reply: it is read by the
checkpoint shims that parent-instrumented source calls between dispatches.

    {"type": "control", "action": "interrupt",
     "reason": str, "functions": [str], "stop": bool}

Final response from subprocess:
    {"type": "complete", "result": Any, "error": str|null, "stdout": str, "stderr": str}

An interrupted run's completion additionally carries {"interrupted": str}, so
the parent can tell an unwound attempt from a genuine failure.

A single daemon thread owns stdin for the process lifetime and routes each
message to whoever is waiting on it: RPC replies to their blocked callers,
control directives into checkpoint-visible state, and runner commands
(execute / get_state / shutdown) to the main thread. Function execution
happens on the main thread.
"""

import asyncio
import io
import json
import os
import signal
import sys
import threading
import traceback
import types
import uuid
from contextlib import redirect_stderr, redirect_stdout
from queue import Queue
from typing import Any, Dict

# Defense-in-depth: strip any raw provider OAuth token that may have leaked into
# this sandbox process via inherited environment. Connected-provider REST is
# reached only through the trusted localhost proxy (see unify.provider_proxy),
# never with a raw token held in the sandbox.
for _leaked_token in (
    "MICROSOFT_ACCESS_TOKEN",
    "MICROSOFT_REFRESH_TOKEN",
    "GOOGLE_ACCESS_TOKEN",
    "GOOGLE_REFRESH_TOKEN",
):
    os.environ.pop(_leaked_token, None)

# ────────────────────────────────────────────────────────────────────────────
# Signal Handling for Graceful Shutdown
# ────────────────────────────────────────────────────────────────────────────


def _cleanup_multiprocessing_children() -> None:
    """Terminate all multiprocessing child processes."""
    try:
        import multiprocessing

        # Get all active children and terminate them
        for child in multiprocessing.active_children():
            try:
                child.terminate()
                child.join(timeout=1.0)
                if child.is_alive():
                    child.kill()
            except Exception:
                pass
    except ImportError:
        pass


def _sigterm_handler(signum: int, frame: Any) -> None:
    """Handle SIGTERM signal for graceful shutdown."""
    _cleanup_multiprocessing_children()
    # Exit gracefully
    sys.exit(0)


def _setup_signal_handlers() -> None:
    """Install signal handlers for graceful shutdown."""
    # Only set up on Unix-like systems
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, _sigterm_handler)


# ────────────────────────────────────────────────────────────────────────────
# RPC Communication Layer
# ────────────────────────────────────────────────────────────────────────────

# Global state for RPC communication
_rpc_responses: Dict[str, Queue] = {}
_rpc_lock = threading.Lock()
_stdout_lock = threading.Lock()

#: Messages that drive the runner itself (execute / get_state / shutdown),
#: queued by the stdin reader for main()/main_server() to consume. None is
#: the EOF sentinel.
_main_msgs: Queue = Queue()

#: The pending interrupt directive, written only by the stdin reader thread
#: and read by the checkpoint shims. Keeping it as plain module state is the
#: point: a checkpoint costs one attribute read, not an RPC round trip.
_interrupt_reason: str | None = None
_interrupt_functions: frozenset = frozenset()
_interrupt_stop: bool = False


class ControlledInterruption(Exception):
    """Raised inside the run when the parent interrupts it.

    The parent holds a correction for this block and wants the attempt to
    unwind so it can re-send patched source. Calls the attempt already
    completed replay from the parent's cache on the re-run, so unwinding here
    does not repeat their effects. Raised either by a blocked RPC call site
    receiving ``rpc_interrupt``, or by an instrumented ``_int`` checkpoint
    seeing a control directive.
    """


def send_message(msg: dict) -> None:
    """Send a JSON message to stdout (to main process)."""
    with _stdout_lock:
        # Write to the original stdout (before any capture)
        sys.__stdout__.write(json.dumps(msg) + "\n")
        sys.__stdout__.flush()


def _apply_control(msg: dict) -> None:
    """Record a control directive where the checkpoints will see it."""
    global _interrupt_reason, _interrupt_functions, _interrupt_stop
    if msg.get("action") == "interrupt":
        _interrupt_functions = frozenset(msg.get("functions") or ())
        _interrupt_stop = bool(msg.get("stop"))
        _interrupt_reason = str(msg.get("reason") or "steered")


def _clear_interrupt() -> None:
    """Forget a consumed interrupt so the next (patched) run starts clean."""
    global _interrupt_reason, _interrupt_functions, _interrupt_stop
    _interrupt_reason = None
    _interrupt_functions = frozenset()
    _interrupt_stop = False


def _fail_pending_rpc(error: str) -> None:
    """Unblock every caller waiting on an RPC reply that can no longer come."""
    with _rpc_lock:
        for response_queue in _rpc_responses.values():
            response_queue.put({"type": "rpc_error", "error": error})


def _stdin_reader_loop() -> None:
    """Route every stdin message to whoever is waiting on it.

    One blocking reader owns stdin for the process lifetime, which is what
    lets control directives arrive outside the request/response rhythm: a
    select loop over buffered readline cannot, because a directive glued to
    an RPC reply sits invisible in the TextIO buffer where select never
    fires again.

    Reads the raw descriptor rather than the buffered text wrapper: a
    blocking ``readline`` holds the wrapper's lock, and a fork()ed
    multiprocessing child that inherits the held lock deadlocks in its own
    bootstrap when it closes ``sys.stdin``. ``os.read`` holds no Python-level
    lock while it waits.
    """
    fd = sys.__stdin__.fileno()
    buffer = b""
    while True:
        try:
            chunk = os.read(fd, 65536)
        except OSError:
            chunk = b""
        if not chunk:
            _fail_pending_rpc("stdin closed")
            _main_msgs.put(None)
            return
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            if not line.strip():
                continue
            try:
                msg = json.loads(line.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                _main_msgs.put({"type": "_invalid_json", "error": str(e)})
                continue
            msg_type = msg.get("type")
            if msg_type in ("rpc_result", "rpc_error", "rpc_interrupt"):
                dispatch_rpc_response(msg)
            elif msg_type == "control":
                _apply_control(msg)
            else:
                _main_msgs.put(msg)


def _start_stdin_reader() -> None:
    threading.Thread(target=_stdin_reader_loop, daemon=True).start()


def rpc_call_sync(path: str, kwargs: dict) -> Any:
    """Make a synchronous RPC call to the main process."""
    request_id = uuid.uuid4().hex

    # Create a queue for this request's response
    response_queue: Queue = Queue()
    with _rpc_lock:
        _rpc_responses[request_id] = response_queue

    try:
        # Send RPC request
        send_message(
            {
                "type": "rpc_call",
                "id": request_id,
                "path": path,
                "kwargs": kwargs,
            },
        )

        # Wait for response
        response = response_queue.get(timeout=300)  # 5 minute timeout

        if response.get("type") == "rpc_interrupt":
            raise ControlledInterruption(response.get("reason") or "interrupted")
        if response.get("type") == "rpc_error":
            raise RuntimeError(f"RPC error: {response.get('error')}")

        return response.get("result")
    finally:
        with _rpc_lock:
            _rpc_responses.pop(request_id, None)


async def rpc_call_async(path: str, kwargs: dict) -> Any:
    """Make an async RPC call to the main process."""
    # Run sync RPC in executor to avoid blocking event loop
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, rpc_call_sync, path, kwargs)


def dispatch_rpc_response(msg: dict) -> None:
    """Dispatch an RPC response to the waiting caller."""
    request_id = msg.get("id")
    with _rpc_lock:
        if request_id in _rpc_responses:
            _rpc_responses[request_id].put(msg)


# ────────────────────────────────────────────────────────────────────────────
# Proxy Classes for Primitives
# ────────────────────────────────────────────────────────────────────────────


class ManagerProxy:
    """Proxy for a state manager (e.g., contacts, tasks)."""

    def __init__(self, manager_name: str, is_async: bool = True):
        self._manager_name = manager_name
        self._is_async = is_async

    def _make_method(self, method_name: str):
        """Create a method that makes an RPC call."""
        path = f"{self._manager_name}.{method_name}"

        if self._is_async:

            async def async_method(**kwargs):
                return await rpc_call_async(path, kwargs)

            return async_method
        else:

            def sync_method(**kwargs):
                return rpc_call_sync(path, kwargs)

            return sync_method

    def __getattr__(self, name: str):
        # Return a callable for any method access
        return self._make_method(name)


class PrimitivesProxy:
    """
    Proxy for the primitives object.

    Provides access to all state manager methods via RPC.
    Usage: await primitives.contacts.ask(question="...")
    """

    def __init__(self, is_async: bool = True):
        self._is_async = is_async
        self._managers: Dict[str, ManagerProxy] = {}

    def __getattr__(self, name: str) -> ManagerProxy:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._managers:
            self._managers[name] = ManagerProxy(name, self._is_async)
        return self._managers[name]


class ComputerPrimitivesProxy:
    """
    Proxy for the computer_primitives object.

    Provides access to web/desktop control methods via RPC.
    Usage: await computer_primitives.click(selector="...")
    """

    def __init__(self, is_async: bool = True):
        self._is_async = is_async

    def _make_method(self, method_name: str):
        """Create a method that makes an RPC call."""
        path = f"computer.{method_name}"

        if self._is_async:

            async def async_method(**kwargs):
                return await rpc_call_async(path, kwargs)

            return async_method
        else:

            def sync_method(**kwargs):
                return rpc_call_sync(path, kwargs)

            return sync_method

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return self._make_method(name)


def _response_format_for_rpc(response_format: Any) -> tuple[Any, Any]:
    if response_format is None:
        return None, None

    try:
        from pydantic import BaseModel

        if isinstance(response_format, type) and issubclass(response_format, BaseModel):
            return (
                {
                    "type": "json_schema",
                    "json_schema": {
                        "name": response_format.__name__,
                        "schema": response_format.model_json_schema(),
                        "strict": True,
                    },
                },
                response_format,
            )
    except ImportError:
        pass

    return response_format, None


async def query_llm(
    prompt: str,
    *,
    system: str = None,
    response_format: Any = None,
    model: str = None,
    origin: str = "CodeActActor.query_llm",
    temperature: float = 0.0,
    images: list[str | bytes] | None = None,
    **generate_kwargs: Any,
) -> Any:
    """Proxy an LLM query to the parent Unity process."""

    rpc_response_format, response_model = _response_format_for_rpc(response_format)
    rpc_images = _images_for_rpc(images)
    result = await rpc_call_async(
        "runtime.query_llm",
        {
            "prompt": prompt,
            "system": system,
            "response_format": rpc_response_format,
            "model": model,
            "origin": origin,
            "temperature": temperature,
            "images": rpc_images,
            **generate_kwargs,
        },
    )
    if response_model is not None:
        return response_model.model_validate(result)
    return result


def _bytes_to_data_url(image_bytes: bytes) -> str:
    """Encode image bytes as a data URL without importing unify."""

    import base64

    head = image_bytes[:10]
    if head.startswith(b"\xff\xd8"):
        mime = "image/jpeg"
    elif head.startswith(b"\x89PNG\r\n\x1a\n"):
        mime = "image/png"
    else:
        mime = "application/octet-stream"
    b64_data = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime};base64,{b64_data}"


def _images_for_rpc(images: list[str | bytes] | None) -> list[str] | None:
    """Normalize image payloads so RPC kwargs stay JSON-serializable."""

    if not images:
        return None

    rpc_images: list[str] = []
    for image in images:
        if isinstance(image, bytes):
            rpc_images.append(_bytes_to_data_url(image))
        else:
            rpc_images.append(image)
    return rpc_images


def list_llms(provider: str = None) -> list[str]:
    """Return supported UniLLM endpoint strings from the parent Unity process."""
    return rpc_call_sync("runtime.list_llms", {"provider": provider})


def get_oauth_access_token(provider: str, *, min_ttl_seconds: int = 300) -> str:
    """
    Return a current OAuth access token for a refresh-token backed provider.

    Custom virtual environments run in a child process whose environment can
    be older than the parent Unity worker. This helper calls the parent process
    over JSON-RPC so rotating OAuth access tokens are read from the current
    assistant secret state instead of the child process's inherited env.

    Examples
    --------
    ``token = get_oauth_access_token("microsoft")``
    ``token = get_oauth_access_token("google")``
    """
    return rpc_call_sync(
        "runtime.get_oauth_access_token",
        {"provider": provider, "min_ttl_seconds": min_ttl_seconds},
    )


# ────────────────────────────────────────────────────────────────────────────
# Steering Checkpoint Shims
# ────────────────────────────────────────────────────────────────────────────
# The parent instruments the source it ships when a steering session is in
# flight, so a loop that makes no primitive call still runs a probe every
# iteration. In-process those probes consult the session; here they consult
# the control state the stdin reader maintains, which costs an attribute read
# when nothing is pending — the reason a directive is pushed down the channel
# rather than fetched with a per-checkpoint round trip.


class _ChildRuntime:
    """Receives the position probes instrumented source emits.

    Position feeds the cache key in-process; out-of-process the parent keys
    dispatches by occurrence alone, so these only need to exist and be free.
    """

    def push_path_context(self, context_id: str) -> None:
        pass

    def pop_path_context(self) -> None:
        pass

    def start_loop_context(self, loop_id: str) -> None:
        pass

    def increment_loop_iteration(self, loop_id: str) -> None:
        pass

    def end_loop_context(self, loop_id: str) -> None:
        pass


async def _cp(label: str = "") -> None:
    """Cooperative checkpoint for control that applies to the whole block.

    Pause is not propagated to subprocesses: the parent already holds RPC
    replies while paused, and nothing in the runtime drives a pause between
    dispatches.
    """
    if _interrupt_reason is not None and _interrupt_stop:
        raise ControlledInterruption(_interrupt_reason)


async def _int(func_name: str) -> None:
    """Raise if the parent's pending correction targets *func_name*.

    A stop directive targets whatever is running, so it fires at every
    checkpoint regardless of the enclosing function.
    """
    if _interrupt_reason is not None and (
        _interrupt_stop or func_name in _interrupt_functions
    ):
        raise ControlledInterruption(_interrupt_reason)


def _int_s(func_name: str) -> None:
    """The interrupt probe for synchronous functions.

    Raising needs no await, and the stdin reader keeps the directive state
    fresh from its own thread — so unlike in-process, a sync loop here is
    interruptible mid-run.
    """
    if _interrupt_reason is not None and (
        _interrupt_stop or func_name in _interrupt_functions
    ):
        raise ControlledInterruption(_interrupt_reason)


async def _around_cp(label: str, awaitable: Any) -> Any:
    """Bracket one awaited dispatch, mirroring the in-process probe."""
    await _cp(f"Before: {label}")
    try:
        return await awaitable
    finally:
        await _cp(f"After: {label}")


# ────────────────────────────────────────────────────────────────────────────
# Execution Environment
# ────────────────────────────────────────────────────────────────────────────


def create_safe_globals(is_async: bool = True):
    """Create a sandboxed globals dict for function execution."""
    import collections
    import datetime
    import functools
    import re
    import statistics
    import typing

    # Safe builtins - excluding dangerous functions
    safe_builtins = {}
    builtins_dict = (
        __builtins__ if isinstance(__builtins__, dict) else vars(__builtins__)
    )
    for k in [
        "print",
        "len",
        "str",
        "int",
        "float",
        "bool",
        "list",
        "dict",
        "set",
        "tuple",
        "range",
        "type",
        "object",
        "bytes",
        "frozenset",
        "isinstance",
        "hasattr",
        "getattr",
        "setattr",
        "callable",
        "dir",
        "vars",
        "iter",
        "next",
        "filter",
        "map",
        "reversed",
        "enumerate",
        "zip",
        "any",
        "all",
        "sum",
        "min",
        "max",
        "abs",
        "round",
        "pow",
        "divmod",
        "sorted",
        "format",
        "chr",
        "ord",
        "Exception",
        "NotImplementedError",
        "ValueError",
        "TypeError",
        "KeyError",
        "IndexError",
        "AttributeError",
        "RuntimeError",
        "StopIteration",
        "AssertionError",
        "NameError",
        "ImportError",
        "ModuleNotFoundError",
        "FileNotFoundError",
        "OSError",
        "IOError",
        "EOFError",
        "ZeroDivisionError",
        "OverflowError",
        "MemoryError",
        "RecursionError",
        "super",
        "property",
        "classmethod",
        "staticmethod",
        "__build_class__",
        "__name__",
        "__import__",
    ]:
        if k in builtins_dict:
            safe_builtins[k] = builtins_dict[k]

    globals_dict = {
        "__builtins__": safe_builtins,
        # Standard library modules
        "asyncio": asyncio,
        "re": re,
        "json": json,
        "datetime": datetime,
        "collections": collections,
        "statistics": statistics,
        "functools": functools,
        # Typing module and common types
        "typing": typing,
        "Any": typing.Any,
        "Callable": typing.Callable,
        "Dict": typing.Dict,
        "List": typing.List,
        "Optional": typing.Optional,
        "Tuple": typing.Tuple,
        "Set": typing.Set,
        "Union": typing.Union,
        "Literal": typing.Literal,
        # Primitives proxy (computer and actor accessible via primitives.computer.* etc.)
        "primitives": PrimitivesProxy(is_async=is_async),
        "query_llm": query_llm,
        "list_llms": list_llms,
        "get_oauth_access_token": get_oauth_access_token,
        # Steering probes: parent-instrumented source calls these; inert
        # until a control directive arrives.
        "_cp": _cp,
        "_int": _int,
        "_int_s": _int_s,
        "_around_cp": _around_cp,
        "runtime": _ChildRuntime(),
    }

    # Try to add pydantic if available in this venv
    try:
        import pydantic
        from pydantic import BaseModel, Field

        globals_dict.update(
            {
                "pydantic": pydantic,
                "BaseModel": BaseModel,
                "Field": Field,
            },
        )
    except ImportError:
        pass

    # Register the globals as a proper module in sys.modules
    mod_name = f"__sandbox_{uuid.uuid4()}__"
    mod = types.ModuleType(mod_name)
    mod.__dict__.update(globals_dict)
    sys.modules[mod_name] = mod
    mod.__dict__["__name__"] = mod_name
    return mod.__dict__


def execute_sync(implementation: str, call_kwargs: dict) -> dict:
    """Execute a synchronous function (one-shot mode with fresh globals)."""
    globals_dict = create_safe_globals(is_async=False)
    return execute_sync_in_globals(implementation, call_kwargs, globals_dict)


def execute_sync_in_globals(
    implementation: str,
    call_kwargs: dict,
    globals_dict: dict,
) -> dict:
    """Execute a synchronous function in the provided globals dict."""
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    result = None
    error = None
    interrupted = None

    try:
        # Extract function name from implementation BEFORE exec
        func_name = _extract_function_name(implementation)
        if not func_name:
            raise ValueError("No function definition found in implementation")

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exec(implementation, globals_dict)

            fn = globals_dict.get(func_name)
            if fn is None:
                raise ValueError(f"Function '{func_name}' not found after exec")

            result = fn(**call_kwargs)

    except ControlledInterruption as ci:
        # An unwind the parent asked for, not a failure: mark it so the
        # parent can discard this attempt and re-run the patched source.
        interrupted = str(ci) or "interrupted"
        error = traceback.format_exc()
    except Exception:
        error = traceback.format_exc()

    out = {
        "result": result,
        "error": error,
        "stdout": stdout_capture.getvalue(),
        "stderr": stderr_capture.getvalue(),
    }
    if interrupted is not None:
        out["interrupted"] = interrupted
    return out


async def execute_async(implementation: str, call_kwargs: dict) -> dict:
    """Execute an asynchronous function (one-shot mode with fresh globals)."""
    globals_dict = create_safe_globals(is_async=True)
    return await execute_async_in_globals(implementation, call_kwargs, globals_dict)


def _extract_function_name(implementation: str) -> str:
    """Extract the function name from an implementation string using AST.

    Raises SyntaxError if the implementation has invalid Python syntax.
    Returns empty string if no function definition is found.
    """
    import ast as _ast

    # Let SyntaxError propagate so callers see the actual parsing error
    tree = _ast.parse(implementation)
    for node in tree.body:
        if isinstance(node, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
            return node.name
    return ""


async def execute_async_in_globals(
    implementation: str,
    call_kwargs: dict,
    globals_dict: dict,
) -> dict:
    """Execute an asynchronous function in the provided globals dict."""
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    result = None
    error = None
    interrupted = None

    try:
        # Extract function name from implementation BEFORE exec
        func_name = _extract_function_name(implementation)
        if not func_name:
            raise ValueError("No function definition found in implementation")

        exec(implementation, globals_dict)

        fn = globals_dict.get(func_name)
        if fn is None:
            raise ValueError(f"Function '{func_name}' not found after exec")
        if not asyncio.iscoroutinefunction(fn):
            raise ValueError(f"Function '{func_name}' is not async")

        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            result = await fn(**call_kwargs)

    except ControlledInterruption as ci:
        # An unwind the parent asked for, not a failure: mark it so the
        # parent can discard this attempt and re-run the patched source.
        interrupted = str(ci) or "interrupted"
        error = traceback.format_exc()
    except Exception:
        error = traceback.format_exc()

    out = {
        "result": result,
        "error": error,
        "stdout": stdout_capture.getvalue(),
        "stderr": stderr_capture.getvalue(),
    }
    if interrupted is not None:
        out["interrupted"] = interrupted
    return out


def make_json_serializable(obj):
    """Convert an object to a JSON-serializable form."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [make_json_serializable(item) for item in obj]
    if isinstance(obj, dict):
        return {str(k): make_json_serializable(v) for k, v in obj.items()}
    try:
        from pydantic import BaseModel

        if isinstance(obj, BaseModel):
            return make_json_serializable(obj.model_dump())
    except ImportError:
        pass
    # For other types, convert to string
    return str(obj)


# ────────────────────────────────────────────────────────────────────────────
# State Serialization for Read-Only Mode
# ────────────────────────────────────────────────────────────────────────────


def _is_user_defined_state(key: str, value: Any, base_globals: dict) -> bool:
    """
    Check if a key-value pair represents user-defined state.

    Returns True if the variable was defined by user code (not part of the
    base execution environment).
    """
    # Skip private/dunder names
    if key.startswith("_"):
        return False

    # Skip if it exists in the base globals (it's a built-in)
    if key in base_globals:
        return False

    # Skip modules (typically imported by base globals)
    if isinstance(value, type(json)):
        return False

    # Skip proxies
    if isinstance(value, (PrimitivesProxy, ManagerProxy)):
        return False

    return True


def _serialize_value(value: Any) -> tuple[bool, Any]:
    """
    Attempt to serialize a value for state transfer.

    Returns (success, serialized_value). If success is False, the value
    cannot be serialized and should be skipped.
    """
    # Primitives are directly serializable
    if value is None or isinstance(value, (bool, int, float, str)):
        return True, {"type": "primitive", "value": value}

    # Lists and tuples
    if isinstance(value, (list, tuple)):
        items = []
        for item in value:
            success, serialized = _serialize_value(item)
            if not success:
                return False, None
            items.append(serialized)
        return True, {
            "type": "list" if isinstance(value, list) else "tuple",
            "items": items,
        }

    # Dicts
    if isinstance(value, dict):
        serialized_dict = {}
        for k, v in value.items():
            if not isinstance(k, str):
                return False, None  # Only string keys supported
            success, serialized = _serialize_value(v)
            if not success:
                return False, None
            serialized_dict[k] = serialized
        return True, {"type": "dict", "items": serialized_dict}

    # Sets and frozensets
    if isinstance(value, (set, frozenset)):
        items = []
        for item in value:
            success, serialized = _serialize_value(item)
            if not success:
                return False, None
            items.append(serialized)
        return True, {
            "type": "set" if isinstance(value, set) else "frozenset",
            "items": items,
        }

    # Bytes
    if isinstance(value, bytes):
        import base64

        return True, {"type": "bytes", "value": base64.b64encode(value).decode("ascii")}

    # Functions - store their source if possible
    if callable(value) and hasattr(value, "__code__"):
        try:
            import inspect

            source = inspect.getsource(value)
            return True, {"type": "function", "name": value.__name__, "source": source}
        except (OSError, TypeError):
            pass

    # Classes - skip for now (complex to serialize)
    # Pydantic models, custom objects - skip

    return False, None


def _deserialize_value(serialized: dict) -> Any:
    """Deserialize a value from state transfer format."""
    value_type = serialized.get("type")

    if value_type == "primitive":
        return serialized["value"]

    if value_type == "list":
        return [_deserialize_value(item) for item in serialized["items"]]

    if value_type == "tuple":
        return tuple(_deserialize_value(item) for item in serialized["items"])

    if value_type == "dict":
        return {k: _deserialize_value(v) for k, v in serialized["items"].items()}

    if value_type == "set":
        return {_deserialize_value(item) for item in serialized["items"]}

    if value_type == "frozenset":
        return frozenset(_deserialize_value(item) for item in serialized["items"])

    if value_type == "bytes":
        import base64

        return base64.b64decode(serialized["value"])

    if value_type == "function":
        # Re-execute the function definition to recreate it
        # This is a best-effort approach
        source = serialized["source"]
        name = serialized["name"]
        local_ns: Dict[str, Any] = {}
        exec(source, {}, local_ns)
        return local_ns.get(name)

    raise ValueError(f"Unknown serialized type: {value_type}")


def serialize_user_state(globals_dict: dict, base_globals: dict) -> dict:
    """
    Extract and serialize user-defined state from globals.

    Returns a dict of {name: serialized_value} for all serializable
    user-defined variables.
    """
    state = {}
    for key, value in globals_dict.items():
        if not _is_user_defined_state(key, value, base_globals):
            continue
        success, serialized = _serialize_value(value)
        if success:
            state[key] = serialized
    return state


def inject_state_into_globals(state: dict, globals_dict: dict) -> None:
    """
    Inject deserialized state into a globals dict.

    Args:
        state: Dict of {name: serialized_value} from serialize_user_state
        globals_dict: The globals dict to inject into
    """
    for name, serialized in state.items():
        try:
            globals_dict[name] = _deserialize_value(serialized)
        except Exception:
            # Skip values that fail to deserialize
            pass


def apply_env_overlay(env_overlay: dict | None) -> None:
    """Apply parent-supplied runtime env updates inside the child process.

    The venv runner can be a long-lived subprocess, so inherited environment
    variables may be older than Unity's parent runtime. The parent sends the
    localhost workspace-proxy endpoints (base URLs + nonce) before each call.
    Raw provider OAuth tokens are never overlaid; any that slipped in are
    scrubbed defensively.
    """
    if not env_overlay:
        return
    for key, value in env_overlay.items():
        if isinstance(key, str) and isinstance(value, str):
            os.environ[key] = value
    for leaked in (
        "MICROSOFT_ACCESS_TOKEN",
        "MICROSOFT_REFRESH_TOKEN",
        "GOOGLE_ACCESS_TOKEN",
        "GOOGLE_REFRESH_TOKEN",
    ):
        os.environ.pop(leaked, None)


# ────────────────────────────────────────────────────────────────────────────
# Main Entry Point
# ────────────────────────────────────────────────────────────────────────────


def _run_request(
    implementation: str,
    call_kwargs: dict,
    is_async: bool,
    globals_dict: dict,
) -> dict:
    """Run one execute request on the main thread.

    The stdin reader thread keeps routing RPC replies and control directives
    while this blocks, so calls into the parent resolve and interrupts land
    mid-run.
    """
    _clear_interrupt()
    try:
        if is_async:
            return asyncio.run(
                execute_async_in_globals(implementation, call_kwargs, globals_dict),
            )
        return execute_sync_in_globals(implementation, call_kwargs, globals_dict)
    except Exception:
        return {
            "result": None,
            "error": traceback.format_exc(),
            "stdout": "",
            "stderr": "",
        }


def main():
    """Main entry point for one-shot runner mode."""
    # Set up signal handlers for graceful shutdown
    _setup_signal_handlers()
    _start_stdin_reader()

    input_data = _main_msgs.get()
    if input_data is None:
        send_message(
            {
                "type": "complete",
                "result": None,
                "error": "No input received",
                "stdout": "",
                "stderr": "",
            },
        )
        sys.exit(1)
    if input_data.get("type") == "_invalid_json":
        send_message(
            {
                "type": "complete",
                "result": None,
                "error": f"Invalid JSON input: {input_data.get('error')}",
                "stdout": "",
                "stderr": "",
            },
        )
        sys.exit(1)

    msg_type = input_data.get("type", "execute")
    if msg_type != "execute":
        send_message(
            {
                "type": "complete",
                "result": None,
                "error": f"Expected 'execute' message, got '{msg_type}'",
                "stdout": "",
                "stderr": "",
            },
        )
        sys.exit(1)

    implementation = input_data.get("implementation", "")
    call_kwargs = input_data.get("call_kwargs", {})
    is_async = input_data.get("is_async", False)
    initial_state = input_data.get("initial_state")
    apply_env_overlay(input_data.get("env_overlay"))

    globals_dict = create_safe_globals(is_async=is_async)
    if initial_state:
        inject_state_into_globals(initial_state, globals_dict)

    result = _run_request(implementation, call_kwargs, is_async, globals_dict)

    # Make result JSON-serializable
    result["result"] = make_json_serializable(result["result"])

    # Send completion message
    send_message(
        {
            "type": "complete",
            **result,
        },
    )


# ────────────────────────────────────────────────────────────────────────────
# Persistent Server Mode
# ────────────────────────────────────────────────────────────────────────────


def main_server():
    """
    Persistent server mode entry point.

    Maintains state across multiple function calls by keeping a persistent
    globals dict. The server loops waiting for execute requests until it
    receives a shutdown message or stdin is closed.

    Protocol:
        Input messages:
            {"type": "execute", "implementation": str, "call_kwargs": dict, "is_async": bool}
            {"type": "get_state"}
            {"type": "shutdown"}
            {"type": "control", "action": "interrupt", ...}  (mid-execute)

        Output messages:
            {"type": "complete", "result": Any, "error": str|null, "stdout": str, "stderr": str}
            {"type": "state", "state": dict}
            {"type": "ack"}  (response to shutdown)
    """
    _setup_signal_handlers()
    _start_stdin_reader()

    # Send ready signal so parent knows we're listening
    send_message({"type": "ready"})

    # Persistent globals - survives across calls
    globals_dict = create_safe_globals(is_async=True)
    # Keep a reference to the base globals for state serialization
    base_globals = create_safe_globals(is_async=True)

    while True:
        input_data = _main_msgs.get()
        if input_data is None:
            # stdin closed, exit gracefully
            break
        if input_data.get("type") == "_invalid_json":
            send_message(
                {
                    "type": "complete",
                    "result": None,
                    "error": f"Invalid JSON input: {input_data.get('error')}",
                    "stdout": "",
                    "stderr": "",
                },
            )
            continue

        msg_type = input_data.get("type", "execute")

        if msg_type == "shutdown":
            send_message({"type": "ack"})
            _cleanup_multiprocessing_children()
            break

        if msg_type == "get_state":
            # Serialize and return current user-defined state
            try:
                state = serialize_user_state(globals_dict, base_globals)
                send_message({"type": "state", "state": state})
            except Exception as e:
                send_message({"type": "state", "state": {}, "error": str(e)})
            continue

        if msg_type != "execute":
            send_message(
                {
                    "type": "complete",
                    "result": None,
                    "error": f"Expected 'execute', 'get_state', or 'shutdown' message, got '{msg_type}'",
                    "stdout": "",
                    "stderr": "",
                },
            )
            continue

        implementation = input_data.get("implementation", "")
        call_kwargs = input_data.get("call_kwargs", {})
        is_async = input_data.get("is_async", True)
        apply_env_overlay(input_data.get("env_overlay"))

        result = _run_request(implementation, call_kwargs, is_async, globals_dict)

        # Make result JSON-serializable
        result["result"] = make_json_serializable(result["result"])

        # Send completion message
        send_message(
            {
                "type": "complete",
                **result,
            },
        )


if __name__ == "__main__":
    # Check for server mode flag
    if len(sys.argv) > 1 and sys.argv[1] == "--server":
        main_server()
    else:
        main()
