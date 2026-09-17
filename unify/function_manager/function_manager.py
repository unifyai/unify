import ast
import asyncio
import builtins
import concurrent.futures
import dataclasses
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import inspect
import functools
import json
import os
import re
import signal
import socket
import sys
import tempfile
import logging
import threading
from pathlib import Path
from weakref import WeakSet

from secrets import token_hex
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)
from unify import db
from .shell_pool import ShellPool
from unify.db import StoreError as _UnifyRequestError
from ..common.authorship import strip_authoring_assistant_id
from ..common.log_utils import create_logs as unity_create_logs
from ..common.embed_utils import ensure_vector_column, list_private_fields
from ..common.federated_search import (
    SCORE_FIELD,
    FederatedSearchContext,
    federated_filter,
    federated_ranked_search,
)
from .activation import (
    ActivationSettings,
    activation,
    in_scope,
    merged_usage,
    rank_score,
    similarity_from_distance,
)
from ..common.builtins import builtins_project
from .builtins_catalog import BUILTINS_PRIMITIVES_CONTEXT
from ..common.tool_outcome import ToolErrorException
from .execution_env import ENVIRONMENT_MODULES, create_base_globals
from .steering import (
    DEFAULT_TOOL_NAMESPACES,
    ControlledInterruption,
    ExecutionStopped,
    MemoisedDispatch,
    active_session,
    bind_session,
    dispatch_with_steering,
    instrument,
    interrupt_directive,
    restore_session,
    run_with_steering,
)
from .dependency_analysis import (
    collect_dependencies_from_function_node,
    detect_third_party_imports,
)
from .types.function import Function
from .types.meta import FunctionsMeta
from .types.venv import VirtualEnv
from .types.verification import (
    Fixture,
    FunctionContract,
    SideEffectClass,
    StaticReviewRecord,
    VerdictKind,
    VerificationPolicy,
    VerificationRow,
    VerificationSummary,
)
from .verification.classify import (
    Classification,
    classify_source,
    effective_class,
)
from .verification.contracts import contract_from_callable, merge_contract
from .verification.fixtures import (
    FixtureRegressionError,
    coerce_fixtures,
    replay_fixtures,
)
from .verification.ledger import (
    fold_rows,
    function_trust_hash as _function_trust_hash,
)
from .verification.policy import derive_verify
from .verification.source_labels import compile_function_source
from .verification.tier0 import Tier0Checker, signature_from_source, tier0_boundary
from .settings import VerificationSettings
from .base import BaseFunctionManager
from ..common.model_to_fields import model_to_fields, with_ui_editable_forced_false
from ..file_manager.managers.local import LocalFileManager
from ..image_manager.image_manager import ImageHandle
from ..manager_registry import ManagerRegistry
from ..common.filter_utils import normalize_filter_expr
from ..common.context_registry import ContextRegistry, TableContext
from ..common.stale_reason import (
    StaleReason,
    coerce_stale_reasons,
    merge_stale_reasons,
)
from unify.function_manager.primitives.scope import (
    PrimitiveScope,
    default_runtime_scope,
)
from unify.function_manager.primitives.registry import get_registry

logger = logging.getLogger(__name__)

# One lock per venv directory so concurrent ``prepare_venv`` calls serialise.
_VENV_PREPARE_LOCKS: dict[str, asyncio.Lock] = {}

FUNCTIONS_VENVS_TABLE = "Functions/VirtualEnvs"
FUNCTIONS_COMPOSITIONAL_TABLE = "Functions/Compositional"
FUNCTIONS_PRIMITIVES_TABLE = "Functions/Primitives"
FUNCTIONS_META_TABLE = "Functions/Meta"
FUNCTIONS_VERIFICATIONS_TABLE = "Functions/Verifications"
# Ledger state the runtime maintains; hidden from catalogue reads that do not
# hand back callables (fixtures alone can run to tens of kilobytes).
_LEDGER_INTERNAL_FIELDS = ("fixtures", "ledger", "static_review", "verified_hash")

#: What a reader of the catalogue never acts on, minus the history a repairer
#: cannot work without. A repair is asked to fix a function against a verdict;
#: without the prior verdicts on the same function it cannot tell a fresh
#: objection from one it has already answered, and cannot see that its last
#: attempt is what produced the current complaint.
_REPAIR_HIDDEN_FIELDS = ("fixtures", "verified_hash")


def strip_ledger_internals(
    rows: List[Dict[str, Any]],
    *,
    keep_verdict_history: bool = False,
) -> List[Dict[str, Any]]:
    """Return ``rows`` without the ledger state a reader of the catalogue never acts on.

    ``keep_verdict_history`` retains ``ledger`` and ``static_review`` for a
    caller that is reasoning about the verdicts themselves.
    """
    hidden = set(
        _REPAIR_HIDDEN_FIELDS if keep_verdict_history else _LEDGER_INTERNAL_FIELDS,
    )
    return [
        (
            {k: v for k, v in row.items() if k not in hidden}
            if isinstance(row, dict)
            else row
        )
        for row in rows
    ]


# Sentinel: omit ``destination`` to federate; pass ``None``/``"personal"`` to scope.
_DESTINATION_UNSET = object()
FUNCTIONS_COMPOSITIONAL_DESTINATION_GUIDANCE = """destination : str | None, default None
    Where this composed function (or set of functions) lives. Only the
    personal root exists: pass ``"personal"`` or leave it ``None``."""
FUNCTIONS_VENV_DESTINATION_GUIDANCE = """destination : str | None, default None
    Where the virtual env definition lives. Only the personal root exists:
    pass ``"personal"`` or leave it ``None``."""


def _compositional_contexts() -> list[str]:
    """Every compositional functions context readable from here."""

    from unify.common.context_registry import ContextRegistry

    return [
        f"{root.strip('/')}/{FUNCTIONS_COMPOSITIONAL_TABLE}"
        for root in ContextRegistry.read_roots(
            FunctionManager,
            FUNCTIONS_COMPOSITIONAL_TABLE,
        )
    ]


def function_id_resolves(function_id: int) -> bool:
    """Whether a stored id still points at a compositional function.

    For callers holding an id and asking only about referential integrity --
    a task's stored ``entrypoint``, say. An id rather than a manager handle
    because the question is asked from stores that keep one and have no
    reason to hold a FunctionManager.

    Asked per id rather than by listing every function and testing
    membership: ``get_logs`` pages at a thousand rows, so an enumeration
    would report perfectly good ids as missing on any deployment past that,
    and callers reading this as "gone" would act on it.
    """

    for context in _compositional_contexts():
        if db.get_logs(
            context=context,
            filter=f"function_id == {int(function_id)}",
            limit=1,
        ):
            return True
    return False


def delete_functions(function_ids: "set[int] | list[int]") -> list[int]:
    """Delete compositional functions by id, returning the ids actually removed.

    For a caller that has already decided which functions should go and needs
    them gone -- a source being withdrawn clearing what its runs distilled.
    Deciding *which* is the caller's problem; this only carries it out.
    """

    if not function_ids:
        return []
    deleted: list[int] = []
    for context in _compositional_contexts():
        for function_id in function_ids:
            logs = db.get_logs(
                context=context,
                filter=f"function_id == {int(function_id)}",
                limit=1,
            )
            if not logs:
                continue
            db.delete_logs(context=context, logs=[logs[0].id])
            deleted.append(int(function_id))
    return deleted


class _LineageTrackedFunction:
    """Boundary wrapper for FunctionManager callables injected into CodeActActor sandboxes.

    This wrapper preserves hierarchical lineage across mixed execution, e.g.:

        CodeActActor.act -> execute_code -> <function> -> primitives.contacts.ask -> ...

    It is injected into the Python namespace **in place of** the raw callable so that
    inter-function calls (function A calling function B) still pass through a boundary that:
    - updates `TOOL_LOOP_LINEAGE` (ContextVar)
    - emits a concise boundary log line for terminal debugging

    Note: async functions can be awaited in a different task context than the call-site.
    ContextVar tokens are only valid in the context they were created, so this wrapper sets
    lineage around coroutine construction (call-site) and again inside the awaited coroutine
    (execution-site).
    """

    def __init__(
        self,
        wrapped_callable: Callable[..., Any],
        function_name: str,
        on_call: Optional[Callable[[], None]] = None,
    ):
        self._wrapped = wrapped_callable
        self._function_name = function_name
        # Usage-trace hook: this class is the one layer every boundary call
        # already passes through, and its __getattr__ delegation keeps proxy
        # identity intact — an OUTER wrapper broke remote-routing and venv
        # cleanup introspection, which is why the trace records here.
        self._on_call = on_call

        # Preserve introspection attributes.
        self.__name__ = function_name
        self.__doc__ = getattr(wrapped_callable, "__doc__", None)
        self.__wrapped__ = wrapped_callable

    def __getattr__(self, name: str) -> Any:
        # Preserve wrapped callable API (e.g. venv proxy state helpers).
        return getattr(self._wrapped, name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._on_call is not None:
            try:
                self._on_call()
            except Exception:  # noqa: BLE001 - metering must never break a call
                pass
        # Local imports to avoid import-time cycles.
        from unify.common._async_tool.loop_config import TOOL_LOOP_LINEAGE
        from unify.common.hierarchical_logger import log_boundary_event

        suffix = token_hex(2)

        parent = TOOL_LOOP_LINEAGE.get([])
        parent_lineage = list(parent) if isinstance(parent, list) else []
        hierarchy = [*parent_lineage, f"{self._function_name}({suffix})"]

        try:
            log_boundary_event("->".join(hierarchy), "Executing function...", icon="🛠️")
        except Exception:
            pass

        # Ensure synchronous work at call-time (if any) happens under the lineage frame.
        token_call = TOOL_LOOP_LINEAGE.set(hierarchy)
        try:
            result = self._wrapped(*args, **kwargs)
        except Exception:
            TOOL_LOOP_LINEAGE.reset(token_call)
            raise
        finally:
            # For async results we only needed the lineage during coroutine construction.
            # The actual awaited execution will run under a new token created in the
            # awaiting task context below.
            try:
                TOOL_LOOP_LINEAGE.reset(token_call)
            except Exception:
                pass

        if inspect.isawaitable(result):

            async def _await_and_finalize():
                token_run = TOOL_LOOP_LINEAGE.set(hierarchy)
                try:
                    return await result
                finally:
                    TOOL_LOOP_LINEAGE.reset(token_run)

            return _await_and_finalize()

        return result


class _DependencyVisitor(ast.NodeVisitor):
    """
    Statefully analyzes function AST to find direct calls and indirect calls
    via variables assigned function names, specifically looking for names
    known to the FunctionManager.
    """

    def __init__(self, known_function_names: Set[str]):
        self.known_function_names = known_function_names
        self.dependencies: Set[str] = set()
        self._assignment_map: Dict[str, str] = {}

    def visit_Assign(self, node: ast.Assign):
        # Only track simple assignments: target_var = potential_func_name
        if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            target_var = node.targets[0].id
            if isinstance(node.value, ast.Name):
                assigned_name = node.value.id
                # Check if the assigned name is one of the functions we manage
                if assigned_name in self.known_function_names:
                    # Record the mapping for the current scope
                    self._assignment_map[target_var] = assigned_name
                # If variable is assigned something else, remove mapping
                elif target_var in self._assignment_map:
                    del self._assignment_map[target_var]
            # If variable is assigned non-Name, remove mapping
            elif target_var in self._assignment_map:
                del self._assignment_map[target_var]

        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        func_node = node.func
        called_name: Optional[str] = None

        # Case 1: Direct call -> func_name()
        if isinstance(func_node, ast.Name):
            func_name = func_node.id
            # Check if it's a direct call to a known library function
            if func_name in self.known_function_names:
                called_name = func_name
            # Check if it's an indirect call via a mapped variable -> var()
            elif func_name in self._assignment_map:
                called_name = self._assignment_map[func_name]

        # Case 2: Method call -> obj.method() - generally ignore for dependency injection
        # (objects like ``primitives`` are globally available)

        if called_name:
            self.dependencies.add(called_name)

        self.generic_visit(node)  # Continue traversal

    def visit_Return(self, node: ast.Return):
        # Case 3: Return statement -> return func_name or return var
        if isinstance(node.value, ast.Name):
            returned_name = node.value.id
            # Check if returning a known function name directly
            if returned_name in self.known_function_names:
                self.dependencies.add(returned_name)
            # Also check if returning a variable that was assigned a function
            elif returned_name in self._assignment_map:
                self.dependencies.add(self._assignment_map[returned_name])
        self.generic_visit(node)


# Pattern for shell script metadata comments
_SHELL_NAME_PATTERN = re.compile(r"^#\s*@name:\s*(.+?)\s*$", re.MULTILINE)
_SHELL_ARGS_PATTERN = re.compile(r"^#\s*@args:\s*(.+?)\s*$", re.MULTILINE)
_SHELL_DESC_PATTERN = re.compile(r"^#\s*@description:\s*(.+?)\s*$", re.MULTILINE)


def _parse_shell_script_metadata(source: str) -> Dict[str, Optional[str]]:
    """
    Parse metadata from shell script comments.

    Expected format at the top of the script::

        #!/bin/sh
        # @name: my_function
        # @args: (input_file output_file --verbose)
        # @description: Brief description of what the function does

    Returns:
        Dict with keys: name, argspec, docstring (any may be None if not found)
    """
    name_match = _SHELL_NAME_PATTERN.search(source)
    args_match = _SHELL_ARGS_PATTERN.search(source)
    desc_match = _SHELL_DESC_PATTERN.search(source)

    return {
        "name": name_match.group(1).strip() if name_match else None,
        "argspec": args_match.group(1).strip() if args_match else "()",
        "docstring": desc_match.group(1).strip() if desc_match else "",
    }


def _instrument_for_child(source: str) -> str:
    """Ship steering probes with source bound for a venv subprocess.

    The child runs the probes against shims ``venv_runner`` installs, which
    read the control channel — so a loop that makes no primitive call is
    still interruptible between dispatches. Source that does not parse ships
    unchanged, so the child reports the SyntaxError exactly as an unsteered
    run would.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source
    return ast.unparse(instrument(tree, tool_namespaces=set(DEFAULT_TOOL_NAMESPACES)))


class _VenvConnection:
    """
    Manages a persistent connection to a venv subprocess in server mode.

    The subprocess maintains state across calls, enabling variables to persist
    between function executions within the same venv.
    """

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        venv_id: int,
        function_manager: "FunctionManager",
    ):
        self._process = process
        self._venv_id = venv_id
        self._function_manager = function_manager
        self._lock = asyncio.Lock()  # Serialize calls to same venv
        self._closed = False
        self._tainted = False  # Set to True after timeout or other corruption

    @classmethod
    async def create(
        cls,
        venv_id: int,
        function_manager: "FunctionManager",
        timeout: float = 30.0,
    ) -> "_VenvConnection":
        """
        Create a new persistent venv connection.

        Args:
            venv_id: The virtual environment to connect to.
            function_manager: The FunctionManager instance for venv preparation.
            timeout: Timeout for subprocess startup.

        Returns:
            A new _VenvConnection instance.

        Raises:
            RuntimeError: If the subprocess fails to start or send ready signal.
        """
        python_path = await function_manager.prepare_venv(venv_id=venv_id)
        runner_path = function_manager._get_venv_runner_path(venv_id)

        from unify.function_manager.execution_env import (
            sandbox_env as build_sandbox_env,
        )

        use_process_group = sys.platform != "win32"
        process = await asyncio.create_subprocess_exec(
            str(python_path),
            str(runner_path),
            "--server",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=use_process_group,
            env=build_sandbox_env(),
        )

        conn = cls(process, venv_id, function_manager)

        # Wait for ready signal from subprocess
        try:
            ready_msg = await asyncio.wait_for(
                conn._read_message(),
                timeout=timeout,
            )
            if ready_msg.get("type") != "ready":
                raise RuntimeError(
                    f"Venv {venv_id} subprocess sent unexpected message: {ready_msg}",
                )
        except asyncio.TimeoutError:
            await conn.shutdown()
            raise RuntimeError(
                f"Venv {venv_id} subprocess did not send ready signal within {timeout}s",
            )
        except Exception as e:
            await conn.shutdown()
            raise RuntimeError(
                f"Venv {venv_id} subprocess failed to start: {e}",
            ) from e

        return conn

    async def _read_message(self) -> dict:
        """Read a JSON message from the subprocess stdout."""
        if self._process.stdout is None:
            raise RuntimeError("Subprocess stdout is None")
        line = await self._process.stdout.readline()
        if not line:
            raise EOFError("Subprocess stdout closed")
        return json.loads(line.decode().strip())

    async def _write_message(self, msg: dict) -> None:
        """Write a JSON message to the subprocess stdin."""
        if self._process.stdin is None:
            raise RuntimeError("Subprocess stdin is None")
        data = json.dumps(msg) + "\n"
        self._process.stdin.write(data.encode())
        await self._process.stdin.drain()

    def is_alive(self) -> bool:
        """Check if the subprocess is still running and usable."""
        return (
            not self._closed and not self._tainted and self._process.returncode is None
        )

    async def execute(
        self,
        implementation: str,
        call_kwargs: dict,
        is_async: bool,
        primitives: Optional[Any] = None,
        timeout: Optional[float] = None,
        env_overlay: Optional[Dict[str, str]] = None,
    ) -> dict:
        """
        Execute a function in the persistent venv subprocess.

        While a steering session is in flight, each RPC reply doubles as a
        checkpoint and a correction re-sends the (patched) source over the
        same connection, replaying already-completed dispatches from the
        parent's cache. The session arrives by contextvar so it follows the
        call rather than this long-lived connection.

        Args:
            implementation: The function source code.
            call_kwargs: Keyword arguments to pass to the function.
            is_async: Whether the function is async.
            primitives: The Primitives instance for RPC access.
            timeout: Execution timeout in seconds (None for no timeout).

        Returns:
            Dict with keys: result, error, stdout, stderr

        Raises:
            RuntimeError: If the subprocess has died or execution fails.
            asyncio.TimeoutError: If execution exceeds timeout.
        """
        async with self._lock:
            if not self.is_alive():
                raise RuntimeError(
                    f"Venv {self._venv_id} subprocess has died (returncode={self._process.returncode})",
                )

            steering = active_session()

            async def _attempt(source: str) -> dict:
                """Send one execute request for *source* and relay its RPC."""
                await self._write_message(
                    {
                        "type": "execute",
                        "implementation": (
                            _instrument_for_child(source)
                            if steering is not None
                            else source
                        ),
                        "call_kwargs": call_kwargs,
                        "is_async": is_async,
                        "env_overlay": env_overlay or {},
                    },
                )

                # Set when a correction interrupted this attempt; the child's
                # completion is then an unwind to discard, not a result.
                interrupted: Optional[ControlledInterruption] = None
                # Corrections that land between dispatches reach the child
                # through the control channel, not through an RPC reply.
                watcher = (
                    asyncio.create_task(
                        steering.relay_corrections(
                            source,
                            lambda request: self._write_message(
                                interrupt_directive(request),
                            ),
                        ),
                    )
                    if steering is not None
                    else None
                )
                pause_watcher = (
                    asyncio.create_task(
                        steering.relay_pause(
                            lambda paused: self._function_manager._set_process_paused(
                                self._process,
                                use_process_group=sys.platform != "win32",
                                paused=paused,
                            ),
                        ),
                    )
                    if steering is not None
                    else None
                )

                try:
                    while True:
                        msg = await self._read_message()
                        msg_type = msg.get("type")

                        if msg_type == "complete":
                            if interrupted is not None:
                                raise interrupted
                            child_interrupted = msg.get("interrupted")
                            if child_interrupted:
                                # The child unwound at an instrumented
                                # checkpoint; discard the attempt and retry.
                                raise ControlledInterruption(child_interrupted)
                            return msg

                        if msg_type == "rpc_call":
                            # Handle RPC call from subprocess
                            try:
                                reply = await self._handle_rpc_call(
                                    msg,
                                    primitives=primitives,
                                )
                            except ControlledInterruption as interruption:
                                # The child is blocked on this reply, so
                                # telling it to unwind here is the interrupt
                                # probe realised without instrumentation.
                                interrupted = interruption
                                reply = {
                                    "type": "rpc_interrupt",
                                    "id": msg.get("id"),
                                    "reason": str(interruption),
                                }
                            await self._write_message(reply)
                        else:
                            logger.warning(
                                f"Venv {self._venv_id}: unexpected message type '{msg_type}'",
                            )
                finally:
                    for task in (watcher, pause_watcher):
                        if task is None:
                            continue
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            # A watcher can lose the race with the run ending;
                            # the attempt's own outcome stands.
                            logger.debug(
                                "steering: subprocess watcher failed",
                                exc_info=True,
                            )
                    if pause_watcher is not None:
                        await self._function_manager._set_process_paused(
                            self._process,
                            use_process_group=sys.platform != "win32",
                            paused=False,
                        )

            async def _run(source: str) -> dict:
                if timeout is None:
                    return await _attempt(source)
                try:
                    return await asyncio.wait_for(_attempt(source), timeout=timeout)
                except asyncio.TimeoutError:
                    # After a timeout, the subprocess is in an unknown state.
                    # Mark it as tainted so the pool recreates it on next use.
                    self._tainted = True
                    raise

            if steering is None:
                return await _run(implementation)
            try:
                return await run_with_steering(
                    implementation,
                    _run,
                    session=steering,
                )
            except ExecutionStopped as stopped:
                return {
                    "result": stopped.outcome,
                    "error": None,
                    "stdout": "",
                    "stderr": "",
                }

    async def _handle_rpc_call(
        self,
        msg: dict,
        primitives: Optional[Any],
    ) -> dict:
        """Answer one RPC message from the subprocess.

        Dispatch, memoisation and interrupts are shared with the one-shot
        path via :meth:`FunctionManager._handle_rpc_call`, so a pooled session
        cannot drift into being silently unsteerable. A
        :class:`ControlledInterruption` propagates to the execute loop, which
        owns telling the child to unwind.
        """
        request_id = msg.get("id")
        try:
            result = await self._function_manager._handle_rpc_call(
                path=msg.get("path", ""),
                kwargs=msg.get("kwargs", {}),
                primitives=primitives,
            )
        except ControlledInterruption:
            raise
        except Exception as e:
            return {"type": "rpc_error", "id": request_id, "error": str(e)}
        return {
            "type": "rpc_result",
            "id": request_id,
            "result": self._function_manager._make_json_serializable(result),
        }

    async def get_state(self, timeout: float = 30.0) -> Dict[str, Any]:
        """
        Get serialized user-defined state from the persistent subprocess.

        This is used for read_only mode to capture the current state before
        executing in an ephemeral subprocess.

        Args:
            timeout: Timeout for state retrieval.

        Returns:
            Dict of serialized state variables.

        Raises:
            RuntimeError: If the subprocess has died or retrieval fails.
            asyncio.TimeoutError: If retrieval exceeds timeout.
        """
        async with self._lock:
            if not self.is_alive():
                raise RuntimeError(
                    f"Venv {self._venv_id} subprocess has died (returncode={self._process.returncode})",
                )

            await self._write_message({"type": "get_state"})

            async def wait_for_state() -> Dict[str, Any]:
                while True:
                    msg = await self._read_message()
                    if msg.get("type") == "state":
                        return msg.get("state", {})
                    # Ignore other message types while waiting

            if timeout is not None:
                return await asyncio.wait_for(wait_for_state(), timeout=timeout)
            return await wait_for_state()

    async def shutdown(self, timeout: float = 5.0) -> None:
        """
        Gracefully shut down the subprocess.

        Args:
            timeout: Timeout for graceful shutdown before force-killing.
        """
        if self._closed:
            return
        self._closed = True

        if self._process.returncode is not None:
            return

        try:
            # Try graceful shutdown
            await self._write_message({"type": "shutdown"})
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
        except (asyncio.TimeoutError, Exception):
            # Force kill if graceful shutdown fails
            try:
                if sys.platform != "win32":
                    # Kill entire process group
                    os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
                else:
                    self._process.terminate()
                await asyncio.wait_for(self._process.wait(), timeout=2.0)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass


@dataclass
class SessionMetadata:
    venv_id: int
    session_id: int
    created_at: datetime
    last_used: datetime


class SessionLimitError(RuntimeError):
    def __init__(self, *, message: str):
        super().__init__(message)
        self.message = message

    def to_error_dict(self) -> dict:
        return {"error": self.message, "error_type": "resource_limit"}


class VenvPool:
    """
    Manages a pool of persistent venv subprocess connections.

    Each sandbox gets its own VenvPool, ensuring state isolation between
    different actors/sandboxes while preserving state across function calls
    within the same sandbox.

    Connections are keyed by (venv_id, session_id), allowing multiple independent
    stateful sessions per venv. Each session has its own subprocess and globals.
    """

    _instances = WeakSet()

    def __init__(self, *, max_total_sessions: int = 20) -> None:
        # Key: (venv_id, session_id) -> _VenvConnection
        self._connections: Dict[Tuple[int, int], _VenvConnection] = {}
        self._metadata: Dict[Tuple[int, int], SessionMetadata] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._max_total_sessions = int(max_total_sessions)
        self._invalidation_generation = 0
        self.__class__._instances.add(self)

    @classmethod
    def invalidate_all_pools(cls) -> int:
        """Drop every live pool connection so future executions reload credentials."""
        invalidated = 0
        for pool in list(cls._instances):
            invalidated += pool.invalidate_sessions()
        return invalidated

    def invalidate_sessions(self) -> int:
        """Retire pooled sessions while keeping the pool reusable."""
        self._invalidation_generation += 1
        connections = list(self._connections.values())
        self._connections.clear()
        self._metadata.clear()
        if not connections:
            return 0

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self._shutdown_retired_connections(connections))
        else:
            loop.create_task(self._shutdown_retired_connections(connections))
        return len(connections)

    async def _shutdown_retired_connections(
        self,
        connections: List["_VenvConnection"],
    ) -> None:
        """Close retired connections through the normal subprocess lifecycle."""
        for conn in connections:
            try:
                await conn.shutdown()
            except Exception:
                pass

    async def get_or_create_connection(
        self,
        venv_id: int,
        function_manager: "FunctionManager",
        session_id: int = 0,
        timeout: float = 30.0,
    ) -> _VenvConnection:
        """
        Get an existing connection or create a new one for the given venv/session.

        Args:
            venv_id: The virtual environment ID.
            function_manager: The FunctionManager for venv preparation.
            session_id: The session ID within the venv (default 0).
            timeout: Timeout for creating a new connection.

        Returns:
            A _VenvConnection instance.
        """
        key = (venv_id, session_id)
        while True:
            async with self._lock:
                if self._closed:
                    raise RuntimeError("VenvPool has been closed")

                if key in self._connections:
                    conn = self._connections[key]
                    if conn.is_alive():
                        md = self._metadata.get(key)
                        if md is not None:
                            md.last_used = datetime.now(timezone.utc)
                        return conn
                    # Connection died, remove it and create a new one
                    logger.warning(
                        f"VenvPool: connection for venv {venv_id} session {session_id} died, creating new one",
                    )
                    del self._connections[key]
                    self._metadata.pop(key, None)

                # Enforce global session cap (across all venv_id/session_id combinations).
                active = sum(1 for c in self._connections.values() if c.is_alive())
                if active >= self._max_total_sessions:
                    raise SessionLimitError(
                        message=f"Maximum sessions reached for python ({active}/{self._max_total_sessions})",
                    )

                generation = self._invalidation_generation
                # Create new connection
                conn = await _VenvConnection.create(
                    venv_id=venv_id,
                    function_manager=function_manager,
                    timeout=timeout,
                )
                if generation != self._invalidation_generation:
                    try:
                        await conn.shutdown()
                    except Exception:
                        pass
                    continue
                self._connections[key] = conn
                now = datetime.now(timezone.utc)
                self._metadata[key] = SessionMetadata(
                    venv_id=int(venv_id),
                    session_id=int(session_id),
                    created_at=now,
                    last_used=now,
                )
                return conn

    async def execute_in_venv(
        self,
        *,
        venv_id: int,
        implementation: str,
        call_kwargs: dict,
        is_async: bool,
        session_id: int = 0,
        primitives: Optional[Any] = None,
        function_manager: "FunctionManager",
        timeout: Optional[float] = None,
        env_overlay: Optional[Dict[str, str]] = None,
    ) -> dict:
        """
        Execute a function in a persistent venv subprocess.

        Args:
            venv_id: The virtual environment to use.
            implementation: The function source code.
            call_kwargs: Keyword arguments to pass to the function.
            is_async: Whether the function is async.
            session_id: The session ID within the venv (default 0).
            primitives: The Primitives instance for RPC access.
            function_manager: The FunctionManager for venv preparation.
            timeout: Execution timeout in seconds.

        Returns:
            Dict with keys: result, error, stdout, stderr
        """
        key = (venv_id, session_id)
        try:
            conn = await self.get_or_create_connection(
                venv_id=venv_id,
                function_manager=function_manager,
                session_id=session_id,
            )
        except SessionLimitError as e:
            return {
                "result": None,
                "stdout": "",
                "stderr": "",
                "error": e.message,
                "error_type": "resource_limit",
            }

        try:
            out = await conn.execute(
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                primitives=primitives,
                timeout=timeout,
                env_overlay=env_overlay,
            )
            # Update last_used best-effort
            md = self._metadata.get(key)
            if md is not None:
                md.last_used = datetime.now(timezone.utc)
            return out
        except RuntimeError as e:
            if "subprocess has died" in str(e):
                # Try to recreate and retry once
                logger.warning(
                    f"VenvPool: retrying after subprocess death for venv {venv_id} session {session_id}",
                )
                async with self._lock:
                    if key in self._connections:
                        del self._connections[key]

                conn = await self.get_or_create_connection(
                    venv_id=venv_id,
                    function_manager=function_manager,
                    session_id=session_id,
                )
                return await conn.execute(
                    implementation=implementation,
                    call_kwargs=call_kwargs,
                    is_async=is_async,
                    primitives=primitives,
                    timeout=timeout,
                    env_overlay=env_overlay,
                )
            raise

    def get_all_sessions(self) -> List[Dict[str, Any]]:
        """Return list of all active python venv sessions with metadata."""
        out: List[Dict[str, Any]] = []
        for (venv_id, session_id), conn in list(self._connections.items()):
            if not conn.is_alive():
                continue
            md = self._metadata.get((venv_id, session_id))
            if md is None:
                now = datetime.now(timezone.utc)
                md = SessionMetadata(
                    venv_id=int(venv_id),
                    session_id=int(session_id),
                    created_at=now,
                    last_used=now,
                )
                self._metadata[(venv_id, session_id)] = md
            out.append(
                {
                    "language": "python",
                    "session_id": int(session_id),
                    "venv_id": int(venv_id),
                    "created_at": md.created_at.isoformat(),
                    "last_used": md.last_used.isoformat(),
                    "state_summary": "active",
                },
            )
        return out

    async def get_session_state(
        self,
        *,
        venv_id: int,
        session_id: int,
        function_manager: "FunctionManager",
        detail: str = "summary",
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """
        Inspect state of a python venv-backed session.
        """
        key = (int(venv_id), int(session_id))
        if key not in self._connections or not self._connections[key].is_alive():
            return {
                "error": f"Python venv session {(int(venv_id), int(session_id))} not found",
                "error_type": "validation",
            }
        state = await self.get_connection_state(
            venv_id=int(venv_id),
            function_manager=function_manager,
            session_id=int(session_id),
            timeout=timeout,
        )

        def _is_secret_name(n: str) -> bool:
            nn = n.lower()
            return any(
                tok in nn
                for tok in ("token", "secret", "apikey", "api_key", "password", "key")
            )

        def _safe_repr(name: str, value: Any) -> str:
            if _is_secret_name(name):
                return "<redacted>"
            try:
                s = repr(value)
            except Exception:
                s = f"<{type(value).__name__}>"
            if len(s) > 500:
                s = s[:500] + "..."
            return s

        names = sorted(
            [k for k in state.keys() if isinstance(k, str) and not k.startswith("_")],
        )
        if detail in ("summary", "names"):
            return {
                "names": names,
                "count": len(names),
            }
        if detail == "full":
            return {name: _safe_repr(name, state.get(name)) for name in names}
        return {
            "error": f"Unsupported detail level: {detail!r}",
            "error_type": "validation",
        }

    async def close_session(self, *, venv_id: int, session_id: int) -> bool:
        """Close a specific venv session and free resources."""
        key = (int(venv_id), int(session_id))
        async with self._lock:
            conn = self._connections.get(key)
            if conn is None:
                return False
            try:
                await conn.shutdown()
            except Exception:
                pass
            self._connections.pop(key, None)
            self._metadata.pop(key, None)
            return True

    async def get_connection_state(
        self,
        venv_id: int,
        function_manager: "FunctionManager",
        session_id: int = 0,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        """
        Get serialized state from a venv connection.

        Used for read_only mode to snapshot current state before ephemeral execution.

        Args:
            venv_id: The virtual environment ID.
            function_manager: The FunctionManager for venv preparation.
            session_id: The session ID within the venv (default 0).
            timeout: Timeout for state retrieval.

        Returns:
            Dict of serialized state variables.
        """
        conn = await self.get_or_create_connection(
            venv_id=venv_id,
            function_manager=function_manager,
            session_id=session_id,
        )
        return await conn.get_state(timeout=timeout)

    def list_active_sessions(self) -> List[Tuple[int, int]]:
        """
        List all active venv sessions in the pool.

        Returns:
            List of (venv_id, session_id) tuples for sessions with live connections.
        """
        return [key for key, conn in self._connections.items() if conn.is_alive()]

    async def get_all_states(
        self,
        function_manager: "FunctionManager",
        timeout: float = 30.0,
    ) -> Dict[Tuple[int, int], Dict[str, Any]]:
        """
        Get serialized state from all active venv connections.

        Args:
            function_manager: The FunctionManager for venv preparation.
            timeout: Timeout for state retrieval per connection.

        Returns:
            Dict mapping (venv_id, session_id) -> state dict for each active session.
        """
        results: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for key, conn in list(self._connections.items()):
            if conn.is_alive():
                try:
                    state = await conn.get_state(timeout=timeout)
                    results[key] = state
                except Exception as e:
                    # Connection may have died during iteration
                    results[key] = {"__error__": str(e)}
        return results

    async def close(self) -> None:
        """Close all connections in the pool."""
        async with self._lock:
            self._closed = True
            for conn in self._connections.values():
                await conn.shutdown()
            self._connections.clear()
            self._metadata.clear()

    def __del__(self) -> None:
        """Ensure cleanup on garbage collection."""
        if self._connections and not self._closed:
            # Can't run async cleanup in __del__, but we can try to kill processes
            for conn in self._connections.values():
                try:
                    if conn._process.returncode is None:
                        conn._process.kill()
                except Exception:
                    pass


class _InProcessFunctionProxy:
    """Proxy that wraps an in-process function with state mode support.

    This proxy enables in-process functions (no venv) to be called with the same
    state mode API as venv-backed functions. It supports three execution modes
    for fine-grained control over state management:

    Execution Modes
    ---------------
    **stateful** (default via ``__call__``, or explicit via ``.stateful()``):
        Executes in a persistent in-process session. Variables defined in previous
        calls persist across executions within the same session_id. Use this for
        iterative sessions where you want to build up state incrementally.

    **stateless** (via ``.stateless()``):
        Executes in a fresh globals dict with no inherited state. Each call starts
        with a clean environment. Use this for pure functions that should not
        depend on or affect any global state - guarantees reproducible results.

    **read_only** (via ``.read_only()``):
        Reads the current global state from the persistent session but executes
        in a fresh globals dict. Changes made during execution are NOT persisted
        back to the session. Use this for "what-if" exploration.

    Usage Examples
    --------------
    ```python
    # Stateful (default) - state persists between calls
    await set_config(key="debug", value=True)
    await run_analysis()  # can access 'config' from previous call

    # Explicit stateful (equivalent to default __call__)
    result = await compute.stateful(x=1, y=2)

    # Stateless - fresh environment each time
    result = await compute.stateless(x=1, y=2)

    # Read-only - see current state but don't modify it
    preview = await transform.read_only(factor=2)
    ```

    Design Note
    -----------
    The proxy is returned to callers (e.g., CodeActActor) for state mode control,
    but the **raw function** remains in the execution namespace. This allows:
    - Inter-function calls to work naturally (``await b()`` calls raw ``b``)
    - ``typing.get_type_hints(fn_name)`` to resolve correctly
    - Custom decorators (``@my_decorator``) to work during exec()

    The proxy exposes ``__wrapped__`` pointing to the raw function for introspection.
    """

    def __init__(
        self,
        *,
        function_manager: "FunctionManager",
        func_data: Dict[str, Any],
        namespace: Dict[str, Any],
        raw_callable: Callable[..., Any],
    ):
        self._function_manager = function_manager
        self._func_data = func_data
        self._namespace = namespace
        self._raw_callable = raw_callable

        # Copy key attributes from raw callable for introspection
        self.__name__ = str(func_data.get("name") or "unknown")
        self.__doc__ = str(func_data.get("docstring") or "")
        self.__wrapped__ = raw_callable  # Standard Python convention for wrapper chains

    async def _execute_with_mode(
        self,
        state_mode: Literal["stateful", "read_only", "stateless"],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute the function with the specified state mode.

        Args:
            state_mode: How to handle global state during execution.
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's return value.
        """
        if state_mode == "stateful":
            # Execute directly using the raw callable in the shared namespace.
            # This is the existing behavior - state naturally persists in the namespace.
            result = self._raw_callable(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            return result

        # For stateless and read_only, use execute_function with appropriate
        # mode. Forward environment namespace objects (primitives, etc.) but
        # NOT user-defined state variables -- those are managed by state_mode.
        proxy_ns: Dict[str, Any] = {}
        val = self._namespace.get("primitives")
        if val is not None:
            proxy_ns["primitives"] = val
        result = await self._function_manager.execute_function(
            function_name=self.__name__,
            call_kwargs=kwargs,
            target_venv_id=None,  # Force in-process execution
            state_mode=state_mode,
            session_id=0,  # Default session for read_only state source
            extra_namespaces=proxy_ns if proxy_ns else None,
        )

        if result.get("error"):
            raise RuntimeError(str(result.get("error")))
        return result.get("result")

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the function in stateful mode (default).

        State persists across calls within the shared namespace. Variables
        defined in previous executions remain accessible. This is the default
        behavior, suitable for iterative/interactive sessions.

        Equivalent to calling ``.stateful()`` explicitly.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's return value.
        """
        return await self._execute_with_mode("stateful", *args, **kwargs)

    def stateful(self, *args: Any, **kwargs: Any):
        """
        Execute the function in stateful mode (explicit form of default ``__call__``).

        State persists across calls within the shared namespace. Variables
        defined in previous executions remain accessible. Use this when you want
        to be explicit about the execution mode in your code.

        Equivalent to ``await fn()`` but more self-documenting.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.
        """
        return self._execute_with_mode("stateful", *args, **kwargs)

    def stateless(self, *args: Any, **kwargs: Any):
        """
        Execute the function in stateless mode (fresh environment).

        Each call executes with fresh globals and no inherited state.
        The function cannot see or modify any variables from previous executions.
        Use this for pure functions that should produce identical results
        regardless of execution history.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.
        """
        return self._execute_with_mode("stateless", *args, **kwargs)

    def read_only(self, *args: Any, **kwargs: Any):
        """
        Execute the function in read-only mode (sees state, no persistence).

        Reads the current global state from the persistent in-process session but
        executes in a fresh globals dict. Any modifications during execution are
        discarded - the persistent session state remains unchanged.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.
        """
        return self._execute_with_mode("read_only", *args, **kwargs)


class _VenvFunctionProxy:
    """Proxy that wraps a venv-backed function as an awaitable callable.

    This proxy enables venv-isolated functions to be called transparently from
    the CodeActActor sandbox. It supports three execution modes for fine-grained
    control over state management:

    Execution Modes
    ---------------
    **stateful** (default via ``__call__``, or explicit via ``.stateful()``):
        Executes in a persistent subprocess connection via VenvPool. Variables
        defined in previous calls persist across executions. Use this for
        iterative sessions where you want to build up state incrementally
        (e.g., loading data once, then running multiple analyses).

    **stateless** (via ``.stateless()``):
        Executes in a fresh subprocess with no inherited state. Each call starts
        with a clean globals dict. Use this for pure functions that should not
        depend on or affect any global state - guarantees reproducible results
        regardless of prior execution history.

    **read_only** (via ``.read_only()``):
        Reads the current global state from the persistent connection but executes
        in an ephemeral subprocess. Changes made during execution are NOT persisted
        back to the session. Use this for "what-if" exploration - you can inspect
        or transform session state without side effects.

    Usage Examples
    --------------
    ```python
    # Stateful (default) - state persists between calls
    # First call: loads data into session globals
    await load_dataset(path="data.csv")
    # Second call: can access the loaded data
    await analyze_dataset()

    # Explicit stateful (equivalent to default __call__)
    result = await my_func.stateful(x=1, y=2)

    # Stateless - fresh environment each time, no side effects
    # Useful for pure computations that shouldn't depend on session state
    result = await my_func.stateless(x=1, y=2)

    # Read-only - see current state but don't modify it
    # Useful for exploratory queries without affecting the main session
    preview = await transform_data.read_only(sample_size=100)
    ```

    When to Use Each Mode
    ---------------------
    - **stateful**: Default for most use cases. Enables Jupyter-notebook-style
      sessions where you iteratively build up state.
    - **stateless**: When you need guaranteed isolation - the function's behavior
      depends only on its explicit arguments, never on hidden global state.
    - **read_only**: When you want to "peek" at what a transformation would do
      without committing the changes, or run exploratory analysis without
      polluting the session namespace.
    """

    def __init__(
        self,
        *,
        function_manager: "FunctionManager",
        func_data: Dict[str, Any],
        namespace: Dict[str, Any],
    ):
        self._function_manager = function_manager
        self._func_data = func_data
        self._namespace = namespace

        self.__name__ = str(func_data.get("name") or "unknown")
        self.__doc__ = str(func_data.get("docstring") or "")
        # Note: venv functions don't have a raw_callable since they run in subprocess

    @staticmethod
    def _map_positional_args(
        *,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        implementation: str,
        func_name: str,
    ) -> dict[str, Any]:
        """
        Map positional args to kwargs using AST-extracted parameter names.

        Note: the venv runner currently executes with ``fn(**call_kwargs)``, so we can
        only support positional args by mapping them onto non-positional-only params.
        """
        if not args:
            return kwargs

        try:
            tree = ast.parse(implementation)
        except Exception as e:
            raise TypeError(
                f"Cannot map positional args for venv function '{func_name}': failed to parse implementation",
            ) from e

        if not tree.body or not isinstance(
            tree.body[0],
            (ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            raise TypeError(
                f"Cannot map positional args for venv function '{func_name}': implementation must contain exactly one top-level function",
            )

        node: ast.FunctionDef | ast.AsyncFunctionDef = tree.body[0]
        if node.args.posonlyargs:
            raise TypeError(
                f"Cannot call venv function '{func_name}' with positional-only args; use keyword arguments",
            )
        if node.args.vararg is not None:
            raise TypeError(
                f"Cannot call venv function '{func_name}' with *args; use keyword arguments",
            )

        param_names = [a.arg for a in node.args.args]
        if len(args) > len(param_names):
            raise TypeError(
                f"Too many positional arguments for venv function '{func_name}'",
            )

        mapped: dict[str, Any] = dict(kwargs)
        for k, v in zip(param_names[: len(args)], args):
            if k in mapped:
                raise TypeError(
                    f"Multiple values for argument '{k}' in venv function '{func_name}'",
                )
            mapped[k] = v
        return mapped

    async def _execute_with_mode(
        self,
        state_mode: Literal["stateful", "read_only", "stateless"],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute the function with the specified state mode.

        Args:
            state_mode: How to handle global state during execution.
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's return value.

        Raises:
            ValueError: If venv_id is missing or implementation is invalid.
            RuntimeError: If execution fails (error from subprocess).
        """
        venv_id = self._func_data.get("venv_id")
        if venv_id is None:
            raise ValueError(f"Venv proxy '{self.__name__}' missing venv_id")

        implementation = self._func_data.get("implementation")
        if not isinstance(implementation, str) or not implementation.strip():
            raise ValueError(f"Venv function '{self.__name__}' has no implementation")

        # Strip @custom_function decorators (not available in subprocess runner).

        # Determine async-ness based on source.
        is_async = "async def" in implementation

        # Resolve RPC targets from the injected namespace (caller-controlled).
        primitives = self._namespace.get("primitives")

        call_kwargs = self._map_positional_args(
            args=args,
            kwargs=kwargs,
            implementation=implementation,
            func_name=self.__name__,
        )

        # Check if a persistent venv pool is available (injected by PythonExecutionSession)
        venv_pool = self._namespace.get("__venv_pool__")
        venv_id_int = int(venv_id)

        if state_mode == "stateful":
            # Use persistent connection via VenvPool - state persists across calls
            if venv_pool is not None:
                result = await venv_pool.execute_in_venv(
                    venv_id=venv_id_int,
                    implementation=implementation,
                    call_kwargs=call_kwargs,
                    is_async=is_async,
                    primitives=primitives,
                    function_manager=self._function_manager,
                )
            else:
                # No pool available - fall back to stateless (one-shot) execution
                # This maintains backward compatibility when VenvPool isn't injected
                result = await self._function_manager.execute_in_venv(
                    venv_id=venv_id_int,
                    implementation=implementation,
                    call_kwargs=call_kwargs,
                    is_async=is_async,
                    primitives=primitives,
                )

        elif state_mode == "read_only":
            # Read current state from persistent connection, execute in ephemeral subprocess
            # Changes are NOT persisted back to the session
            if venv_pool is None:
                raise ValueError(
                    f"read_only mode for '{self.__name__}' requires a VenvPool to read "
                    f"existing state. Use stateless mode if you don't need to read session state.",
                )
            # Get current state from the persistent connection
            initial_state = await venv_pool.get_connection_state(
                venv_id=venv_id_int,
                function_manager=self._function_manager,
            )
            # Execute in fresh subprocess with that state (not modifying persistent state)
            result = await self._function_manager.execute_in_venv(
                venv_id=venv_id_int,
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                initial_state=initial_state,
                primitives=primitives,
            )

        else:  # state_mode == "stateless"
            # Fresh subprocess with no inherited state - pure function behavior
            result = await self._function_manager.execute_in_venv(
                venv_id=venv_id_int,
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                primitives=primitives,
            )

        if result.get("error"):
            raise RuntimeError(str(result.get("error")))
        return result.get("result")

    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the function in stateful mode (default).

        State persists across calls within the same VenvPool session. Variables
        defined in previous executions remain accessible. This is the default
        behavior, suitable for iterative/interactive sessions.

        Equivalent to calling ``.stateful()`` explicitly.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            The function's return value.

        Example:
            ```python
            # First call - defines 'data' in session globals
            await load_data(path="input.csv")
            # Second call - can access 'data' from previous call
            await process_data()
            ```
        """
        return await self._execute_with_mode("stateful", *args, **kwargs)

    def stateful(self, *args: Any, **kwargs: Any):
        """
        Execute the function in stateful mode (explicit form of default ``__call__``).

        State persists across calls within the same VenvPool session. Variables
        defined in previous executions remain accessible. Use this when you want
        to be explicit about the execution mode in your code.

        Equivalent to ``await fn()`` but more self-documenting.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.
        """
        return self._execute_with_mode("stateful", *args, **kwargs)

    def stateless(self, *args: Any, **kwargs: Any):
        """
        Execute the function in stateless mode (fresh environment).

        Each call executes in a fresh subprocess with no inherited global state.
        The function cannot see or modify any variables from previous executions.
        Use this for pure functions that should produce identical results
        regardless of execution history.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.

        Example:
            ```python
            # Each call is completely independent - no shared state
            result1 = await compute_score.stateless(data=[1, 2, 3])
            result2 = await compute_score.stateless(data=[4, 5, 6])
            # result1 and result2 computed in isolated environments
            ```

        When to use:
            - Pure computations that shouldn't depend on hidden state
            - Functions where reproducibility is critical
            - Avoiding accidental state pollution from prior calls
        """
        return self._execute_with_mode("stateless", *args, **kwargs)

    def read_only(self, *args: Any, **kwargs: Any):
        """
        Execute the function in read-only mode (sees state, no persistence).

        Reads the current global state from the persistent VenvPool session but
        executes in an ephemeral subprocess. Any modifications to globals during
        execution are discarded - the persistent session state remains unchanged.

        This is useful for "what-if" exploration: you can inspect or transform
        the current session state without committing changes.

        Args:
            *args: Positional arguments passed to the function.
            **kwargs: Keyword arguments passed to the function.

        Returns:
            Awaitable that resolves to the function's return value.

        Raises:
            ValueError: If no VenvPool is available (read_only requires existing state).

        Example:
            ```python
            # Session has 'df' DataFrame from prior stateful calls
            await load_data(path="sales.csv")  # stateful: df now in session

            # Preview a transformation without modifying the session
            preview = await filter_data.read_only(min_value=100)
            # 'df' in session is unchanged - filter was applied to a copy

            # If the preview looks good, run it statefully to persist
            await filter_data(min_value=100)  # now session 'df' is filtered
            ```

        When to use:
            - Exploratory analysis without side effects
            - Previewing transformations before committing
            - Running queries against session state without modification
        """
        return self._execute_with_mode("read_only", *args, **kwargs)


class FunctionManager(BaseFunctionManager):
    """
    Keeps a catalogue of user-supplied Python functions and system primitives.

    User-defined functions are stored in `Functions/Compositional` with auto-incrementing
    IDs. System primitives (state manager methods) are stored in `Functions/Primitives`
    with explicit stable IDs that are consistent across all users.

    This separation ensures:
    - User function IDs are stable (adding/removing primitives doesn't affect them)
    - Primitive IDs are consistent across all users (hash-based stable IDs)
    - No ID collisions between the two namespaces
    """

    class Config:
        required_contexts = [
            TableContext(
                name=FUNCTIONS_VENVS_TABLE,
                description="Virtual environment configurations (pyproject.toml content).",
                fields=model_to_fields(VirtualEnv),
                unique_keys={"venv_id": "int"},
                auto_counting={"venv_id": None},
            ),
            TableContext(
                name=FUNCTIONS_COMPOSITIONAL_TABLE,
                description="User-defined functions with auto-incrementing IDs.",
                fields=model_to_fields(Function),
                unique_keys={"function_id": "int"},
                auto_counting={"function_id": None},
                foreign_keys=[
                    {
                        "name": "guidance_ids[*]",
                        "references": "Guidance.guidance_id",
                        "on_delete": "CASCADE",
                        "on_update": "CASCADE",
                    },
                    {
                        "name": "venv_id",
                        "references": f"{FUNCTIONS_VENVS_TABLE}.venv_id",
                        "on_delete": "SET NULL",
                        "on_update": "CASCADE",
                    },
                ],
            ),
            TableContext(
                name=FUNCTIONS_PRIMITIVES_TABLE,
                description="System action primitives with stable explicit IDs.",
                # Primitives share the `Function` model with Compositional, but
                # are never user-editable in place (implementation lives in Python,
                # not in stored rows), so the allowlisted ui_editable=True
                # annotations on `Function` (name, docstring, etc.) are
                # overridden to False here.
                fields=with_ui_editable_forced_false(model_to_fields(Function)),
                unique_keys={"function_id": "int"},
                # No auto_counting - primitives get explicit IDs from collect_primitives()
            ),
            TableContext(
                name=FUNCTIONS_META_TABLE,
                description="Metadata for primitives sync state.",
                fields=model_to_fields(FunctionsMeta),
                unique_keys={"meta_id": "int"},
            ),
            TableContext(
                name=FUNCTIONS_VERIFICATIONS_TABLE,
                description=(
                    "Append-only verification verdicts for compositional "
                    "functions, one row per pass per call."
                ),
                fields=model_to_fields(VerificationRow),
            ),
        ]

    # ------------------------------------------------------------------ #
    #  Construction                                                      #
    # ------------------------------------------------------------------ #

    def __init__(
        self,
        *,
        primitive_scope: Optional[PrimitiveScope] = None,
        filter_scope: Optional[str] = None,
        exclude_primitive_ids: Optional[FrozenSet[int]] = None,
        exclude_compositional_ids: Optional[FrozenSet[int]] = None,
        include_primitives: bool = True,
        daemon: bool = True,
        file_manager: Optional[LocalFileManager] = None,
    ) -> None:
        # Store the scope - this FunctionManager instance is permanently scoped
        # Default to the canonical role-scoped manager set when not specified.
        self._primitive_scope = primitive_scope or default_runtime_scope()
        self._filter_scope = filter_scope
        self._exclude_primitive_ids = (
            frozenset(exclude_primitive_ids) if exclude_primitive_ids else None
        )
        self._exclude_compositional_ids = (
            frozenset(exclude_compositional_ids) if exclude_compositional_ids else None
        )
        self._include_primitives = include_primitives
        self._registry = get_registry()
        self._daemon = daemon
        # ToDo: expose tools to LLM once needed
        self._tools: Dict[str, callable] = {}

        # Internal monotonically-increasing function-id counter.  We keep it local
        # to the manager to avoid an expensive scan across *all* logs every
        # time we create a function.  Initialised lazily on first use.
        self._next_id: Optional[int] = None

        self._venvs_ctx = ContextRegistry.get_context(self, FUNCTIONS_VENVS_TABLE)
        self._compositional_ctx = ContextRegistry.get_context(
            self,
            FUNCTIONS_COMPOSITIONAL_TABLE,
        )
        self._primitives_ctx = ContextRegistry.get_context(
            self,
            FUNCTIONS_PRIMITIVES_TABLE,
        )
        self._meta_ctx = ContextRegistry.get_context(self, FUNCTIONS_META_TABLE)
        self._verifications_ctx = ContextRegistry.get_context(
            self,
            FUNCTIONS_VERIFICATIONS_TABLE,
        )

        self._destination_context_lock = threading.RLock()
        self._destination_write_scoped = False

        # ------------------------------------------------------------------ #
        #  LocalFileManager reference (for VM sync manager access)           #
        # ------------------------------------------------------------------ #
        try:
            self._fm: Optional[LocalFileManager] = (
                file_manager if file_manager is not None else LocalFileManager()
            )
        except Exception:
            self._fm = None

        # ------------------------------------------------------------------ #
        #  In-process session state (for stateful/read_only modes)           #
        # ------------------------------------------------------------------ #
        # Dict[session_id, Dict[str, Any]] - persistent globals per session
        self._in_process_sessions: Dict[int, Dict[str, Any]] = {}

    @property
    def primitive_scope(self) -> PrimitiveScope:
        """The scope controlling which managers' primitives are accessible."""
        return self._primitive_scope

    @property
    def filter_scope(self) -> Optional[str]:
        """A boolean expression permanently applied to all compositional read queries."""
        return self._filter_scope

    @filter_scope.setter
    def filter_scope(self, value: Optional[str]) -> None:
        self._filter_scope = value

    @property
    def exclude_primitive_ids(self) -> Optional[FrozenSet[int]]:
        """Primitive function IDs excluded from ``Functions/Primitives`` queries."""
        return self._exclude_primitive_ids

    @exclude_primitive_ids.setter
    def exclude_primitive_ids(self, value: Optional[FrozenSet[int]]) -> None:
        self._exclude_primitive_ids = frozenset(value) if value else None

    @property
    def exclude_compositional_ids(self) -> Optional[FrozenSet[int]]:
        """Compositional function IDs excluded from ``Functions/Compositional`` queries."""
        return self._exclude_compositional_ids

    @exclude_compositional_ids.setter
    def exclude_compositional_ids(self, value: Optional[FrozenSet[int]]) -> None:
        self._exclude_compositional_ids = frozenset(value) if value else None

    @staticmethod
    def _build_id_exclusion(ids: Optional[FrozenSet[int]]) -> Optional[str]:
        """Build a filter clause excluding a set of function IDs.

        Returns ``None`` when *ids* is empty or ``None``.
        """
        if not ids:
            return None
        sorted_ids = sorted(ids)
        if len(sorted_ids) == 1:
            return f"function_id != {sorted_ids[0]}"
        joined_ids = ", ".join(str(fid) for fid in sorted_ids)
        return f"function_id not in [{joined_ids}]"

    def _scoped_filter(self, caller_filter: Optional[str]) -> Optional[str]:
        """Compose *caller_filter* with ``_filter_scope`` and compositional exclusions.

        Returns ``None`` when all parts are absent, meaning "no filter".
        """
        parts = [
            p
            for p in [
                caller_filter,
                self._filter_scope,
                self._build_id_exclusion(self._exclude_compositional_ids),
            ]
            if p
        ]
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        return " and ".join(f"({p})" for p in parts)

    def _scoped_primitive_filter(self) -> str:
        """Compose ``primitive_row_filter`` with primitive exclusions.

        Always returns a non-empty string (``primitive_row_filter`` never
        returns empty for valid scopes).
        """
        base = self._registry.primitive_row_filter(self._primitive_scope)
        excl = self._build_id_exclusion(self._exclude_primitive_ids)
        if not excl:
            return base
        return f"({base}) and ({excl})"

    def _primitive_read_specs(
        self,
        *,
        allowed_fields: Optional[List[str]] = None,
    ) -> List[FederatedSearchContext]:
        """Return the federated sources holding this deployment's primitives.

        Static primitives live once platform-wide in the public-read builtins
        catalogue project; the per-assistant ``Functions/Primitives`` context
        holds only materialized provider-backed integration tool rows. Both
        are scope-filtered at read time.
        """
        scoped = self._scoped_primitive_filter()
        return [
            FederatedSearchContext(
                context=BUILTINS_PRIMITIVES_CONTEXT,
                source="primitives",
                row_filter=scoped,
                allowed_fields=allowed_fields,
                project=builtins_project(),
            ),
            FederatedSearchContext(
                context=self._primitives_ctx,
                source="primitives",
                row_filter=f'({scoped}) and metadata["source"] == "provider_backed"',
                allowed_fields=allowed_fields,
            ),
        ]

    def _primitive_logs(
        self,
        *,
        extra_filter: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch primitive rows from every primitive source (non-ranked)."""
        rows: List[Dict[str, Any]] = []
        for spec in self._primitive_read_specs():
            row_filter = spec.row_filter
            if extra_filter:
                row_filter = f"({extra_filter}) and ({row_filter})"
            kwargs: Dict[str, Any] = {
                "context": spec.context,
                "project": spec.project,
                "filter": row_filter,
                "exclude_fields": list_private_fields(
                    spec.context,
                    project=spec.project,
                ),
            }
            if limit is not None:
                kwargs["limit"] = limit
            try:
                logs = db.get_logs(**kwargs)
            except _UnifyRequestError as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status == 404:
                    continue
                raise
            rows.extend(lg.entries for lg in logs)
        return rows

    def _function_context_for_root(self, root_context: str, table_name: str) -> str:
        """Return a concrete Functions context under a registry root."""
        return f"{root_context.strip('/')}/{table_name}"

    def _function_context_for_destination(
        self,
        table_name: str,
        *,
        destination: str | None,
    ) -> str:
        """Resolve a public destination into one concrete Functions context."""
        root_context = ContextRegistry.write_root(
            self,
            table_name,
            destination=destination,
        )
        return self._function_context_for_root(root_context, table_name)

    def _read_function_contexts(self, table_name: str) -> list[str]:
        """Return personal-first concrete contexts for a Functions table."""
        return list(
            dict.fromkeys(
                self._function_context_for_root(root, table_name)
                for root in ContextRegistry.read_roots(self, table_name)
            ),
        )

    def _read_compositional_contexts(self) -> list[str]:
        """Return function contexts, narrowed during destination-scoped writes."""
        if self._destination_write_scoped:
            return [self._compositional_ctx]
        return self._read_function_contexts(FUNCTIONS_COMPOSITIONAL_TABLE)

    def _read_venv_contexts(self) -> list[str]:
        """Return venv contexts, narrowed during destination-scoped writes."""
        if self._destination_write_scoped:
            return [self._venvs_ctx]
        return self._read_function_contexts(FUNCTIONS_VENVS_TABLE)

    @contextmanager
    def _temporary_function_context(self, attr_name: str, context: str):
        """Temporarily bind an existing storage method to a resolved context."""
        with self._destination_context_lock:
            original = getattr(self, attr_name)
            was_write_scoped = self._destination_write_scoped
            setattr(self, attr_name, context)
            self._destination_write_scoped = True
            try:
                yield
            finally:
                setattr(self, attr_name, original)
                self._destination_write_scoped = was_write_scoped

    @property
    def _dangerous_builtins(self) -> Set[str]:
        """
        A minimal set of truly dangerous built-ins that should never be allowed.
        These could compromise security or system integrity.
        """
        return {
            "eval",
            "exec",
            "compile",
            "__import__",
            "open",  # File system access should go through proper APIs
            "input",  # No interactive input in automated functions
            "breakpoint",  # No debugging breakpoints
            "exit",
            "quit",
        }

    def _parse_implementation(
        self,
        source: str,
    ) -> Tuple[str, ast.Module, ast.FunctionDef, str]:
        """
        Common syntactic checks (unchanged, but now returns the stripped
        source verbatim so we can persist it later).
        """
        stripped = source.lstrip("\n")
        first_line = stripped.splitlines()[0] if stripped else ""
        if first_line.startswith((" ", "\t")):
            raise ValueError(
                "Function definition must start at column 0 (no indentation).",
            )

        try:
            tree = ast.parse(source)
        except SyntaxError as e:
            raise ValueError(f"Syntax error:\n{e.text}") from e

        if len(tree.body) != 1 or not isinstance(
            tree.body[0],
            (ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            if any(
                isinstance(node, (ast.Import, ast.ImportFrom)) for node in tree.body
            ):
                raise ValueError(
                    "Implementation must be a single top-level function definition "
                    "with no module-level imports or other statements.",
                )
            raise ValueError(
                "Each implementation must contain exactly one top-level function.",
            )

        fn_node: Union[ast.FunctionDef, ast.AsyncFunctionDef] = tree.body[0]
        if fn_node.col_offset != 0:
            raise ValueError(
                f"Function {fn_node.name!r} must start at column 0 (no indentation).",
            )

        return fn_node.name, tree, fn_node, source

    def _collect_verified_dependencies(
        self,
        fn_node: Union[ast.FunctionDef, ast.AsyncFunctionDef],
        all_known_function_names: Set[str],
        *,
        environment_namespaces: FrozenSet[str] = frozenset(),
    ) -> Set[str]:
        """
        Uses the stateful _DependencyVisitor to find verified direct calls,
        indirect calls via variables, and returned function name references
        to other known library functions.

        When *environment_namespaces* is provided, dotted calls whose root segment
        matches one of the namespaces are also captured as dependencies.
        """
        return collect_dependencies_from_function_node(
            fn_node,
            all_known_function_names,
            environment_namespaces=environment_namespaces,
        )

    def _collect_function_calls(
        self,
        fn_node: Union[ast.FunctionDef, ast.AsyncFunctionDef],
    ) -> Set[str]:
        calls: Set[str] = set()
        for node in ast.walk(fn_node):
            if isinstance(node, ast.Call):
                name = self._format_callable_name(node.func)
                if name:
                    calls.add(name)
        return calls

    @staticmethod
    def _format_callable_name(callable_node: ast.AST) -> Optional[str]:
        """Return a best-effort fully qualified name for a callable.

        Handles both simple names (e.g., ``foo()``) and nested attributes
        (e.g., ``a.b.c()``). If the base of the attribute chain is not a simple
        ``ast.Name`` (e.g., ``get().b()``), this falls back to ``ast.unparse``
        when available.
        """
        # Simple function call: foo()
        if isinstance(callable_node, ast.Name):
            return callable_node.id

        # Attribute access: a.b.c()
        if isinstance(callable_node, ast.Attribute):
            parts: List[str] = []
            current: ast.AST = callable_node
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
                return ".".join(reversed(parts))
            # Fallback to unparse for complex bases like calls/subscripts
            try:
                return ast.unparse(callable_node)
            except Exception:
                pass
            return ".".join(reversed(parts)) if parts else None

        try:
            return ast.unparse(callable_node)
        except Exception:
            return None

    def _validate_function_calls(
        self,
        fn_name: str,
        calls: Set[str],
        provided_names: Set[str],
    ) -> None:
        """
        Validates function calls to prevent dangerous operations.

        Allows:
        - Built-in functions (except dangerous ones)
        - Any method calls on objects (e.g., primitives.*, call_handle.*, call.*)
        - User-defined functions (tracked as dependencies)

        Disallows:
        - Dangerous built-in functions (eval, exec, etc.)
        """
        dangerous = self._dangerous_builtins

        for called in calls:
            # Allow all method calls (anything with a dot)
            # This includes primitives.*, call_handle.*, obj.method(), etc.
            if "." in called:
                continue

            # Block only truly dangerous built-ins
            if called in dangerous:
                raise ValueError(
                    f"Dangerous built-in '{called}' is not permitted in {fn_name}(). "
                    f"Functions cannot use: {', '.join(sorted(dangerous))}",
                )

    # ------------------------------------------------------------------ #
    #  Private helpers for persistence                                    #
    # ------------------------------------------------------------------ #

    def _get_log_by_function_id(
        self,
        *,
        function_id: int,
        raise_if_missing: bool = True,
    ) -> Optional[db.Log]:
        logs = db.get_logs(
            context=self._compositional_ctx,
            filter=f"function_id == {function_id}",
            exclude_fields=list_private_fields(self._compositional_ctx),
        )
        if len(logs) == 0:
            if raise_if_missing:
                raise ValueError(f"No function with id {function_id!r} exists.")
            return None
        assert len(logs) == 1, f"Multiple functions found with id {function_id!r}."
        return logs[0]

    # ------------------------------------------------------------------ #
    #  Verification ledger                                                #
    # ------------------------------------------------------------------ #

    @property
    def verification_settings(self) -> VerificationSettings:
        from unify.settings import SETTINGS

        return SETTINGS.function.verification

    # ------------------------------------------------------------------ #
    #  Activation: the usage trace behind memory-weighted retrieval       #
    # ------------------------------------------------------------------ #

    @property
    def activation_settings(self) -> "ActivationSettings":
        from unify.settings import SETTINGS

        return SETTINGS.function.activation

    def _note_function_use(self, func_data: Dict[str, Any]) -> None:
        """Record one invocation on the function's usage trace.

        Fire-and-forget off the caller's loop: metering must never slow or
        break execution, and a lost count only under-reports standing.
        The read-modify-write can race a concurrent call and drop a count —
        acceptable for a log-saturating signal. Primitives are platform
        surface, not library memory, and are never traced. The owning
        context comes from ``_federated_context`` (a row's trace must land
        on the root that holds the row — ``_context`` is never set; see the
        verification-fields callers that inherited that trap).
        """
        settings = self.activation_settings
        if not settings.enabled:
            return
        if func_data.get("is_primitive"):
            return
        fid = func_data.get("function_id")
        if fid is None:
            return
        ctx = func_data.get("_federated_context") or self._compositional_ctx
        now_iso = datetime.now(timezone.utc).isoformat()
        kept = settings.recent_calls_kept

        def _write() -> None:
            logs = db.get_logs(
                context=ctx,
                filter=f"function_id == {int(fid)}",
                from_fields=["function_id", "usage_calls", "usage_recent_calls"],
                limit=1,
            )
            if not logs:
                return
            entries = logs[0].entries or {}
            recents = list(entries.get("usage_recent_calls") or [])
            recents.append(now_iso)
            db.update_logs(
                logs=[logs[0].id],
                context=ctx,
                entries={
                    "usage_calls": int(entries.get("usage_calls") or 0) + 1,
                    "usage_last_called_at": now_iso,
                    "usage_recent_calls": recents[-kept:],
                },
                overwrite=True,
            )

        try:
            self._write_off_loop(_write, what=f"usage:{func_data.get('name')}")
        except Exception:  # noqa: BLE001 - metering must never break a call
            pass

    def _bump_search_hits(self, rows: List[Dict[str, Any]]) -> None:
        """Retrieved-but-never-called is a signal of its own; count it."""
        settings = self.activation_settings
        if not settings.enabled:
            return
        for row in rows:
            fid = row.get("function_id")
            if fid is None or row.get("is_primitive"):
                continue
            ctx = row.get("_federated_context") or self._compositional_ctx

            def _write(fid: int = int(fid), ctx: str = ctx) -> None:
                logs = db.get_logs(
                    context=ctx,
                    filter=f"function_id == {fid}",
                    from_fields=["function_id", "usage_search_hits"],
                    limit=1,
                )
                if not logs:
                    return
                entries = logs[0].entries or {}
                db.update_logs(
                    logs=[logs[0].id],
                    context=ctx,
                    entries={
                        "usage_search_hits": int(
                            entries.get("usage_search_hits") or 0,
                        )
                        + 1,
                    },
                    overwrite=True,
                )

            try:
                self._write_off_loop(_write, what=f"search_hit:{row.get('name')}")
            except Exception:  # noqa: BLE001 - metering must never break search
                pass

    def _stamp_new_function_usage(
        self,
        entry_data: Dict[str, Any],
        name: str,
    ) -> None:
        """Creation is the row's first activation event; a same-name
        delete-then-add (the librarian's supersede flow) inherits the
        deleted row's usage trace so the replacement stands where its
        predecessor stood."""
        entry_data["created_at"] = datetime.now(timezone.utc).isoformat()
        inherited = self._take_usage_legacy(name)
        if inherited:
            entry_data.update(
                {
                    "usage_calls": int(inherited.get("calls") or 0),
                    "usage_last_called_at": inherited.get("last_called_at"),
                    "usage_recent_calls": inherited.get("recent_calls") or [],
                    "usage_search_hits": int(inherited.get("search_hits") or 0),
                },
            )

    def _stash_usage_legacy(
        self,
        name: object,
        entries: Dict[str, Any],
    ) -> None:
        if not self.activation_settings.enabled or not name:
            return
        trace = {
            "calls": entries.get("usage_calls"),
            "last_called_at": entries.get("usage_last_called_at"),
            "recent_calls": entries.get("usage_recent_calls"),
            "search_hits": entries.get("usage_search_hits"),
        }
        if not any(v for v in trace.values()):
            return
        store: Dict[str, Dict[str, Any]] = getattr(self, "_usage_legacies", None) or {}
        store[str(name)] = merged_usage(
            store.get(str(name)),
            trace,
            self.activation_settings,
        )
        # Bounded, same-process-only memory: oldest entries fall off.
        while len(store) > 64:
            store.pop(next(iter(store)))
        self._usage_legacies = store

    def _take_usage_legacy(self, name: object) -> Optional[Dict[str, Any]]:
        store = getattr(self, "_usage_legacies", None)
        return store.pop(str(name), None) if store else None

    def _activation_rank(
        self,
        rows: List[Dict[str, Any]],
        *,
        n: int,
        include_dormant: bool,
    ) -> List[Dict[str, Any]]:
        """Order search results by similarity × standing; drop the lapsed.

        Similarity dominates (the activation term is capped in settings) and
        backfilled rows — which carry no score — keep their tail position.
        Primitives never drop out of scope: platform surface is not memory.
        Each surviving row is annotated with the components — ``_similarity``,
        ``_standing``, ``_retrieval_score`` — so the querying model sees WHY
        a result ranked where it did (relevant-but-lapsed reads differently
        from mediocre-but-battle-tested) instead of re-deriving the math.
        """
        settings = self.activation_settings
        if not settings.enabled:
            return rows[:n]
        now = datetime.now(timezone.utc)
        ranked: List[tuple[float, int, Dict[str, Any]]] = []
        for idx, row in enumerate(rows):
            standing = activation(
                now=now,
                created_at=row.get("created_at"),
                call_count=int(row.get("usage_calls") or 0),
                recent_calls=row.get("usage_recent_calls") or [],
                settings=settings,
            )
            if (
                not include_dormant
                and not row.get("is_primitive")
                and not in_scope(standing, settings)
            ):
                continue
            similarity = (
                similarity_from_distance(row.get(SCORE_FIELD))
                if SCORE_FIELD in row
                else 0.0
            )
            score = rank_score(similarity, standing, settings)
            row["_similarity"] = round(similarity, 4)
            row["_standing"] = round(standing, 4)
            row["_retrieval_score"] = round(score, 4)
            ranked.append((-score, idx, row))
        ranked.sort(key=lambda item: (item[0], item[1]))
        return [row for _, _, row in ranked[:n]]

    def function_trust_hash(self, fn: Dict[str, Any]) -> str:
        """Trust hash of a compositional row: source, dependency closure, venv, language."""
        return _function_trust_hash(
            fn,
            resolve_row=lambda name: self._get_function_data_by_name(name=name),
            resolve_venv=lambda venv_id: self.get_venv(venv_id=venv_id),
        )

    def _primitive_rows_by_name(self) -> Dict[str, Dict[str, Any]]:
        """Materialized primitive rows keyed by name, for integration action classes."""
        try:
            return {row["name"]: row for row in self._primitive_logs()}
        except Exception:
            return {}

    def classify_function(
        self,
        source: str,
        *,
        known_function_names: Set[str],
        primitive_rows: Optional[Dict[str, Dict[str, Any]]] = None,
        batch_classes: Optional[Dict[str, SideEffectClass]] = None,
    ) -> Classification:
        """Detect the effect-class lower bound of ``source`` from its AST.

        ``batch_classes`` supplies classes for dependencies stored in the same
        call, before their rows exist.
        """

        def _dependency_class(name: str) -> Optional[SideEffectClass]:
            if batch_classes and name in batch_classes:
                return batch_classes[name]
            row = self._get_function_data_by_name(name=name)
            if row is None:
                return None
            return SideEffectClass(
                str(row.get("side_effect_class") or SideEffectClass.unsafe_effectful),
            )

        return classify_source(
            source,
            known_function_names=known_function_names,
            dependency_class=_dependency_class,
            primitive_rows=(
                primitive_rows
                if primitive_rows is not None
                else self._primitive_rows_by_name()
            ),
        )

    def _verification_fields_for_source(
        self,
        *,
        source: str,
        fn_obj: Optional[Callable[..., Any]],
        known_function_names: Set[str],
        prior: Optional[Dict[str, Any]] = None,
        primitive_rows: Optional[Dict[str, Dict[str, Any]]] = None,
        authored_contract: Optional[Dict[str, Any]] = None,
        authored_fixtures: Optional[List[Dict[str, Any]]] = None,
        batch_classes: Optional[Dict[str, SideEffectClass]] = None,
    ) -> Dict[str, Any]:
        """Ledger fields for a row whose content is ``source``.

        A stored librarian confirmation survives while it still sits at or
        above the newly detected bound; the policy survives always (it only
        raises the bar); fixtures survive so they can be replayed against the
        new content; authored postconditions survive unless replaced. Everything
        hash-bound resets: the row starts on the ramp.
        """
        prior = prior or {}
        classification = self.classify_function(
            source,
            known_function_names=known_function_names,
            primitive_rows=primitive_rows,
            batch_classes=batch_classes,
        )
        confirmed: Optional[SideEffectClass] = None
        if prior.get("class_source") == "librarian" and prior.get("side_effect_class"):
            candidate = SideEffectClass(str(prior["side_effect_class"]))
            if candidate.rank >= classification.detected.rank:
                confirmed = candidate
        effective = effective_class(
            detected=classification.detected,
            source=classification.source,
            confirmed=confirmed,
        )
        hinted = (
            contract_from_callable(fn_obj) if fn_obj is not None else FunctionContract()
        )
        if authored_contract is None:
            prior_contract = prior.get("contract") or {}
            prior_postconditions = list(
                (
                    prior_contract.get("postconditions")
                    if isinstance(prior_contract, dict)
                    else None
                )
                or [],
            )
            authored_contract = (
                {"postconditions": prior_postconditions}
                if prior_postconditions
                else None
            )
        contract = merge_contract(hinted, authored_contract)
        settings = self.verification_settings
        stored_fixtures = list(prior.get("fixtures") or [])
        if authored_fixtures is not None:
            if effective is not SideEffectClass.safe_noop:
                raise ValueError(
                    "Fixtures are only meaningful for safe_noop functions; "
                    f"this function is {effective.value}.",
                )
            authored = coerce_fixtures(
                authored_fixtures,
                max_bytes=settings.max_fixture_bytes,
            )
            merged: Dict[str, Dict[str, Any]] = {
                item.get("args_signature"): item for item in stored_fixtures
            }
            for fixture in authored:
                merged[fixture.args_signature] = fixture.model_dump(mode="json")
            stored_fixtures = list(merged.values())[
                -settings.max_fixtures_per_function :
            ]
        if effective is not SideEffectClass.safe_noop:
            # Fixtures record pure computation only; the world moved for every
            # other class, so replaying them would prove nothing.
            stored_fixtures = []
        return {
            "side_effect_class_detected": classification.detected.value,
            "side_effect_class": effective.value,
            "class_source": (
                "librarian" if confirmed is not None else classification.source
            ),
            "class_rationale": (
                prior.get("class_rationale") if confirmed is not None else None
            ),
            "verification_policy": dict(prior.get("verification_policy") or {}),
            "verified_hash": None,
            "static_review": None,
            "ledger": VerificationSummary().model_dump(mode="json"),
            "contract": contract.model_dump(mode="json"),
            "fixtures": stored_fixtures,
            "verify": True,
        }

    def _replay_fixtures_for_entry(
        self,
        *,
        name: str,
        entry: Dict[str, Any],
        namespace: Dict[str, Any],
    ) -> List[str]:
        """Replay a ``safe_noop`` entry's fixtures through its new implementation.

        Any mismatch raises ``FixtureRegressionError`` before the row is
        written. When every fixture passes, the ledger is seeded with one
        ``tier0`` pass per fixture under the new trust hash, so a pure function
        that still reproduces its recorded behaviour needs no live runs beyond
        its static review to be trusted again.
        """
        fixtures = [
            Fixture.model_validate(item) for item in entry.get("fixtures") or []
        ]
        if (
            not fixtures
            or entry.get("side_effect_class") != SideEffectClass.safe_noop.value
        ):
            return []
        self._inject_dependencies(
            {"name": name, "depends_on": entry.get("depends_on") or []},
            namespace=namespace,
            visited={name},
        )
        fn_obj = namespace.get(name)
        if not callable(fn_obj):
            return []

        from unify.common.asyncio_compat import run_coro_sync

        replayed = run_coro_sync(
            lambda: replay_fixtures(fixtures, fn_obj, function_name=name),
        )
        probe_row = {
            "name": name,
            "implementation": entry.get("implementation"),
            "depends_on": entry.get("depends_on") or [],
            "language": entry.get("language") or "python",
            "venv_id": entry.get("venv_id"),
        }
        entry["verified_hash"] = self.function_trust_hash(probe_row)
        entry["ledger"] = VerificationSummary(
            passes={VerdictKind.tier0.value: replayed},
            distinct_args_signatures=[fixture.args_signature for fixture in fixtures],
        ).model_dump(mode="json")
        return [fixture.args_signature for fixture in fixtures]

    @staticmethod
    def _order_batch_by_dependencies(
        parsed: List[Tuple[str, ast.Module, ast.FunctionDef, str]],
        known_function_names: Set[str],
    ) -> List[Tuple[str, ast.Module, ast.FunctionDef, str]]:
        """Return ``parsed`` with same-batch dependencies before their dependents."""
        by_name = {item[0]: item for item in parsed}
        deps: Dict[str, Set[str]] = {}
        for name, _tree, node, _source in parsed:
            found = collect_dependencies_from_function_node(
                node,
                known_function_names,
                environment_namespaces=frozenset({"primitives"}),
            )
            deps[name] = {d for d in found if d in by_name and d != name}
        ordered: List[Tuple[str, ast.Module, ast.FunctionDef, str]] = []
        placed: Set[str] = set()

        def _place(name: str, stack: Set[str]) -> None:
            if name in placed or name in stack:
                return
            stack.add(name)
            for dep in sorted(deps.get(name, ())):
                _place(dep, stack)
            stack.discard(name)
            placed.add(name)
            ordered.append(by_name[name])

        for name, _tree, _node, _source in parsed:
            _place(name, set())
        return ordered

    @staticmethod
    def _unclassifiable_verification_fields() -> Dict[str, Any]:
        """Ledger fields for a row whose source cannot be analysed: unsafe until confirmed."""
        return {
            "side_effect_class_detected": SideEffectClass.unsafe_effectful.value,
            "side_effect_class": SideEffectClass.unsafe_effectful.value,
            "class_source": "inferred_third_party",
            "class_rationale": None,
            "verification_policy": {},
            "verified_hash": None,
            "static_review": None,
            "ledger": VerificationSummary().model_dump(mode="json"),
            "contract": FunctionContract().model_dump(mode="json"),
            "fixtures": [],
            "verify": True,
        }

    def _hydrate_verification_fields(
        self,
        rows: List[Dict[str, Any]],
        *,
        default_context: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Backfill ledger fields on compositional rows written before they existed.

        Idempotent: a row that already carries ``side_effect_class`` is left
        untouched. A row lacking it is classified from its stored source, the
        fields are persisted on the row, and the in-memory dict is updated so
        every reader sees a classified row.
        """
        pending = [
            row
            for row in rows
            if isinstance(row, dict)
            and not row.get("is_primitive")
            and row.get("side_effect_class") is None
            and row.get("function_id") is not None
        ]
        if not pending:
            return rows
        known_names: Set[str] = set()
        for row in rows:
            name = row.get("name") if isinstance(row, dict) else None
            if isinstance(name, str):
                known_names.add(name)
        primitive_rows = self._primitive_rows_by_name()
        for row in pending:
            source = row.get("implementation")
            if not isinstance(source, str) or not source.strip():
                continue
            fields = self._unclassifiable_verification_fields()
            if str(row.get("language") or "python") == "python":
                stripped = source
                fn_obj: Any = None
                try:
                    namespace = create_base_globals()
                    self._inject_forward_ref_annotation_placeholders(
                        stripped,
                        namespace=namespace,
                    )
                    exec(stripped, namespace)
                    fn_obj = namespace.get(str(row.get("name")))
                except Exception:
                    fn_obj = None
                try:
                    fields = self._verification_fields_for_source(
                        source=stripped,
                        fn_obj=fn_obj if callable(fn_obj) else None,
                        known_function_names=known_names,
                        prior=row,
                        primitive_rows=primitive_rows,
                    )
                except (SyntaxError, ValueError):
                    logger.warning(
                        "Function %r could not be classified from its stored source; "
                        "treating it as unsafe_effectful.",
                        row.get("name"),
                    )
            context = row.get("_context") or default_context or self._compositional_ctx
            self._persist_verification_fields(
                function_id=int(row["function_id"]),
                fields=fields,
                context=str(context),
            )
            row.update(fields)
        return rows

    def _persist_verification_fields(
        self,
        *,
        function_id: int,
        fields: Dict[str, Any],
        context: Optional[str] = None,
    ) -> None:
        """Write ledger-owned fields onto the row with ``function_id``."""
        ctx = context or self._compositional_ctx
        logs = db.get_logs(
            context=ctx,
            filter=f"function_id == {int(function_id)}",
            from_fields=["function_id"],
            limit=1,
        )
        if not logs:
            raise ValueError(f"No function with id {function_id!r} exists in {ctx}.")
        db.update_logs(
            logs=[logs[0].id],
            context=ctx,
            entries=dict(fields),
            overwrite=True,
        )

    def record_verification(self, row: VerificationRow) -> None:
        """Append one verdict row to ``Functions/Verifications`` and refold the ledger.

        The append-only rows are the source of truth; the summary on the
        function row is recomputed from them for the current trust hash after
        every write, and ``verify`` is derived from that summary. Verdicts
        recorded against content that has since changed are kept as history
        but never counted.
        """
        payload = row.model_dump(mode="json")
        if payload.get("created_at") is None:
            payload["created_at"] = datetime.now(timezone.utc).isoformat()
        unity_create_logs(
            context=self._verifications_ctx,
            entries=[payload],
            stamp_authoring=False,
        )
        self.refresh_trust(int(row.function_id))

    _refold_lock = threading.Lock()

    def refresh_trust(self, function_id: int) -> Optional[bool]:
        """Recompute the ledger fold and ``verify`` for one function from its rows.

        Returns the derived ``verify`` value, or None when the row is gone.
        Serialised per process so two verdicts landing together cannot lose a
        FAIL between read and write.
        """
        with self._refold_lock:
            log = self._get_log_by_function_id(
                function_id=function_id,
                raise_if_missing=False,
            )
            if log is None:
                return None
            fn = dict(log.entries)
            if fn.get("is_primitive"):
                return None
            current = self.function_trust_hash(fn)
            rows = [
                VerificationRow.model_validate(entry)
                for entry in self.list_verifications(
                    function_id=function_id,
                    function_hash=current,
                    limit=1000,
                )
            ]
            summary = fold_rows(rows)
            fn["ledger"] = summary.model_dump(mode="json")
            fn["verified_hash"] = (
                current
                if rows or fn.get("verified_hash") == current
                else fn.get("verified_hash")
            )
            if fn.get("verified_hash") != current and rows:
                fn["verified_hash"] = current
            static = fn.get("static_review")
            stale_static = (
                isinstance(static, dict) and static.get("function_hash") != current
            )
            if stale_static:
                fn["static_review"] = None
            verify = derive_verify(
                fn,
                settings=self.verification_settings,
                current_hash=current,
            )
            updates = {
                "ledger": fn["ledger"],
                "verified_hash": fn.get("verified_hash"),
                "verify": verify,
            }
            if stale_static:
                # Written only to clear a stale cache: echoing the read value
                # back would clobber a static-review persist landing between
                # this fold's read and its write.
                updates["static_review"] = None
            db.update_logs(
                logs=[log.id],
                context=self._compositional_ctx,
                entries=updates,
                overwrite=True,
            )
            return verify

    def _invalidation_fields(self) -> Dict[str, Any]:
        return {
            "verified_hash": None,
            "static_review": None,
            "ledger": VerificationSummary().model_dump(mode="json"),
            "verify": True,
        }

    def invalidate_trust(
        self,
        function_ids: Iterable[int],
        *,
        stale_reason: Optional[StaleReason] = None,
    ) -> List[int]:
        """Put functions (and everything that depends on them) back on the ramp.

        Returns the ids invalidated, dependents included. History rows are
        kept; the summary simply points at no hash until new evidence lands.
        """
        seed = {int(fid) for fid in function_ids}
        if not seed:
            return []
        all_logs = db.get_logs(
            context=self._compositional_ctx,
            exclude_fields=list_private_fields(self._compositional_ctx),
        )
        by_id: Dict[int, Any] = {}
        dependents: Dict[str, List[int]] = {}
        names: Dict[int, str] = {}
        for log in all_logs:
            entries = log.entries or {}
            fid = entries.get("function_id")
            if fid is None:
                continue
            by_id[int(fid)] = log
            names[int(fid)] = str(entries.get("name") or "")
            for dep in entries.get("depends_on") or []:
                if isinstance(dep, str) and "." not in dep:
                    dependents.setdefault(dep, []).append(int(fid))
        targets: set[int] = set()
        queue = list(seed)
        while queue:
            fid = queue.pop()
            if fid in targets or fid not in by_id:
                continue
            targets.add(fid)
            queue.extend(dependents.get(names[fid], []))
        for fid in targets:
            log = by_id[fid]
            fields = self._invalidation_fields()
            if stale_reason is not None and fid in seed:
                existing = coerce_stale_reasons(log.entries.get("stale_reasons") or [])
                fields["stale_reasons"] = [
                    reason.model_dump(mode="json")
                    for reason in merge_stale_reasons(existing, stale_reason)
                ]
            db.update_logs(
                logs=[log.id],
                context=self._compositional_ctx,
                entries=fields,
                overwrite=True,
            )
        return sorted(targets)

    def invalidate_trust_for_guidance(self, guidance_id: int) -> List[int]:
        """A linked guidance entry changed or vanished: its functions go back on the ramp."""
        gid = int(guidance_id)
        logs = db.get_logs(
            context=self._compositional_ctx,
            filter=f"{gid} in guidance_ids",
            from_fields=["function_id"],
        )
        ids = {
            int(log.entries["function_id"])
            for log in logs
            if log.entries.get("function_id") is not None
        }
        # The guidance row's own function_ids are the authored side of the link.
        gctx = self._guidance_context()
        guidance_rows = db.get_logs(
            context=gctx,
            filter=f"guidance_id == {gid}",
            limit=1,
            exclude_fields=list_private_fields(gctx),
        )
        for row in guidance_rows:
            for fid in row.entries.get("function_ids") or []:
                ids.add(int(fid))
        if not ids:
            return []
        return self.invalidate_trust(
            ids,
            stale_reason=StaleReason(
                kind="guidance_changed",
                dep_kind="guidance",
                id=gid,
                message=f"linked guidance_id={gid} changed; trust must be re-earned",
            ),
        )

    def invalidate_trust_for_venv(self, venv_id: int) -> List[int]:
        """The venv content changed: every function running in it goes back on the ramp."""
        logs = db.get_logs(
            context=self._compositional_ctx,
            filter=f"venv_id == {int(venv_id)}",
            from_fields=["function_id"],
        )
        ids = [
            int(log.entries["function_id"])
            for log in logs
            if log.entries.get("function_id") is not None
        ]
        return self.invalidate_trust(ids) if ids else []

    def _invalidate_dependents_of(self, names: Iterable[str]) -> List[int]:
        """Functions depending (transitively) on any of ``names`` go back on the ramp."""
        wanted = {str(n) for n in names}
        if not wanted:
            return []
        all_logs = db.get_logs(
            context=self._compositional_ctx,
            from_fields=["function_id", "name", "depends_on"],
        )
        seed: set[int] = set()
        for log in all_logs:
            entries = log.entries or {}
            deps = {d for d in (entries.get("depends_on") or []) if isinstance(d, str)}
            if deps & wanted and entries.get("function_id") is not None:
                seed.add(int(entries["function_id"]))
        return self.invalidate_trust(seed) if seed else []

    def _write_off_loop(
        self,
        fn: Callable[[], None],
        *,
        what: str,
    ) -> "concurrent.futures.Future[None]":
        """Run a ledger write without blocking the caller's event loop.

        Bounded retry: one immediate retry, then the failure is logged. A
        lost row only delays trust; it never grants it. The returned future
        resolves once the attempt (including its retry) has finished,
        carrying the final failure if the write was lost; fire-and-forget
        callers ignore it.
        """
        done: "concurrent.futures.Future[None]" = concurrent.futures.Future()

        def _attempt() -> None:
            for attempt in (1, 2):
                try:
                    fn()
                    done.set_result(None)
                    return
                except Exception as exc:
                    if attempt == 2:
                        logger.warning("%s failed after retry: %s", what, exc)
                        done.set_exception(exc)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            _attempt()
            return done
        loop.run_in_executor(None, _attempt)
        return done

    def record_verification_nowait(
        self,
        row: VerificationRow,
    ) -> "concurrent.futures.Future[None]":
        """Append a verdict row without awaiting the write."""
        return self._write_off_loop(
            lambda: self.record_verification(row),
            what=f"Verification row write for function {row.function_id}",
        )

    def capture_fixture_nowait(
        self,
        fn: Dict[str, Any],
        fields: Dict[str, Any],
    ) -> "concurrent.futures.Future[None]":
        """Persist captured fixtures for ``fn`` without awaiting the write."""
        function_id = int(fn["function_id"])
        context = fn.get("_context")
        return self._write_off_loop(
            lambda: self._persist_verification_fields(
                function_id=function_id,
                fields=fields,
                context=str(context) if context else None,
            ),
            what=f"Fixture capture for function {function_id}",
        )

    def persist_static_review_nowait(
        self,
        fn: Dict[str, Any],
        record: StaticReviewRecord,
    ) -> "concurrent.futures.Future[None]":
        """Persist a static-review verdict onto the row without awaiting the write."""
        function_id = int(fn["function_id"])
        context = fn.get("_context")
        return self._write_off_loop(
            lambda: self._persist_verification_fields(
                function_id=function_id,
                fields={"static_review": record.model_dump(mode="json")},
                context=str(context) if context else None,
            ),
            what=f"Static review write for function {function_id}",
        )

    def _tier0_checker(
        self,
        func_data: Dict[str, Any],
        *,
        call_site: str = "root",
    ) -> Tier0Checker:
        return Tier0Checker(row=func_data, writer=self, call_site=call_site)

    def _boundary(
        self,
        raw: Callable[..., Any],
        func_data: Dict[str, Any],
    ) -> Callable[..., Any]:
        """The namespace-facing callable for a compositional function.

        Lineage tracking sits inside; tier-0 contract checks and fixture
        capture sit outside so every call site — a plan namespace or a
        symbolic closure — pays the same deterministic check.
        """
        name = str(func_data.get("name"))
        inner = raw
        if not isinstance(inner, _LineageTrackedFunction):
            inner = _LineageTrackedFunction(
                raw,
                name,
                on_call=lambda: self._note_function_use(func_data),
            )
        underlying = getattr(raw, "__wrapped__", raw)
        if isinstance(underlying, _VenvFunctionProxy):
            # Venv proxies carry no Python signature; derive one from the source.
            guarded = tier0_boundary(
                inner,
                raw=None,
                checker=self._tier0_checker(func_data),
                signature=signature_from_source(func_data.get("implementation")),
            )
        else:
            guarded = tier0_boundary(
                inner,
                raw=underlying if callable(underlying) else None,
                checker=self._tier0_checker(func_data),
            )
        return guarded

    def list_verifications(
        self,
        *,
        function_id: int,
        function_hash: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Verdict rows for one function, newest last."""
        clauses = [f"function_id == {int(function_id)}"]
        if function_hash is not None:
            clauses.append(f"function_hash == {json.dumps(function_hash)}")
        logs = db.get_logs(
            context=self._verifications_ctx,
            filter=" and ".join(clauses),
            limit=limit,
        )
        rows = [lg.entries for lg in logs]
        rows.sort(key=lambda entry: str(entry.get("created_at") or ""))
        return rows

    def derive_verify_for_row(self, row: Dict[str, Any]) -> bool:
        """``verify`` for a compositional row under the current settings and content."""
        return derive_verify(
            row,
            settings=self.verification_settings,
            current_hash=self.function_trust_hash(row),
        )

    def confirm_side_effect_class(
        self,
        *,
        function_id: int,
        side_effect_class: str,
        rationale: str,
    ) -> Dict[str, Any]:
        """Confirm, raise, or lower a stored function's effect class.

        The effect class decides how much independent verification a stored
        function needs before it is trusted and whether its calls must wait
        for earlier verdicts. Detection from the source is a lower bound:
        ``safe_noop`` (pure computation, no I/O beyond its arguments) <
        ``read_only`` (reads external state, mutates nothing) <
        ``idempotent_effectful`` (mutates, but re-running with the same inputs
        converges to the same state: upsert by key, write a file at a path,
        set a field) < ``unsafe_effectful`` (mutates non-idempotently or
        irreversibly: send, delete, pay, post).

        You may raise the class freely — do so whenever the docstring or the
        trajectory shows an effect the source alone does not reveal. You may
        lower it only down to the detected bound: a class below what the
        source proves is rejected, not clamped. Confirming a class that
        detection inferred from third-party imports replaces the safe
        default (unsafe until confirmed) with your judgement, so confirm only
        what you can defend in ``rationale``. Confirmation never grants trust;
        the function still earns it from verdicts against the confirmed bar.

        Returns a dict with ``outcome`` (``confirmed`` or ``rejected``), the
        ``detected`` bound, the ``effective`` class after the call and, when
        rejected, ``reason``.
        """
        log = self._get_log_by_function_id(function_id=int(function_id))
        row = dict(log.entries)
        try:
            requested = SideEffectClass(str(side_effect_class))
        except ValueError:
            return {
                "outcome": "rejected",
                "function_id": int(function_id),
                "reason": (
                    f"Unknown side_effect_class {side_effect_class!r}; choose one of "
                    f"{[c.value for c in SideEffectClass]}."
                ),
            }
        detected = SideEffectClass(
            str(
                row.get("side_effect_class_detected")
                or SideEffectClass.unsafe_effectful,
            ),
        )
        if requested.rank < detected.rank:
            return {
                "outcome": "rejected",
                "function_id": int(function_id),
                "detected": detected.value,
                "effective": row.get("side_effect_class"),
                "reason": (
                    f"The source proves at least {detected.value}; a class below the "
                    "detected bound cannot be confirmed."
                ),
            }
        rationale_text = str(rationale or "").strip()
        if not rationale_text:
            return {
                "outcome": "rejected",
                "function_id": int(function_id),
                "reason": "A rationale is required when confirming an effect class.",
            }
        db.update_logs(
            logs=[log.id],
            context=self._compositional_ctx,
            entries={
                "side_effect_class": requested.value,
                "class_source": "librarian",
                "class_rationale": rationale_text[:2000],
            },
            overwrite=True,
        )
        verify = self.refresh_trust(int(function_id))
        return {
            "outcome": "confirmed",
            "function_id": int(function_id),
            "detected": detected.value,
            "effective": requested.value,
            "verify": verify,
        }

    def set_verification_policy(
        self,
        *,
        function_id: int,
        always_verify: Optional[bool] = None,
        required_passes: Optional[int] = None,
        min_distinct_inputs: Optional[int] = None,
        fixture_only: Optional[bool] = None,
        spot_check_rate: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Raise the verification bar for a stored function; never lower it.

        Trust is earned by policy from independent verdicts. These knobs let
        you demand more than the class default when a function is unusually
        consequential or its inputs unusually varied: ``always_verify`` keeps
        every call under verification forever; ``required_passes`` and
        ``min_distinct_inputs`` raise how many passing verdicts, and across how
        many distinct inputs, are needed before trust; ``spot_check_rate``
        raises how often a trusted effectful call is re-checked afterwards.
        ``fixture_only`` records that a ``safe_noop`` function is judged by its
        recorded fixtures and deterministic contract alone (which is already
        how pure functions are trusted).

        Every argument may only move the bar up: a value at or below the
        class default, or below the current policy, is rejected. Omit what you
        do not want to change. Trust can never be granted here; only made
        harder to earn.
        """
        from .verification.policy import (
            min_distinct_inputs as _class_min_inputs,
            required_passes as _class_required_passes,
            spot_check_rate as _class_spot_rate,
        )

        log = self._get_log_by_function_id(function_id=int(function_id))
        row = dict(log.entries)
        settings = self.verification_settings
        current = VerificationPolicy.model_validate(
            row.get("verification_policy") or {},
        )
        base_row = {**row, "verification_policy": {}}
        rejections: List[str] = []
        updated = current.model_copy()

        if always_verify is not None:
            if always_verify is False and current.always_verify:
                rejections.append("always_verify cannot be switched off once set.")
            elif always_verify:
                updated.always_verify = True
        if required_passes is not None:
            floor = max(
                _class_required_passes(base_row, settings),
                current.required_passes or 0,
            )
            if int(required_passes) <= floor:
                rejections.append(
                    f"required_passes must exceed the current bar ({floor}); {required_passes} does not raise it.",
                )
            else:
                updated.required_passes = int(required_passes)
        if min_distinct_inputs is not None:
            floor = max(
                _class_min_inputs(base_row, settings),
                current.min_distinct_inputs or 0,
            )
            if int(min_distinct_inputs) <= floor:
                rejections.append(
                    f"min_distinct_inputs must exceed the current bar ({floor}); {min_distinct_inputs} does not raise it.",
                )
            else:
                updated.min_distinct_inputs = int(min_distinct_inputs)
        if spot_check_rate is not None:
            floor = max(
                _class_spot_rate(base_row, settings),
                current.spot_check_rate or 0.0,
            )
            if not (0.0 <= float(spot_check_rate) <= 1.0):
                rejections.append("spot_check_rate must be between 0 and 1.")
            elif float(spot_check_rate) <= floor:
                rejections.append(
                    f"spot_check_rate must exceed the current rate ({floor}); {spot_check_rate} does not raise it.",
                )
            else:
                updated.spot_check_rate = float(spot_check_rate)
        if fixture_only is not None:
            if row.get("side_effect_class") != SideEffectClass.safe_noop.value:
                rejections.append("fixture_only applies to safe_noop functions only.")
            elif fixture_only is False and current.fixture_only:
                rejections.append("fixture_only cannot be switched off once set.")
            elif fixture_only:
                updated.fixture_only = True
        if rejections:
            return {
                "outcome": "rejected",
                "function_id": int(function_id),
                "reasons": rejections,
                "policy": current.model_dump(mode="json"),
            }
        if updated == current:
            return {
                "outcome": "unchanged",
                "function_id": int(function_id),
                "policy": current.model_dump(mode="json"),
            }
        db.update_logs(
            logs=[log.id],
            context=self._compositional_ctx,
            entries={"verification_policy": updated.model_dump(mode="json")},
            overwrite=True,
        )
        verify = self.refresh_trust(int(function_id))
        return {
            "outcome": "raised",
            "function_id": int(function_id),
            "policy": updated.model_dump(mode="json"),
            "verify": verify,
        }

    # ------------------------------------------------------------------ #
    #  Public API                                                        #
    # ------------------------------------------------------------------ #

    def warm_embeddings(self) -> None:
        for ctx in (self._compositional_ctx, self._primitives_ctx):
            try:
                ensure_vector_column(
                    ctx,
                    embed_column="_embedding_text_emb",
                    source_column="embedding_text",
                )
            except Exception:
                pass

    @functools.wraps(BaseFunctionManager.clear, updated=())
    def clear(self) -> None:
        db.delete_context(self._compositional_ctx)
        db.delete_context(self._primitives_ctx)
        db.delete_context(self._venvs_ctx)
        db.delete_context(self._meta_ctx)

        # Reset any manager-local counters or caches
        try:
            self._next_id = None
            # Clear in-process session state
            self._in_process_sessions.clear()
        except Exception:
            pass

        # Force re-provisioning
        ContextRegistry.refresh(self, "Functions/VirtualEnvs")
        ContextRegistry.refresh(self, "Functions/Compositional")
        ContextRegistry.refresh(self, "Functions/Primitives")
        ContextRegistry.refresh(self, "Functions/Meta")

        # Verify visibility before proceeding
        try:
            import time as _time  # local import to avoid polluting module namespace

            for _ in range(3):
                try:
                    db.get_fields(context=self._compositional_ctx)
                    break
                except Exception:
                    _time.sleep(0.05)
        except Exception:
            pass

    def clear_in_process_sessions(self, session_id: Optional[int] = None) -> None:
        """
        Clear in-process session state.

        Parameters
        ----------
        session_id : int | None, default ``None``
            If provided, clear only the specified session. If None, clear all sessions.
        """
        if session_id is not None:
            self._in_process_sessions.pop(session_id, None)
        else:
            self._in_process_sessions.clear()

    # ------------------------------------------------------------------ #
    #  Primitives sync                                                   #
    # ------------------------------------------------------------------ #

    def _get_stored_hash_map(self, field_name: str) -> Dict[str, str]:
        """Read a hash map field from the singleton Functions/Meta row."""

        try:
            logs = db.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                return logs[0].entries.get(field_name, {}) or {}
        except Exception:
            pass
        return {}

    def _store_hash_map(self, field_name: str, hashes: Dict[str, str]) -> None:
        """Store a hash map field on the singleton Functions/Meta row."""

        try:
            logs = db.get_logs(
                context=self._meta_ctx,
                filter="meta_id == 1",
                limit=1,
            )
            if logs:
                db.update_logs(
                    logs=[logs[0].id],
                    context=self._meta_ctx,
                    entries={field_name: hashes},
                    overwrite=True,
                )
            else:
                unity_create_logs(
                    context=self._meta_ctx,
                    entries=[
                        {"meta_id": 1, field_name: hashes},
                    ],
                    stamp_authoring=True,
                )
        except Exception as e:
            logger.warning("Failed to store %s hash map: %s", field_name, e)

    @staticmethod
    def _compact_function_search_rows(
        rows: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Return actor-facing discovery rows without large structured payloads."""

        compact_rows: list[dict[str, Any]] = []
        for row in rows:
            compact = {
                key: value for key, value in row.items() if key != "implementation"
            }
            if compact.get("is_primitive"):
                # Primitive docstrings are full manual pages; discovery
                # results must not re-import what the actor prompt
                # deliberately leaves out. Summary + params is enough to
                # call the method.
                doc = str(compact.get("docstring") or "")
                if doc:
                    summary = get_registry()._extract_summary_and_params(doc)
                    compact["docstring"] = summary or doc[:800]
                compact.pop("embedding_text", None)
            compact_rows.append(compact)
        return compact_rows

    def _delete_primitives_by_function_ids(self, function_ids: list[int]) -> None:
        if not function_ids:
            return
        ids = sorted(set(function_ids))
        filter_expr = (
            f"function_id == {ids[0]}"
            if len(ids) == 1
            else f"function_id in [{', '.join(str(function_id) for function_id in ids)}]"
        )
        logs = db.get_logs(
            context=self._primitives_ctx,
            filter=filter_expr,
            exclude_fields=list_private_fields(self._primitives_ctx),
        )
        if logs:
            db.delete_logs(
                context=self._primitives_ctx,
                logs=[log.id for log in logs],
            )

    def _insert_primitives(self, primitives: List[Dict[str, Any]]) -> bool:
        """Insert primitive rows into the Primitives context with explicit IDs.

        Provider-backed primitive rows are connection-agnostic catalogue
        entries (see ``sync_provider_integration_tools``): the same tool
        definition is shared, not duplicated per assistant, so it may already
        exist (e.g. seeded once system-wide) even on an assistant's first
        sync. The delete above can't remove a row this session doesn't own,
        so re-inserting it would otherwise collide on the ``function_id``
        unique key. ``on_duplicate="skip"`` inserts whichever rows are
        genuinely new and leaves already-catalogued rows alone instead of
        failing the whole batch on the first collision.

        Returns ``True`` only when every row was freshly written by this
        call. ``False`` means one or more rows already existed under their
        ``function_id`` and were left untouched -- this session can't
        confirm their content still matches what was requested (it may be
        stale, or two distinct tools may have collided on the same
        function_id hash), so callers must not cache "fully synced" state
        for a ``False`` result; a future sync should keep retrying instead
        of silently trusting a row it never actually wrote.
        """
        if not primitives:
            return True

        entries = [
            Function.model_validate(data).model_dump(include=set(data.keys()))
            for data in primitives
        ]

        try:
            self._delete_primitives_by_function_ids(
                [
                    entry["function_id"]
                    for entry in entries
                    if isinstance(entry.get("function_id"), int)
                ],
            )
            created = unity_create_logs(
                context=self._primitives_ctx,
                entries=entries,
                stamp_authoring=True,
                on_duplicate="skip",
            )
            written_ids = (
                {log.entries.get("function_id") for log in created}
                if isinstance(created, list)
                else {entry.get("function_id") for entry in entries}
            )
            skipped = [
                (entry.get("function_id"), entry.get("name"))
                for entry in entries
                if entry.get("function_id") not in written_ids
            ]
            if skipped:
                # Routine and expected: a non-owning assistant's first sync
                # of an app the shared catalogue already has (see docstring)
                # skips every time, forever, since the hash below is
                # deliberately never cached for it -- info, not warning, to
                # avoid paging on the steady-state case. A genuine
                # function_id collision between two *different* tools would
                # show up here as the same function_id recurring under a
                # different name across log lines -- greppable, unlike a
                # bare count.
                logger.info(
                    "Provider primitive insert: %d/%d already catalogued "
                    "under an existing function_id, left in place: %s",
                    len(skipped),
                    len(entries),
                    sorted(skipped),
                )
            else:
                logger.debug(f"Inserted {len(entries)} primitives")
            return not skipped
        except Exception as e:
            logger.error(f"Failed to insert primitives: {e}")
            raise

    # ------------------------------------------------------------------ #
    #  Custom Functions Sync                                              #
    # ------------------------------------------------------------------ #

    def _derived_verification_fields_for_row(
        self,
        data: Dict[str, Any],
        *,
        prior: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Ledger fields derived from a row dict that carries its implementation."""
        source = data.get("implementation")
        if not isinstance(source, str) or not source.strip():
            return self._unclassifiable_verification_fields()
        if str(data.get("language") or "python") != "python":
            return self._unclassifiable_verification_fields()
        stripped = source
        fn_obj: Any = None
        try:
            namespace = create_base_globals()
            self._inject_forward_ref_annotation_placeholders(
                stripped,
                namespace=namespace,
            )
            exec(stripped, namespace)
            fn_obj = namespace.get(str(data.get("name")))
        except Exception:
            fn_obj = None
        try:
            return self._verification_fields_for_source(
                source=stripped,
                fn_obj=fn_obj if callable(fn_obj) else None,
                known_function_names=self._available_dependency_names(),
                prior=prior,
            )
        except (SyntaxError, ValueError):
            return self._unclassifiable_verification_fields()

    # ------------------------------------------------------------------ #
    #  Custom Venvs Sync                                                  #
    # ------------------------------------------------------------------ #

    def list_primitives(self) -> Dict[str, Dict[str, Any]]:
        """
        Return a mapping of primitive name to primitive metadata.

        Only returns primitives for managers in this FunctionManager's scope,
        combining the global builtins catalogue with materialized
        provider-backed integration tool rows.

        Returns:
            Dict mapping primitive name to metadata dict (includes function_id).
        """
        entries: Dict[str, Dict[str, Any]] = {}
        try:
            for row in self._primitive_logs():
                data = {
                    "function_id": row.get("function_id"),
                    "name": row["name"],
                    "argspec": row.get("argspec", ""),
                    "docstring": row.get("docstring", ""),
                    "is_primitive": True,
                    "primitive_class": row.get("primitive_class"),
                    "primitive_method": row.get("primitive_method"),
                }
                for key in ("metadata",):
                    if key in row:
                        data[key] = row.get(key)
                entries.setdefault(row["name"], data)
        except Exception as e:
            logger.warning(f"Failed to list primitives: {e}")
        return entries

    # 1. Add / register ------------------------------------------------- #

    @functools.wraps(BaseFunctionManager.add_functions, updated=())
    def add_functions(
        self,
        *,
        implementations: Union[str, List[str]],
        language: Literal["python", "bash", "zsh", "sh", "powershell"] = "python",
        preconditions: Optional[Dict[str, Dict]] = None,
        contracts: Optional[Dict[str, Dict[str, Any]]] = None,
        fixtures: Optional[Dict[str, List[Dict[str, Any]]]] = None,
        overwrite: bool = False,
        raise_on_error: bool = True,
        venv_id: Optional[int] = None,
    ) -> Dict[str, str]:
        """
        Add or update functions in batch.

        Args:
            implementations: Function source code (single string or list of strings).
            language: The language/interpreter for the function(s). Default is "python".
            preconditions: Optional preconditions for functions.
            contracts: Optional per-function contract additions (postconditions).
            fixtures: Optional per-function recorded (args, result) pairs.
            overwrite: If True, update existing functions; if False, skip duplicates.
            raise_on_error: If True (default), raise ValueError when any function
                fails to add. If False, errors are returned in the result dict.
            venv_id: Virtual environment to associate with the functions. Required
                when any function imports third-party packages.

        Returns:
            Dictionary mapping function names to status ("added", "updated", "skipped", or "error").

        Raises:
            ValueError: If raise_on_error=True and any function fails to add,
                or if third-party imports are detected without a venv_id.
        """

        if preconditions is None:
            preconditions = {}
        if contracts is None:
            contracts = {}
        if fixtures is None:
            fixtures = {}
        if isinstance(implementations, str):
            implementations = [implementations]

        # Branch based on language
        if language != "python":
            return self._add_shell_functions(
                implementations=implementations,
                language=language,
                preconditions=preconditions,
                overwrite=overwrite,
                raise_on_error=raise_on_error,
            )

        # Python-specific parsing and validation
        parsed: List[Tuple[str, ast.Module, ast.FunctionDef, str]] = []
        parse_errors: Dict[str, str] = {}
        temp_names: Set[str] = set()

        # Parse all implementations
        for i, source in enumerate(implementations):
            try:
                # _parse_implementation validates basic structure (one func at col 0)
                name, tree, node, src = self._parse_implementation(source)
                parsed.append((name, tree, node, src))
                temp_names.add(name)
            except ValueError as e:
                # Associate error with name or index
                potential_name = f"implementation_{i+1}"
                try:
                    name_in_error = ast.parse(source).body[0].name
                except:
                    name_in_error = None
                key = name_in_error or potential_name
                parse_errors[key] = f"error: {e}"

        results: Dict[str, str] = parse_errors
        fixture_regressions: List[FixtureRegressionError] = []

        # Get existing functions for duplicate detection and dependency checking
        try:
            existing_functions = self.list_functions()
            existing_names = set(existing_functions.keys())
            all_known_function_names = existing_names.union(temp_names)
        except Exception as e:
            logger.warning(
                f"Failed to list existing functions for dependency check: {e}",
            )
            existing_functions = {}
            existing_names = set()
            all_known_function_names = temp_names
        primitive_rows = {
            name: data
            for name, data in existing_functions.items()
            if data.get("is_primitive")
        }

        # Check for duplicates and separate into new vs. existing functions
        duplicates_to_skip: Set[str] = set()
        existing_to_update: Set[str] = set()

        for name in temp_names:
            if name in existing_names:
                if overwrite:
                    # Mark for in-place update
                    existing_to_update.add(name)
                else:
                    # Skip this function - already exists
                    duplicates_to_skip.add(name)
                    results[name] = "skipped: already exists"

        # Validate dependencies and prepare entries for batch operations
        entries_to_create: List[Dict[str, Any]] = []
        entries_to_update: List[Dict[str, Any]] = []
        log_ids_to_update: List[int] = []
        log_id_to_name: Dict[int, str] = {}

        # Sandbox namespace roots whose dotted calls should be recorded in
        # depends_on (e.g. "primitives.actor.act" → depends_on includes
        # "primitives.actor.act").  At runtime, _inject_dependencies reads
        # these entries and calls construct_sandbox_root() to materialise
        # the root object.  All primitives live under a single "primitives"
        # namespace.
        env_namespaces = frozenset({"primitives"})

        # Store dependencies before their dependents so a same-batch dependency
        # contributes its class (and exists for hash/replay) when its dependent
        # is processed.
        parsed = self._order_batch_by_dependencies(parsed, all_known_function_names)
        batch_classes: Dict[str, SideEffectClass] = {}
        replayed_fixtures: Dict[str, Tuple[str, List[str]]] = {}

        for name, tree, node, source in parsed:
            if name in duplicates_to_skip:
                continue

            try:
                dependencies = self._collect_verified_dependencies(
                    node,
                    all_known_function_names,
                    environment_namespaces=env_namespaces,
                )
                dependencies_list = sorted(list(dependencies))

                tp_imports = detect_third_party_imports(
                    node,
                    environment_modules=ENVIRONMENT_MODULES,
                )
                if tp_imports and venv_id is None:
                    raise ValueError(
                        f"Function '{name}' imports third-party packages "
                        f"{sorted(tp_imports)} but no venv_id was provided. "
                        f"Create a virtual environment with "
                        f"FunctionManager_add_venv first, then pass the "
                        f"returned venv_id to FunctionManager_add_functions "
                        f"(or link it afterwards with "
                        f"FunctionManager_set_function_venv).",
                    )

                all_calls = self._collect_function_calls(node)
                self._validate_function_calls(name, all_calls, temp_names)
                namespace = create_base_globals()
                exec(source, namespace)
                fn_obj = namespace[name]
                signature = str(inspect.signature(fn_obj))
                docstring = inspect.getdoc(fn_obj) or ""
                embedding_text = f"Function Name: {name}\nSignature: {signature}\nDocstring: {docstring}"
                precondition = preconditions.get(name)

                prior_log = None
                if name in existing_to_update:
                    prior_log = self._get_log_by_function_id(
                        function_id=existing_functions[name]["function_id"],
                        raise_if_missing=True,
                    )

                entry_data = {
                    "language": "python",
                    "argspec": signature,
                    "docstring": docstring,
                    "implementation": source,
                    "depends_on": dependencies_list,
                    "third_party_imports": sorted(tp_imports),
                    "embedding_text": embedding_text,
                    "precondition": precondition,
                    "stale_reasons": [
                        reason.model_dump(mode="json")
                        for reason in self._dependency_stale_reasons(
                            dependencies_list,
                            available_names=all_known_function_names,
                        )
                    ],
                    **self._verification_fields_for_source(
                        source=source,
                        fn_obj=fn_obj,
                        known_function_names=all_known_function_names,
                        prior=prior_log.entries if prior_log is not None else None,
                        primitive_rows=primitive_rows,
                        authored_contract=contracts.get(name),
                        authored_fixtures=fixtures.get(name),
                        batch_classes=batch_classes,
                    ),
                }
                batch_classes[name] = SideEffectClass(entry_data["side_effect_class"])

                if venv_id is not None:
                    entry_data["venv_id"] = venv_id

                signatures = self._replay_fixtures_for_entry(
                    name=name,
                    entry=entry_data,
                    namespace=namespace,
                )
                if signatures:
                    replayed_fixtures[name] = (entry_data["verified_hash"], signatures)

                if prior_log is not None:
                    # Update existing function
                    log_id = prior_log.id
                    log_ids_to_update.append(log_id)
                    log_id_to_name[log_id] = name
                    entries_to_update.append(entry_data)
                    results[name] = "updated"
                else:
                    # Create new function
                    entry_data["name"] = name
                    entry_data["guidance_ids"] = []
                    self._stamp_new_function_usage(entry_data, name)
                    entries_to_create.append(entry_data)
                    results[name] = "added"
            except FixtureRegressionError as e:
                results[name] = f"error: {e}"
                fixture_regressions.append(e)
            except ValueError as e:
                results[name] = f"error: {e}"
            except Exception as e:
                results[name] = f"error: Unexpected error - {e}"
                logger.error(
                    f"Unexpected error processing function {name}: {e}",
                    exc_info=True,
                )

        # Batch create new functions
        if entries_to_create:
            try:
                unity_create_logs(
                    context=self._compositional_ctx,
                    entries=entries_to_create,
                    stamp_authoring=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to batch create function logs: {e}",
                    exc_info=True,
                )
                for entry in entries_to_create:
                    name = entry["name"]
                    if results.get(name) == "added":
                        results[name] = f"error: Failed to create log - {e}"

        # Batch update existing functions
        if log_ids_to_update and entries_to_update:
            try:
                db.update_logs(
                    logs=log_ids_to_update,
                    context=self._compositional_ctx,
                    entries=[
                        strip_authoring_assistant_id(entry)
                        for entry in entries_to_update
                    ],
                    overwrite=True,
                )
                # Content changed under everything that depends on these
                # names: dependents lose their trust before their next call.
                self._invalidate_dependents_of(log_id_to_name.values())
            except Exception as e:
                logger.error(
                    f"Failed to batch update function logs: {e}",
                    exc_info=True,
                )
                for log_id in log_ids_to_update:
                    name = log_id_to_name.get(log_id)
                    if name and results.get(name) == "updated":
                        results[name] = f"error: Failed to update log - {e}"

        # Replayed fixtures are evidence: record one tier-0 pass per fixture
        # under the new hash so the ledger fold matches the seeded summary.
        for name, (function_hash, signatures) in replayed_fixtures.items():
            if str(results.get(name, "")).startswith("error"):
                continue
            row = self._get_function_data_by_name(name=name)
            if row is None:
                continue
            for signature in signatures:
                self.record_verification(
                    VerificationRow(
                        function_id=int(row["function_id"]),
                        function_hash=function_hash,
                        kind=VerdictKind.tier0,
                        verdict="PASS",
                        reason="fixture replay reproduced the recorded result",
                        call_site="replay",
                        args_signature=signature,
                    ),
                )

        # Check for errors and raise if requested
        if raise_on_error:
            if fixture_regressions:
                raise fixture_regressions[0]
            errors = {k: v for k, v in results.items() if v.startswith("error")}
            if errors:
                error_details = "; ".join(f"{k}: {v}" for k, v in errors.items())
                raise ValueError(f"Failed to add function(s): {error_details}")

        return results

    def _add_shell_functions(
        self,
        *,
        implementations: List[str],
        language: Literal["bash", "zsh", "sh", "powershell"],
        preconditions: Dict[str, Dict],
        overwrite: bool,
        raise_on_error: bool = True,
    ) -> Dict[str, str]:
        """
        Add shell script functions (bash, zsh, sh, powershell).

        Shell scripts must include metadata comments at the top:
            # @name: my_function
            # @args: (input_file output_file --verbose)
            # @description: Brief description

        The @name comment is required. @args and @description are optional.
        """
        results: Dict[str, str] = {}
        parsed: List[Tuple[str, str, str, str, str]] = (
            []
        )  # (name, argspec, docstring, source, language)
        temp_names: Set[str] = set()

        # Parse metadata from all implementations
        for i, source in enumerate(implementations):
            metadata = _parse_shell_script_metadata(source)
            name = metadata["name"]

            if not name:
                key = f"implementation_{i+1}"
                results[key] = (
                    "error: Shell script must include '# @name: <function_name>' comment"
                )
                continue

            parsed.append(
                (
                    name,
                    metadata["argspec"],
                    metadata["docstring"],
                    source,
                    language,
                ),
            )
            temp_names.add(name)

        # Get existing functions for duplicate detection
        try:
            existing_functions = self.list_functions()
            existing_names = set(existing_functions.keys())
        except Exception as e:
            logger.warning(f"Failed to list existing functions: {e}")
            existing_functions = {}
            existing_names = set()

        # Check for duplicates
        duplicates_to_skip: Set[str] = set()
        existing_to_update: Set[str] = set()

        for name in temp_names:
            if name in existing_names:
                if overwrite:
                    existing_to_update.add(name)
                else:
                    duplicates_to_skip.add(name)
                    results[name] = "skipped: already exists"

        # Prepare entries for batch operations
        entries_to_create: List[Dict[str, Any]] = []
        entries_to_update: List[Dict[str, Any]] = []
        log_ids_to_update: List[int] = []
        log_id_to_name: Dict[int, str] = {}

        for name, argspec, docstring, source, lang in parsed:
            if name in duplicates_to_skip:
                continue

            try:
                embedding_text = f"Function Name: {name}\nLanguage: {lang}\nSignature: {argspec}\nDocstring: {docstring}"
                precondition = preconditions.get(name)

                # A shell script can reach anything on the machine; without an
                # AST to bound it, it sits at the unsafe end until a librarian
                # confirms otherwise.
                entry_data = {
                    "argspec": argspec,
                    "docstring": docstring,
                    "implementation": source,
                    "language": lang,
                    "depends_on": [],  # Shell scripts don't have auto-detected dependencies
                    "embedding_text": embedding_text,
                    "precondition": precondition,
                    "stale_reasons": [],
                    **self._unclassifiable_verification_fields(),
                }

                if name in existing_to_update:
                    # Update existing function
                    log_id = self._get_log_by_function_id(
                        function_id=existing_functions[name]["function_id"],
                        raise_if_missing=True,
                    ).id
                    log_ids_to_update.append(log_id)
                    log_id_to_name[log_id] = name
                    entries_to_update.append(entry_data)
                    results[name] = "updated"
                else:
                    # Create new function
                    entry_data["name"] = name
                    entry_data["guidance_ids"] = []
                    self._stamp_new_function_usage(entry_data, name)
                    entries_to_create.append(entry_data)
                    results[name] = "added"

            except Exception as e:
                results[name] = f"error: {e}"
                logger.error(
                    f"Error processing shell function {name}: {e}",
                    exc_info=True,
                )

        # Batch create new functions
        if entries_to_create:
            try:
                unity_create_logs(
                    context=self._compositional_ctx,
                    entries=entries_to_create,
                    stamp_authoring=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to batch create shell function logs: {e}",
                    exc_info=True,
                )
                for entry in entries_to_create:
                    name = entry["name"]
                    if results.get(name) == "added":
                        results[name] = f"error: Failed to create log - {e}"

        # Batch update existing functions
        if log_ids_to_update and entries_to_update:
            try:
                db.update_logs(
                    logs=log_ids_to_update,
                    context=self._compositional_ctx,
                    entries=[
                        strip_authoring_assistant_id(entry)
                        for entry in entries_to_update
                    ],
                    overwrite=True,
                )
            except Exception as e:
                logger.error(
                    f"Failed to batch update shell function logs: {e}",
                    exc_info=True,
                )
                for log_id in log_ids_to_update:
                    name = log_id_to_name.get(log_id)
                    if name and results.get(name) == "updated":
                        results[name] = f"error: Failed to update log - {e}"

        # Check for errors and raise if requested
        if raise_on_error:
            errors = {k: v for k, v in results.items() if v.startswith("error")}
            if errors:
                error_details = "; ".join(f"{k}: {v}" for k, v in errors.items())

        return results

    # ------------------------------------------------------------------ #
    #  Callable return + dependency injection                             #
    # ------------------------------------------------------------------ #

    def _get_function_data_by_name(self, *, name: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single compositional function record by name.

        Returns the full stored record (as a dict) or ``None`` if not found.
        """
        import time as _time

        _gfdn_t0 = _time.perf_counter()
        logger.debug(f"⏱️ [FM._get_function_data_by_name] start: {name}")

        # Normalize to the Unify filter grammar (and avoid quote-escaping issues).
        try:
            normalized = normalize_filter_expr(f"name == {json.dumps(name)}")
        except Exception:
            normalized = f"name == {json.dumps(name)}"

        last_exc: Exception | None = None

        # The backend can return 404 for missing contexts in fresh projects/tests.
        for attempt, delay in enumerate((0.0, 0.05, 0.15)):
            if delay:
                _time.sleep(delay)
            try:
                _q_t0 = _time.perf_counter()
                logs = []
                for context in self._read_compositional_contexts():
                    logs.extend(
                        db.get_logs(
                            context=context,
                            filter=normalized,
                            limit=1,
                            exclude_fields=list_private_fields(context),
                        ),
                    )
                    if logs:
                        break
                _q_ms = (_time.perf_counter() - _q_t0) * 1000
                if logs:
                    logger.debug(
                        f"⏱️ [FM._get_function_data_by_name] found (attempt={attempt}, "
                        f"query={_q_ms:.0f}ms, total={(_time.perf_counter() - _gfdn_t0) * 1000:.0f}ms)",
                    )
                    return self._hydrate_verification_fields(
                        [logs[0].entries],
                        default_context=context,
                    )[0]
                logger.debug(
                    f"⏱️ [FM._get_function_data_by_name] miss (attempt={attempt}, "
                    f"query={_q_ms:.0f}ms, total={(_time.perf_counter() - _gfdn_t0) * 1000:.0f}ms)",
                )
                return None
            except _UnifyRequestError as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status == 404:
                    last_exc = e
                    continue
                raise
            except Exception as e:
                last_exc = e
                break

        # Treat missing context as empty library.
        logger.debug(
            f"⏱️ [FM._get_function_data_by_name] exhausted retries "
            f"(total={(_time.perf_counter() - _gfdn_t0) * 1000:.0f}ms)",
        )
        if isinstance(last_exc, _UnifyRequestError):
            status = getattr(getattr(last_exc, "response", None), "status_code", None)
            if status == 404:
                return None
        if last_exc is not None:
            raise last_exc
        return None

    def _create_in_process_callable(
        self,
        func_data: Dict[str, Any],
        *,
        namespace: Dict[str, Any],
    ) -> _InProcessFunctionProxy:
        """Create an in-process callable wrapped in a proxy with state mode support.

        The function is exec'd into the provided ``namespace``, placing the **raw
        function** there. The caller should NOT overwrite ``namespace[func_name]``
        with the returned proxy - this allows:

        - Inter-function calls to work naturally (``await b()`` calls raw ``b``)
        - ``typing.get_type_hints(fn_name)`` to resolve correctly via ``__wrapped__``
        - Custom decorators (``@my_decorator``) to work during exec()

        The returned proxy provides state mode control:
        ``.stateful()`` / ``.stateless()`` / ``.read_only()``.
        """
        func_name = func_data.get("name")
        if not isinstance(func_name, str) or not func_name:
            raise ValueError("func_data missing valid 'name'")

        implementation = func_data.get("implementation")
        if not isinstance(implementation, str) or not implementation.strip():
            raise ValueError(f"Function '{func_name}' has no implementation")

        # Ensure user-defined annotation symbols don't cause NameErrors when callers
        # (e.g., CodeActActor) later resolve type hints via typing.get_type_hints().
        self._inject_forward_ref_annotation_placeholders(
            implementation,
            namespace=namespace,
        )

        exec(compile_function_source(func_name, implementation), namespace)
        raw_fn = namespace.get(func_name)
        if not callable(raw_fn):
            raise ValueError(
                f"Function '{func_name}' not found after exec() into namespace",
            )

        # Wrap in proxy to provide state mode API (.stateless(), .read_only())
        return _InProcessFunctionProxy(
            function_manager=self,
            func_data=func_data,
            namespace=namespace,
            raw_callable=raw_fn,
        )

    @staticmethod
    def _inject_forward_ref_annotation_placeholders(
        implementation: str,
        *,
        namespace: Dict[str, Any],
    ) -> None:
        """
        Inject placeholder types for missing symbols referenced in annotations.

        Motivation:
        - `exec()` succeeds when annotations are strings, but later calls to
          `typing.get_type_hints(fn)` will evaluate forward-ref strings in
          `fn.__globals__` and can raise NameError if the referenced types are not
          present.
        - CodeActActor wants a callable that "just works" without manual seeding
          of domain-specific types into the namespace.

        This only attempts to satisfy *annotation resolution* (not runtime logic).
        If the function body actually uses a type (e.g. `Role.ADMIN`), the
        function must still import/define it itself.
        """
        try:
            tree = ast.parse(implementation)
        except Exception:
            return

        if not tree.body:
            return

        fn_node: Optional[Union[ast.FunctionDef, ast.AsyncFunctionDef]] = None
        if len(tree.body) == 1 and isinstance(
            tree.body[0],
            (ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            fn_node = tree.body[0]
        else:
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    fn_node = node
                    break
        if fn_node is None:
            return

        ann_exprs: List[ast.AST] = []
        args = fn_node.args
        for arg in list(args.posonlyargs) + list(args.args) + list(args.kwonlyargs):
            if getattr(arg, "annotation", None) is not None:
                ann_exprs.append(arg.annotation)  # type: ignore[arg-type]
        if (
            args.vararg is not None
            and getattr(args.vararg, "annotation", None) is not None
        ):
            ann_exprs.append(args.vararg.annotation)  # type: ignore[arg-type]
        if (
            args.kwarg is not None
            and getattr(args.kwarg, "annotation", None) is not None
        ):
            ann_exprs.append(args.kwarg.annotation)  # type: ignore[arg-type]
        if getattr(fn_node, "returns", None) is not None:
            ann_exprs.append(fn_node.returns)  # type: ignore[arg-type]

        annotation_names: Set[str] = set()
        for expr in ann_exprs:
            # Forward-ref strings: parse the string itself as a Python expression.
            if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
                ref = expr.value.strip()
                if not ref:
                    continue
                try:
                    ref_tree = ast.parse(ref, mode="eval")
                except Exception:
                    continue
                for node in ast.walk(ref_tree):
                    if isinstance(node, ast.Name) and node.id:
                        annotation_names.add(node.id)
                continue

            for node in ast.walk(expr):
                if isinstance(node, ast.Name) and node.id:
                    annotation_names.add(node.id)

        if not annotation_names:
            return

        # exec() installs the real builtins module into the namespace, so real
        # builtin names (dict, str, list, ...) are always resolvable even when
        # the caller passes a fresh namespace without __builtins__. Shadowing
        # them with placeholder classes breaks subscripted annotations like
        # ``dict[str, Any]`` at def-evaluation time.
        builtin_names = set(dir(builtins))
        builtins_obj = namespace.get("__builtins__")
        if isinstance(builtins_obj, dict):
            builtin_names |= set(builtins_obj.keys())
        elif builtins_obj is not None:
            builtin_names |= set(dir(builtins_obj))

        typing_mod = namespace.get("typing")
        pydantic_mod = namespace.get("pydantic")

        for name in sorted(annotation_names):
            if name == "typing":
                continue
            if name in namespace:
                continue
            if name in builtin_names:
                continue

            # Common typing helpers can be recovered from typing when present.
            if typing_mod is not None and hasattr(typing_mod, name):
                try:
                    namespace[name] = getattr(typing_mod, name)
                    continue
                except Exception:
                    pass

            # Some common pydantic types may appear in annotations.
            if pydantic_mod is not None and hasattr(pydantic_mod, name):
                try:
                    namespace[name] = getattr(pydantic_mod, name)
                    continue
                except Exception:
                    pass

            # Fall back to a placeholder type to avoid NameError during hint resolution.
            try:
                namespace[name] = type(name, (), {})
            except Exception:
                # If something extremely unusual happens, just skip.
                continue

    def _create_venv_callable(
        self,
        func_data: Dict[str, Any],
        *,
        namespace: Dict[str, Any],
    ) -> Callable[..., Any]:
        """Create a proxy callable for a function that must run in an isolated venv."""
        return _VenvFunctionProxy(
            function_manager=self,
            func_data=func_data,
            namespace=namespace,
        )

    def _inject_dependencies(
        self,
        func_data: Dict[str, Any],
        *,
        namespace: Dict[str, Any],
        visited: Set[str],
    ) -> None:
        """Inject transitive dependencies into ``namespace`` (breadth-first).

        This is the runtime counterpart to the AST-based dependency detection
        in ``dependency_analysis.py``.  Every name that ``add_functions``
        recorded in ``depends_on`` is resolved here into a live object in the
        execution namespace.  The two categories:

        **Bare names** (e.g. ``"helper"``) — other compositional functions.
        The stored implementation is exec'd into the namespace so inter-
        function calls resolve naturally.

        **Dotted names** (e.g. ``"primitives.actor.act"``,
        ``"primitives.contacts.ask"``) — environment-provided namespaces.
        Only the *root* segment matters for injection (``"primitives"``).
        If the root is not already present in the namespace,
        ``construct_sandbox_root()`` from the primitive registry constructs
        a fresh ``Primitives`` instance on demand.  ``Primitives`` is
        fully stateless, so a freshly constructed instance works in
        isolation without any ambient ContextVars or parent actor state.
        """
        from collections import deque

        from unify.function_manager.primitives.registry import construct_sandbox_root

        deps = func_data.get("depends_on") or []
        if not isinstance(deps, list):
            return

        q = deque([d for d in deps if isinstance(d, str) and d])
        while q:
            dep_name = q.popleft()
            if dep_name in visited:
                continue
            visited.add(dep_name)

            # ── Dotted dependency (e.g. "primitives.actor.act", "primitives.contacts.ask") ──
            if "." in dep_name:
                root = dep_name.split(".")[0]
                if root not in namespace:
                    root_obj = construct_sandbox_root(
                        root,
                        primitive_scope=self._primitive_scope,
                    )
                    if root_obj is not None:
                        namespace[root] = root_obj
                    else:
                        logger.warning(
                            "Dotted dependency %r for %r: root %r could not "
                            "be constructed and is not in namespace, skipping",
                            dep_name,
                            func_data.get("name"),
                            root,
                        )
                continue

            # ── Bare dependency (compositional function) ─────────────────────
            dep_data = self._get_function_data_by_name(name=dep_name)
            if not dep_data:
                logger.warning(
                    f"Dependency '{dep_name}' not found for '{func_data.get('name')}', skipping",
                )
                continue

            # Handle venv dependencies: proxy goes in namespace (only way to call them)
            if dep_data.get("venv_id") is not None:
                _venv_cb = self._create_venv_callable(
                    dep_data,
                    namespace=namespace,
                )
                # Wrap boundary so inter-function calls create lineage frames
                # and pay the tier-0 contract check.
                namespace[dep_name] = self._boundary(_venv_cb, dep_data)
                # Treat venv functions as atomic; do not recurse into their deps.
                continue

            # Handle in-process dependencies: exec puts raw function in namespace.
            # We call _create_in_process_callable to exec the function, but we
            # DON'T overwrite namespace with the proxy - the raw function stays
            # for inter-function calls, decorators, and introspection.
            self._create_in_process_callable(
                dep_data,
                namespace=namespace,
            )
            # replace namespace[dep_name] with wrapper so inter-function calls
            # also flow through lineage/event boundaries and tier-0 checks.
            raw_dep = namespace.get(dep_name)
            if callable(raw_dep) and not hasattr(raw_dep, "__tier0_inner__"):
                namespace[dep_name] = self._boundary(raw_dep, dep_data)

            nested = dep_data.get("depends_on") or []
            if isinstance(nested, list):
                for child in nested:
                    if isinstance(child, str) and child and child not in visited:
                        q.append(child)

    def _inject_callables_for_functions(
        self,
        func_rows: List[Dict[str, Any]],
        *,
        namespace: Dict[str, Any],
    ) -> List[Callable[..., Any]]:
        """Convert function records into callables and return proxies to caller.

        For in-process functions, the raw function (from exec) remains in the
        namespace for inter-function calls, decorators, and introspection.
        The returned proxies provide state mode control (.stateful/.stateless/.read_only).

        For venv functions, the proxy is placed in namespace (no raw function exists).

        For primitives, the callable is resolved from the live runtime registry
        via ``get_primitive_callable``. Primitives are NOT injected into the
        namespace (they are already accessible via the ``primitives`` object).
        """
        callables: List[Callable[..., Any]] = []
        visited: Set[str] = set()

        for func_data in func_rows:
            name = func_data.get("name")
            if not isinstance(name, str) or not name:
                raise ValueError("Function record missing valid 'name'")

            # Primitives: resolve callable from the live runtime registry.
            # They are already accessible via the ``primitives`` object in the
            # namespace, so we don't inject a new namespace entry.
            if func_data.get("is_primitive") is True:
                from unify.function_manager.primitives.runtime import (
                    get_primitive_callable,
                )

                primitives_obj = namespace.get("primitives")
                fn = get_primitive_callable(
                    func_data,
                    primitives=primitives_obj,
                )
                if fn is not None:
                    fn = _LineageTrackedFunction(fn, name)
                callables.append(fn)
                continue

            # Check if we've already processed this function (e.g., duplicate in results)
            if name in visited:
                # Already exec'd - just create a new proxy wrapping existing raw fn
                if func_data.get("venv_id") is not None:
                    fn = self._create_venv_callable(func_data, namespace=namespace)
                else:
                    raw_fn = namespace.get(name)
                    if callable(raw_fn):
                        # If the namespace contains our wrapper, unwrap for the proxy.
                        try:
                            if hasattr(raw_fn, "__wrapped__"):
                                raw_fn_for_proxy = getattr(raw_fn, "__wrapped__")
                                if callable(raw_fn_for_proxy):
                                    raw_fn = raw_fn_for_proxy
                        except Exception:
                            pass
                        fn = _InProcessFunctionProxy(
                            function_manager=self,
                            func_data=func_data,
                            namespace=namespace,
                            raw_callable=raw_fn,
                        )
                    else:
                        # Shouldn't happen, but fallback to full creation
                        fn = self._create_in_process_callable(
                            func_data,
                            namespace=namespace,
                        )
                callables.append(fn)
                continue

            visited.add(name)  # Prevent cycles from re-injecting the root function.
            self._inject_dependencies(func_data, namespace=namespace, visited=visited)

            # Create callable for the root function.
            if func_data.get("venv_id") is not None:
                # Venv: proxy goes in namespace (only way to call them)
                fn = self._create_venv_callable(func_data, namespace=namespace)
                # Wrap boundary for lineage/events and tier-0; keep proxy for return value.
                namespace[name] = self._boundary(fn, func_data)
            else:
                # In-process: exec puts raw function in namespace, return proxy to caller
                # DON'T overwrite namespace - raw function stays for internal use
                fn = self._create_in_process_callable(func_data, namespace=namespace)
                # replace namespace[name] with wrapper so inter-function calls
                # also flow through lineage/event boundaries and tier-0 checks.
                raw_root = namespace.get(name)
                if callable(raw_root) and not hasattr(raw_root, "__tier0_inner__"):
                    namespace[name] = self._boundary(raw_root, func_data)

            callables.append(fn)

        return callables

    # 2. Listing -------------------------------------------------------- #

    def list_function_name_to_ids(self) -> Dict[str, int]:
        """Return the authoritative ``{name: function_id}`` catalogue.

        Used by deployment reconcile for guidance entrypoint resolution.
        Prefer this over :meth:`list_functions` when only ids are needed.

        Deployment references are resolved independently of the current
        runtime's discovery surface. A runtime can legitimately hide
        environment-gated functions from actor discovery, but their stored
        ids must still be available when reconciling references authored
        for a different environment. Therefore compositional rows are read
        without ``filter_scope`` or environment exclusions here; ordinary
        list/filter/search operations remain scoped.
        """

        mapping: Dict[str, int] = {}
        for context in self._read_compositional_contexts():
            try:
                logs = db.get_logs(
                    context=context,
                    from_fields=["name", "function_id"],
                )
            except Exception:
                continue
            for lg in logs:
                entries = lg.entries or {}
                name = entries.get("name")
                function_id = entries.get("function_id")
                if (
                    isinstance(name, str)
                    and name
                    and function_id is not None
                    and name not in mapping
                ):
                    mapping[name] = int(function_id)

        if self._include_primitives:
            for ent in self._primitive_logs():
                name = ent.get("name")
                function_id = ent.get("function_id")
                if (
                    isinstance(name, str)
                    and name
                    and function_id is not None
                    and name not in mapping
                ):
                    mapping[name] = int(function_id)
        return mapping

    @functools.wraps(BaseFunctionManager.list_functions, updated=())
    def list_functions(
        self,
        *,
        include_implementations: bool = False,
        _return_callable: bool = False,
        _namespace: Optional[Dict[str, Any]] = None,
        _also_return_metadata: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        if _also_return_metadata and not _return_callable:
            raise ValueError("_also_return_metadata requires _return_callable=True")

        if _return_callable and _namespace is None:
            raise ValueError("_namespace required when _return_callable=True")

        compositional_rows: List[Dict[str, Any]] = []
        for context in self._read_compositional_contexts():
            context_rows = [
                lg.entries
                for lg in db.get_logs(
                    context=context,
                    filter=self._scoped_filter(None),
                    exclude_fields=list_private_fields(context),
                )
            ]
            compositional_rows.extend(
                self._hydrate_verification_fields(
                    context_rows,
                    default_context=context,
                ),
            )

        primitive_rows: List[Dict[str, Any]] = []
        if self._include_primitives:
            primitive_rows = self._primitive_logs()

        all_rows = compositional_rows + primitive_rows

        metadata: Dict[str, Dict[str, Any]] = {}
        func_rows: List[Dict[str, Any]] = []
        seen_names: set[str] = set()
        for ent in all_rows:
            name = ent.get("name")
            if not isinstance(name, str):
                continue
            if name in seen_names:
                continue
            seen_names.add(name)
            func_rows.append(ent)

            data: Dict[str, Any] = {
                "function_id": ent.get("function_id"),
                "language": ent.get(
                    "language",
                    "python",
                ),  # Default for backward compat
                "argspec": ent.get("argspec"),
                "docstring": ent.get("docstring", ""),
                "depends_on": ent.get("depends_on", []),
                "stale_reasons": ent.get("stale_reasons", []),
                "guidance_ids": ent.get("guidance_ids", []),
                "verify": ent.get("verify", True),
                "venv_id": ent.get("venv_id"),
                "third_party_imports": ent.get("third_party_imports", []),
                "is_primitive": ent.get("is_primitive", False),
            }
            for key in (
                "primitive_class",
                "primitive_method",
                "metadata",
            ):
                if key in ent:
                    data[key] = ent.get(key)
            if include_implementations:
                data["implementation"] = ent.get("implementation")
            metadata[name] = data

        if not _return_callable:
            return metadata

        assert _namespace is not None  # validated above
        callables_list = self._inject_callables_for_functions(
            func_rows,
            namespace=_namespace,
        )
        callables_map = {
            row["name"]: cb
            for row, cb in zip(func_rows, callables_list)
            if isinstance(row.get("name"), str)
        }

        if _also_return_metadata:
            return {"callables": callables_map, "metadata": metadata}  # type: ignore[return-value]

        return callables_map  # type: ignore[return-value]

    @functools.wraps(BaseFunctionManager.get_precondition, updated=())
    def get_precondition(self, *, function_name: str) -> Optional[Dict[str, Any]]:
        # Check compositional first, then optionally primitives.
        logs = []
        for context in self._read_compositional_contexts():
            logs.extend(
                db.get_logs(
                    context=context,
                    filter=self._scoped_filter(f"name == '{function_name}'"),
                    limit=1,
                    exclude_fields=list_private_fields(context),
                ),
            )
            if logs:
                break
        if not logs and self._include_primitives:
            primitive_rows = self._primitive_logs(
                extra_filter=f"name == '{function_name}'",
                limit=1,
            )
            if primitive_rows:
                return primitive_rows[0].get("precondition")
        if not logs:
            return None

        return logs[0].entries.get("precondition")

    @staticmethod
    def _dependency_stale_reasons(
        depends_on: List[str],
        *,
        available_names: set[str],
        existing: Optional[List[StaleReason]] = None,
    ) -> List[StaleReason]:
        preserved = [
            reason
            for reason in coerce_stale_reasons(existing)
            if reason.dep_kind != "depends_on"
        ]
        # Primitive references ("primitives.<manager>.<method>") are resolved
        # against whatever environments the runtime injects, not against this
        # FunctionManager instance's own primitive catalog. Whether that
        # catalog contains a given name reflects this instance's own
        # scope/sync state, not whether the primitive actually exists and
        # runs -- so it can never be an authoritative "missing" signal.
        # Compositional dependencies are the only category this FunctionManager
        # owns outright (Functions/Compositional), so only those are checked.
        missing = [
            StaleReason(
                dep_kind="depends_on",
                name=name,
                message=f"missing dependency name={name}",
            )
            for name in depends_on
            if not name.startswith("primitives.") and name not in available_names
        ]
        return merge_stale_reasons(preserved, *missing)

    def _available_dependency_names(
        self,
        compositional_logs: Optional[List[Any]] = None,
    ) -> set[str]:
        if compositional_logs is None:
            compositional_logs = db.get_logs(
                context=self._compositional_ctx,
                exclude_fields=list_private_fields(self._compositional_ctx),
            )
        return {
            str(log.entries["name"])
            for log in compositional_logs
            if log.entries.get("name")
        }

    def _append_missing_dependency_reasons(
        self,
        *,
        logs: List[Any],
        missing_names: set[str],
    ) -> None:
        for log in logs:
            dependencies = set(log.entries.get("depends_on") or [])
            matched = sorted(dependencies.intersection(missing_names))
            if not matched:
                continue
            existing = coerce_stale_reasons(log.entries.get("stale_reasons"))
            merged = merge_stale_reasons(
                existing,
                *[
                    StaleReason(
                        dep_kind="depends_on",
                        name=name,
                        message=f"missing dependency name={name}",
                    )
                    for name in matched
                ],
            )
            if [reason.model_dump(mode="json") for reason in merged] == [
                reason.model_dump(mode="json") for reason in existing
            ]:
                continue
            db.update_logs(
                context=self._compositional_ctx,
                logs=[log.id],
                entries={
                    "stale_reasons": [
                        reason.model_dump(mode="json") for reason in merged
                    ],
                },
                overwrite=True,
            )

    def _mark_guidance_stale_for_deleted_functions(
        self,
        deleted_functions: List[tuple[int, str]],
    ) -> None:
        if not deleted_functions:
            return
        from ..guidance_manager.guidance_manager import GUIDANCE_TABLE, GuidanceManager

        for root in ContextRegistry.read_roots(GuidanceManager, GUIDANCE_TABLE):
            context = f"{root.strip('/')}/{GUIDANCE_TABLE}"
            for function_id, name in deleted_functions:
                logs = db.get_logs(
                    context=context,
                    filter=f"{int(function_id)} in function_ids",
                    exclude_fields=list_private_fields(context),
                )
                for log in logs:
                    existing = coerce_stale_reasons(
                        log.entries.get("stale_reasons"),
                    )
                    merged = merge_stale_reasons(
                        existing,
                        StaleReason(
                            dep_kind="function",
                            id=int(function_id),
                            name=name,
                            message=(
                                f"missing function_id={int(function_id)} name={name}"
                            ),
                        ),
                    )
                    db.update_logs(
                        context=context,
                        logs=[log.id],
                        entries={
                            "stale_reasons": [
                                reason.model_dump(mode="json") for reason in merged
                            ],
                        },
                        overwrite=True,
                    )

    # 3. Deletion ------------------------------------------------------- #

    @functools.wraps(BaseFunctionManager.delete_function, updated=())
    def delete_function(
        self,
        *,
        function_id: Union[int, List[int]],
        delete_dependents: bool = True,
    ) -> Dict[str, str]:
        """
        Delete one or more functions and optionally their dependents in a single batch operation.

        Args:
            function_id: Function ID (int) or list of function IDs to delete.
            delete_dependents: If True, also delete all functions that depend on target(s).

        Returns:
            Dictionary mapping function names to "deleted" or "already_deleted".

        Raises:
            ValueError: If any of the requested function_ids correspond to
                primitives (system-owned functions that cannot be deleted).
        """
        # Normalize to list
        function_ids = [function_id] if isinstance(function_id, int) else function_id

        if not function_ids:
            return {}

        # Reject deletion of primitives (only check when primitives are enabled).
        if self._include_primitives:
            id_clauses = " or ".join(f"function_id == {fid}" for fid in function_ids)
            prim_rows = self._primitive_logs(
                extra_filter=id_clauses,
                limit=len(function_ids),
            )
            if prim_rows:
                prim_names = [
                    row.get("name", row.get("function_id")) for row in prim_rows
                ]
                raise ValueError(
                    f"Cannot delete primitives (system-owned): {prim_names}",
                )

        exclude_fields = list_private_fields(self._compositional_ctx)

        def _load_compositional_logs():
            return db.get_logs(
                context=self._compositional_ctx,
                exclude_fields=exclude_fields,
            )

        # Single-id: cheap existence check before any full-table scan.
        if len(function_ids) == 1:
            log = self._get_log_by_function_id(
                function_id=function_ids[0],
                raise_if_missing=False,
            )
            if log is None:
                return {f"function_{function_ids[0]}": "already_deleted"}

            target_name = log.entries["name"]
            ids_to_delete = {function_ids[0]}
            log_ids_to_delete = [log.id]
            results = {target_name: "deleted"}
            target_names = {target_name}
            id_to_name = {function_ids[0]: target_name}
            id_to_log = {function_ids[0]: log}
            all_logs = None
        else:
            all_logs = _load_compositional_logs()
            id_to_log = {lg.entries["function_id"]: lg for lg in all_logs}
            id_to_name = {
                lg.entries["function_id"]: lg.entries["name"] for lg in all_logs
            }
            ids_to_delete = set(function_ids)
            target_names = {
                id_to_name[fid] for fid in function_ids if fid in id_to_name
            }

            if not target_names:
                return {}

            log_ids_to_delete = [
                id_to_log[fid].id for fid in function_ids if fid in id_to_log
            ]
            results = {
                id_to_name[fid]: "deleted" for fid in function_ids if fid in id_to_name
            }

        if delete_dependents:
            # BFS needs the full depends_on graph.
            if all_logs is None:
                all_logs = _load_compositional_logs()
                id_to_log = {lg.entries["function_id"]: lg for lg in all_logs}
                id_to_name = {
                    lg.entries["function_id"]: lg.entries["name"] for lg in all_logs
                }
            function_deps = {
                lg.entries["function_id"]: set(lg.entries.get("depends_on", []))
                for lg in all_logs
            }
            to_process = set(target_names)
            processed = set()

            while to_process:
                current_name = to_process.pop()
                if current_name in processed:
                    continue
                processed.add(current_name)

                for fid, deps in function_deps.items():
                    if current_name in deps and fid not in ids_to_delete:
                        ids_to_delete.add(fid)
                        if fid in id_to_log:
                            log_ids_to_delete.append(id_to_log[fid].id)
                            dep_name = id_to_name[fid]
                            results[dep_name] = "deleted"
                            to_process.add(dep_name)
        else:
            # Keep dependents, but record link debt on rows that still
            # reference the deleted name(s). Need the compositional snapshot
            # (list-membership filters are not reliable enough here).
            if all_logs is None:
                all_logs = _load_compositional_logs()
            self._append_missing_dependency_reasons(
                logs=[
                    log
                    for log in all_logs
                    if log.entries["function_id"] not in ids_to_delete
                ],
                missing_names=set(target_names),
            )

        self._mark_guidance_stale_for_deleted_functions(
            [
                (int(function_id), str(id_to_name[function_id]))
                for function_id in sorted(ids_to_delete)
                if function_id in id_to_name
            ],
        )

        # The librarian's supersede flow is delete-then-add: stash each
        # deleted row's usage trace by name so a same-process replacement
        # inherits its standing instead of restarting the popularity
        # contest from zero. (A cross-process supersede loses the trace —
        # the replacement simply starts as a newborn, which the grace
        # already handles.)
        for fid in ids_to_delete:
            log = id_to_log.get(fid)
            if log is not None:
                self._stash_usage_legacy(
                    id_to_name.get(fid),
                    log.entries or {},
                )

        # Batch delete all functions
        if log_ids_to_delete:
            db.delete_logs(
                context=self._compositional_ctx,
                logs=log_ids_to_delete,
            )

        return results

    @functools.wraps(BaseFunctionManager.reconcile_dependencies, updated=())
    def reconcile_dependencies(
        self,
        *,
        function_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        all_logs = db.get_logs(
            context=self._compositional_ctx,
            exclude_fields=list_private_fields(self._compositional_ctx),
        )
        selected_ids = (
            {int(function_id) for function_id in function_ids}
            if function_ids is not None
            else None
        )
        selected = [
            log
            for log in all_logs
            if selected_ids is None or int(log.entries["function_id"]) in selected_ids
        ]
        available = self._available_dependency_names(all_logs)
        stale_function_ids: list[int] = []
        for log in selected:
            function = Function(**log.entries)
            refreshed = self._dependency_stale_reasons(
                function.depends_on,
                available_names=available,
                existing=function.stale_reasons,
            )
            if refreshed:
                stale_function_ids.append(int(function.function_id))
            serialized = [reason.model_dump(mode="json") for reason in refreshed]
            if serialized == [
                reason.model_dump(mode="json") for reason in function.stale_reasons
            ]:
                continue
            db.update_logs(
                context=self._compositional_ctx,
                logs=[log.id],
                entries={"stale_reasons": serialized},
                overwrite=True,
            )
        return {
            "outcome": "dependencies reconciled",
            "details": {
                "checked": len(selected),
                "stale_function_ids": stale_function_ids,
                "stale_count": len(stale_function_ids),
            },
        }

    # 4. Filter --------------------------------------------------------- #

    @functools.wraps(BaseFunctionManager.filter_functions, updated=())
    def filter_functions(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        include_implementations: bool = True,
        destination: Optional[str] = _DESTINATION_UNSET,  # type: ignore[assignment]
        _return_callable: bool = False,
        _namespace: Optional[Dict[str, Any]] = None,
        _also_return_metadata: bool = False,
    ) -> List[Dict[str, Any]]:
        if destination is not _DESTINATION_UNSET:
            context = self._function_context_for_destination(
                FUNCTIONS_COMPOSITIONAL_TABLE,
                destination=destination,
            )
            with self._temporary_function_context("_compositional_ctx", context):
                return self._filter_functions_impl(
                    filter=filter,
                    offset=offset,
                    limit=limit,
                    include_implementations=include_implementations,
                    _return_callable=_return_callable,
                    _namespace=_namespace,
                    _also_return_metadata=_also_return_metadata,
                )
        return self._filter_functions_impl(
            filter=filter,
            offset=offset,
            limit=limit,
            include_implementations=include_implementations,
            _return_callable=_return_callable,
            _namespace=_namespace,
            _also_return_metadata=_also_return_metadata,
        )

    def _filter_functions_impl(
        self,
        *,
        filter: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
        include_implementations: bool = True,
        _return_callable: bool = False,
        _namespace: Optional[Dict[str, Any]] = None,
        _also_return_metadata: bool = False,
    ) -> List[Dict[str, Any]]:
        if _also_return_metadata and not _return_callable:
            raise ValueError("_also_return_metadata requires _return_callable=True")

        if _return_callable and _namespace is None:
            raise ValueError("_namespace required when _return_callable=True")

        caller_filter = normalize_filter_expr(filter)
        contexts = [
            FederatedSearchContext(
                context=context,
                source="compositional",
                row_filter=self._scoped_filter(None),
            )
            for context in self._read_compositional_contexts()
        ]

        if self._include_primitives:
            contexts.extend(self._primitive_read_specs())

        contexts = [
            dataclasses.replace(
                spec,
                excluded_fields=list_private_fields(
                    spec.context,
                    project=spec.project,
                ),
            )
            for spec in contexts
        ]

        try:
            rows = federated_filter(
                contexts,
                filter=caller_filter,
                offset=offset,
                limit=limit,
            )
        except ToolErrorException as exc:
            return exc.payload
        self._hydrate_verification_fields(rows)

        if not _return_callable:
            rows = strip_ledger_internals(rows)
            if not include_implementations:
                rows = [
                    {k: v for k, v in row.items() if k != "implementation"}
                    for row in rows
                ]
            return rows

        assert _namespace is not None  # validated above
        callables_list = self._inject_callables_for_functions(
            rows,
            namespace=_namespace,
        )
        if _also_return_metadata:
            # Strip implementations from metadata if not requested
            metadata_rows = rows
            if not include_implementations:
                metadata_rows = [
                    {k: v for k, v in row.items() if k != "implementation"}
                    for row in rows
                ]
            return {"callables": callables_list, "metadata": metadata_rows}  # type: ignore[return-value]
        return callables_list  # type: ignore[return-value]

    # 5. Semantic Search ------------------------------------------------ #
    @functools.wraps(BaseFunctionManager.search_functions, updated=())
    def search_functions(
        self,
        *,
        query: str = "",
        n: int = 5,
        include_implementations: bool = True,
        include_dormant: bool = False,
        _return_callable: bool = False,
        _namespace: Optional[Dict[str, Any]] = None,
        _also_return_metadata: bool = False,
    ) -> List[Dict[str, Any]]:
        if _also_return_metadata and not _return_callable:
            raise ValueError("_also_return_metadata requires _return_callable=True")

        if _return_callable and _namespace is None:
            raise ValueError("_namespace required when _return_callable=True")

        # Soft models sometimes call search with ``{}`` / empty query during
        # discovery. The store rejects embed(""), so fall back to a plain
        # catalogue sample instead of a vector sort.
        if not str(query or "").strip():
            return self.filter_functions(
                filter=None,
                offset=0,
                limit=n,
                include_implementations=include_implementations,
                _return_callable=_return_callable,
                _namespace=_namespace,
                _also_return_metadata=_also_return_metadata,
            )

        allowed_fields = (
            list(Function.model_fields.keys())
            if _return_callable
            else [
                "function_id",
                "language",
                "name",
                "argspec",
                "docstring",
                "depends_on",
                "stale_reasons",
                "embedding_text",
                "precondition",
                "guidance_ids",
                "verify",
                "is_primitive",
                "primitive_class",
                "primitive_method",
                "metadata",
                "venv_id",
                "windows_os_required",
                # The usage trace rides along so ranking can compute
                # standing without a second read per row.
                "created_at",
                "usage_calls",
                "usage_last_called_at",
                "usage_recent_calls",
                "usage_search_hits",
            ]
        )
        if not _return_callable and include_implementations:
            allowed_fields.append("implementation")

        contexts = [
            FederatedSearchContext(
                context=context,
                source="compositional",
                row_filter=self._scoped_filter(None),
                allowed_fields=allowed_fields,
            )
            for context in self._read_compositional_contexts()
        ]

        if self._include_primitives:
            contexts.extend(
                self._primitive_read_specs(allowed_fields=allowed_fields),
            )

        # Overfetch so the activation pass has candidates to rank and drop:
        # the federated cut to `limit` happens before standing is known, and
        # a scope-filtered result set must still be able to fill n slots.
        activation_settings = self.activation_settings
        fetch_limit = (
            min(
                max(n + 4, n * activation_settings.search_overfetch_factor),
                activation_settings.search_overfetch_cap,
            )
            if activation_settings.enabled
            else n
        )
        results = federated_ranked_search(
            contexts,
            {"embedding_text": query},
            limit=fetch_limit,
            unique_id_field="function_id",
            backfill=True,
        )
        results = self._activation_rank(
            results,
            n=n,
            include_dormant=include_dormant,
        )
        self._bump_search_hits(results)
        if _return_callable:
            self._hydrate_verification_fields(results)

        if not _return_callable:
            compact_results = self._compact_function_search_rows(results)
            if include_implementations:
                for compact, full in zip(compact_results, results, strict=True):
                    if "implementation" in full:
                        compact["implementation"] = full["implementation"]
            return compact_results

        assert _namespace is not None  # validated above
        callables_list = self._inject_callables_for_functions(
            results,
            namespace=_namespace,
        )

        if _also_return_metadata:
            metadata_rows = self._compact_function_search_rows(results)
            if include_implementations:
                for compact, full in zip(metadata_rows, results, strict=True):
                    if "implementation" in full:
                        compact["implementation"] = full["implementation"]
            return {"callables": callables_list, "metadata": metadata_rows}  # type: ignore[return-value]

        return callables_list  # type: ignore[return-value]

    # ------------------------------------------------------------------ #
    #  Inverse linkage: Functions → Guidance                              #
    # ------------------------------------------------------------------ #

    def _guidance_context(self) -> str:
        ctxs = db.get_active_context()
        read_ctx = ctxs.get("read")
        return f"{read_ctx}/Guidance" if read_ctx else "Guidance"

    def _get_guidance_ids_for_function(self, *, function_id: int) -> List[int]:
        # Prefer reading from the function row if present
        try:
            log = self._get_log_by_function_id(function_id=function_id)
            gids = log.entries.get("guidance_ids") or []
            if isinstance(gids, list) and gids:
                return [int(g) for g in gids]
        except Exception:
            pass

        # Fallback: scan Guidance rows that reference this function via function_ids
        gctx = self._guidance_context()
        try:
            rows = db.get_logs(
                context=gctx,
                filter=f"{int(function_id)} in function_ids",
                exclude_fields=list_private_fields(gctx),
            )
            return [
                int(r.entries.get("guidance_id"))
                for r in rows
                if r.entries.get("guidance_id") is not None
            ]
        except Exception:
            return []

    def _get_guidance_for_function(
        self,
        *,
        function_id: int,
        include_images: bool = True,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return guidance records linked to the function.

        Each dict includes: guidance_id, title, content, images (optional).
        """
        gids = self._get_guidance_ids_for_function(function_id=function_id)
        if not gids:
            return []
        if limit is not None:
            try:
                limit = int(limit)
            except Exception:
                limit = None
            if isinstance(limit, int) and limit >= 0:
                gids = gids[:limit]
        cond = " or ".join(f"guidance_id == {int(g)}" for g in gids)
        gctx = self._guidance_context()
        rows = db.get_logs(
            context=gctx,
            filter=cond or "False",
            exclude_fields=list_private_fields(gctx),
        )
        out: List[Dict[str, Any]] = []
        for lg in rows:
            ent = lg.entries
            rec: Dict[str, Any] = {
                "guidance_id": ent.get("guidance_id"),
                "title": ent.get("title"),
                "content": ent.get("content"),
            }
            if include_images:
                rec["images"] = ent.get("images") or []
            out.append(rec)
        return out

    def _get_image_handles_for_function_guidance(
        self,
        *,
        function_id: int,
        limit: Optional[int] = None,
    ) -> List[ImageHandle]:
        """Return ImageHandle objects for images referenced by guidance linked to the function."""
        guids = self._get_guidance_for_function(
            function_id=function_id,
            include_images=True,
        )
        image_ids: List[int] = []
        for g in guids:
            imgs = g.get("images") or []
            # Support either raw list (ImageRefs) or a dict with root
            if isinstance(imgs, dict) and "root" in imgs:
                imgs = imgs.get("root") or []
            if not isinstance(imgs, list):
                continue
            for ref in imgs:
                try:
                    if isinstance(ref, dict):
                        # AnnotatedImageRef shape: {"raw_image_ref": {"image_id": X}, "annotation": ...}
                        if "raw_image_ref" in ref and isinstance(
                            ref["raw_image_ref"],
                            dict,
                        ):
                            iid = int(ref["raw_image_ref"].get("image_id"))
                            image_ids.append(iid)
                        elif "image_id" in ref:
                            image_ids.append(int(ref.get("image_id")))
                    else:
                        # If objects leaked through, try attribute access
                        iid = getattr(
                            getattr(ref, "raw_image_ref", ref),
                            "image_id",
                            None,
                        )
                        if iid is not None:
                            image_ids.append(int(iid))
                except Exception:
                    continue
        # Preserve order while de-duplicating
        image_ids = list(dict.fromkeys(image_ids))
        if limit is not None:
            try:
                limit = int(limit)
            except Exception:
                limit = None
            if isinstance(limit, int) and limit >= 0:
                image_ids = image_ids[:limit]

        im = ManagerRegistry.get_image_manager()
        return im.get_images(image_ids)

    def _attach_guidance_images_for_function_to_context(
        self,
        *,
        function_id: int,
        limit: Optional[int] = 3,
    ) -> Dict[str, Any]:
        """Attach images referenced by related guidance into the loop context.

        Returns a dict with keys:
            attached_count: int
            images: list of { meta: {...}, image: <base64> }
        """
        handles = self._get_image_handles_for_function_guidance(
            function_id=function_id,
            limit=limit,
        )
        images: List[Dict[str, Any]] = []
        for h in handles:
            try:
                raw_bytes = h.raw()
            except Exception:
                continue
            import base64

            b64 = base64.b64encode(raw_bytes).decode("utf-8")
            images.append(
                {
                    "meta": {
                        "image_id": int(h.image_id),
                        "caption": h.caption,
                        "timestamp": getattr(h.timestamp, "isoformat", lambda: "")(),
                    },
                    "image": b64,
                },
            )
        return {"attached_count": len(images), "images": images}

    # ------------------------------------------------------------------ #
    #  Virtual Environment Management                                    #
    # ------------------------------------------------------------------ #

    def _safe_get_venv_logs(
        self,
        *,
        filter: Optional[str] = None,
        limit: Optional[int] = None,
        exclude_fields: Optional[List[str]] = None,
        from_fields: Optional[List[str]] = None,
    ) -> List[db.Log]:
        """Best-effort venv reads; treat missing contexts as empty."""
        import time as _time

        last_exc: Exception | None = None
        for delay in (0.0, 0.05, 0.15):
            if delay:
                _time.sleep(delay)
            try:
                logs = db.get_logs(
                    context=self._venvs_ctx,
                    filter=filter,
                    limit=limit,
                    exclude_fields=exclude_fields,
                    from_fields=from_fields,
                )
                if logs or filter is None:
                    return logs
            except _UnifyRequestError as e:
                status = getattr(getattr(e, "response", None), "status_code", None)
                if status == 404:
                    last_exc = e
                    continue
                raise
            except Exception as e:
                last_exc = e
                break

        if isinstance(last_exc, _UnifyRequestError):
            status = getattr(getattr(last_exc, "response", None), "status_code", None)
            if status == 404:
                return []
        if last_exc is not None:
            raise last_exc
        return []

    def add_venv(self, *, venv: str) -> int:
        """
        Add a new virtual environment configuration.

        Args:
            venv: The pyproject.toml content as a string.

        Returns:
            The auto-assigned venv_id.
        """
        result = unity_create_logs(
            context=self._venvs_ctx,
            entries=[{"venv": venv}],
            stamp_authoring=True,
        )
        # unity_create_logs can return either a dict or a list of Log objects
        if isinstance(result, list) and len(result) > 0:
            # List of Log objects - can extract venv_id directly from entries
            log = result[0]
            if hasattr(log, "entries"):
                venv_id = log.entries.get("venv_id")
                if venv_id is not None:
                    return venv_id
        elif isinstance(result, dict):
            log_ids = result.get("log_event_ids", [])
            if log_ids:
                logs = self._safe_get_venv_logs(
                    filter=f"id == {log_ids[0]}",
                    limit=1,
                )
                if logs and hasattr(logs[0], "entries"):
                    venv_id = logs[0].entries.get("venv_id")
                    if venv_id is not None:
                        return venv_id
        raise RuntimeError("Failed to retrieve venv_id after creation")

    def get_venv(self, *, venv_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a virtual environment by its ID.

        Args:
            venv_id: The unique identifier of the virtual environment.

        Returns:
            Dict with venv_id and venv content, or None if not found.
        """
        for context in self._read_venv_contexts():
            logs = (
                self._safe_get_venv_logs(
                    filter=f"venv_id == {venv_id}",
                    limit=1,
                    exclude_fields=list_private_fields(context),
                )
                if context == self._venvs_ctx
                else db.get_logs(
                    context=context,
                    filter=f"venv_id == {venv_id}",
                    limit=1,
                    exclude_fields=list_private_fields(context),
                )
            )
            if logs:
                return logs[0].entries
        return None

    def list_venvs(self) -> List[Dict[str, Any]]:
        """
        List all virtual environments.

        Returns:
            List of dicts, each with venv_id and venv content.
        """
        logs = []
        for context in self._read_venv_contexts():
            logs.extend(
                (
                    self._safe_get_venv_logs(
                        exclude_fields=list_private_fields(context),
                        from_fields=None,
                    )
                    if context == self._venvs_ctx
                    else db.get_logs(
                        context=context,
                        exclude_fields=list_private_fields(context),
                    )
                ),
            )
        return [lg.entries for lg in logs]

    def delete_venv(self, *, venv_id: int) -> bool:
        """
        Delete a virtual environment by its ID.

        Functions referencing this venv will have their venv_id set to None
        (falling back to the default environment) via the foreign key cascade.

        Args:
            venv_id: The unique identifier of the virtual environment.

        Returns:
            True if deleted, False if not found.
        """
        logs = self._safe_get_venv_logs(
            filter=f"venv_id == {venv_id}",
            limit=1,
        )
        if not logs:
            return False
        db.delete_logs(
            context=self._venvs_ctx,
            logs=[logs[0].id],
        )
        return True

    def update_venv(self, *, venv_id: int, venv: str) -> bool:
        """
        Update the content of an existing virtual environment.

        Args:
            venv_id: The unique identifier of the virtual environment.
            venv: The new pyproject.toml content.

        Returns:
            True if updated, False if not found.
        """
        logs = self._safe_get_venv_logs(
            filter=f"venv_id == {venv_id}",
            limit=1,
        )
        if not logs:
            return False
        db.update_logs(
            context=self._venvs_ctx,
            logs=[logs[0].id],
            entries={"venv": venv},
            overwrite=True,
        )
        # A different environment is different content for every function in it.
        self.invalidate_trust_for_venv(int(venv_id))
        return True

    def set_function_venv(
        self,
        *,
        function_id: int,
        venv_id: Optional[int],
    ) -> bool:
        """
        Set the virtual environment for a function.

        Args:
            function_id: The function to update.
            venv_id: The venv_id to associate, or None for default environment.

        Returns:
            True if updated, False if function not found.
        """
        log = self._get_log_by_function_id(
            function_id=function_id,
            raise_if_missing=False,
        )
        if log is None:
            return False
        db.update_logs(
            context=self._compositional_ctx,
            logs=[log.id],
            entries={"venv_id": venv_id},
            overwrite=True,
        )
        self.invalidate_trust([int(function_id)])
        return True

    def get_function_venv(self, *, function_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the virtual environment associated with a function.

        Args:
            function_id: The function to query.

        Returns:
            The venv dict if the function has one, None if using default,
            or raises ValueError if function not found.
        """
        log = self._get_log_by_function_id(
            function_id=function_id,
            raise_if_missing=True,
        )
        venv_id = log.entries.get("venv_id")
        if venv_id is None:
            return None
        return self.get_venv(venv_id=venv_id)

    # ------------------------------------------------------------------ #
    #  Virtual Environment Execution Support                             #
    # ------------------------------------------------------------------ #

    def _get_venv_base_dir(self) -> Path:
        """Get the base directory for all custom venvs.

        The path includes the Unify context name to ensure isolation between
        different assistants/users and during parallel test runs.
        """
        from unify.file_manager.settings import get_local_root

        # Get current context for isolation
        ctx = db.get_active_context()
        ctx_name = ctx.get("read") or ctx.get("write") or "default"
        # Sanitize context name for filesystem use
        safe_ctx = ctx_name.replace("/", "_").replace("\\", "_")
        return Path(get_local_root()) / ".unity" / "venvs" / safe_ctx

    def _get_venv_dir(self, venv_id: int) -> Path:
        """Get the directory for a specific venv."""
        return self._get_venv_base_dir() / str(venv_id)

    def _get_venv_python(self, venv_id: int) -> Path:
        """Get the path to the Python interpreter for a venv."""
        return self._get_venv_dir(venv_id) / ".venv" / "bin" / "python"

    def _get_venv_runner_path(self, venv_id: int) -> Path:
        """Get the path to the runner script for a venv."""
        return self._get_venv_dir(venv_id) / "venv_runner.py"

    def _get_runner_script_content(self) -> str:
        """Get the content of the standalone runner script."""
        runner_path = Path(__file__).parent / "venv_runner.py"
        return runner_path.read_text()

    def is_venv_ready(self, *, venv_id: int) -> bool:
        """
        Check if a virtual environment is ready for execution.

        Args:
            venv_id: The venv to check.

        Returns:
            True if the venv exists and is synced, False otherwise.
        """
        venv_data = self.get_venv(venv_id=venv_id)
        if venv_data is None:
            return False

        venv_dir = self._get_venv_dir(venv_id)
        pyproject_path = venv_dir / "pyproject.toml"
        python_path = self._get_venv_python(venv_id)
        runner_path = self._get_venv_runner_path(venv_id)

        # Check if all required files exist
        if not pyproject_path.exists() or not python_path.exists():
            return False

        # Check if pyproject.toml content matches (normalize line endings)
        stored_content = venv_data["venv"].strip()
        disk_content = pyproject_path.read_text().strip()
        if disk_content != stored_content:
            return False

        # Check if runner script exists
        if not runner_path.exists():
            return False

        return True

    async def prepare_venv(self, *, venv_id: int) -> Path:
        """
        Ensure a virtual environment is created and synced.

        This method is idempotent - if the venv already exists and is up-to-date,
        it returns immediately. Otherwise, it creates/updates the venv.

        Args:
            venv_id: The venv to prepare.

        Returns:
            Path to the Python interpreter in the venv.

        Raises:
            ValueError: If the venv_id does not exist.
            RuntimeError: If venv creation fails.
        """
        venv_data = self.get_venv(venv_id=venv_id)
        if venv_data is None:
            raise ValueError(f"VirtualEnv with ID {venv_id} not found")

        venv_content = venv_data["venv"]
        venv_dir = self._get_venv_dir(venv_id)
        # Concurrent first executions in one venv must not all run
        # ``uv venv``: the second one fails on the directory the first created.
        lock = _VENV_PREPARE_LOCKS.setdefault(str(venv_dir), asyncio.Lock())
        async with lock:
            return await self._prepare_venv_locked(
                venv_id=venv_id,
                venv_content=venv_content,
                venv_dir=venv_dir,
            )

    async def _prepare_venv_locked(
        self,
        *,
        venv_id: int,
        venv_content: str,
        venv_dir: Path,
    ) -> Path:
        pyproject_path = venv_dir / "pyproject.toml"
        python_path = self._get_venv_python(venv_id)
        runner_path = self._get_venv_runner_path(venv_id)

        # Check if already ready
        needs_sync = False
        if pyproject_path.exists():
            if pyproject_path.read_text().strip() != venv_content.strip():
                needs_sync = True
                logger.info(f"Venv {venv_id}: pyproject.toml changed, re-syncing")
        else:
            needs_sync = True
            logger.info(f"Venv {venv_id}: creating new venv")

        if needs_sync or not python_path.exists():
            # Create directory and write pyproject.toml
            venv_dir.mkdir(parents=True, exist_ok=True)
            pyproject_path.write_text(venv_content)

            import shutil as _shutil
            import sys as _sys

            uv_bin = _shutil.which("uv")
            if uv_bin is None:
                try:
                    # NOTE: don't call `.resolve()` here. In venvs, `sys.executable` is
                    # often a symlink to the system Python, and resolving it would lose
                    # the venv bin directory (where `uv` is installed).
                    candidate = Path(_sys.executable).parent / "uv"
                    if candidate.exists():
                        uv_bin = str(candidate)
                except Exception:
                    uv_bin = None

            if uv_bin is None:
                raise RuntimeError(
                    "Failed to sync venv because the 'uv' executable was not found. "
                    "Install uv (recommended) or ensure it is available on PATH.",
                )

            # Two-step venv setup:
            #
            #   1. `uv venv <venv_dir>/.venv` — creates the .venv at the
            #      EXACT path Python will later import from. Passing the
            #      explicit target path (rather than relying on
            #      `--directory` + uv's "current project" discovery) is
            #      defensive: an earlier `--directory <venv_dir>` form
            #      returned exit code 0 on Linux CI but produced no
            #      `.venv/bin/python`, causing a downstream
            #      FileNotFoundError in subprocess.create_subprocess_exec.
            #      Naming the target path leaves no ambiguity.
            #
            #   2. `uv sync --directory <venv_dir>` installs project +
            #      deps into the freshly-created `.venv`. uv discovers
            #      the .venv automatically when run from the project
            #      directory.
            #
            # The original `cwd=str(venv_dir)` race ("Current directory
            # does not exist" when a sibling tmux session rmtree'd a
            # shared parent's cwd inode) is avoided here too: cwd is set
            # to the just-mkdir'd venv_dir, AND uv's --directory flag is
            # passed to make uv chdir before any cwd-dependent work.
            venv_target = venv_dir / ".venv"
            uv_steps: list[tuple[str, list[str]]] = [
                (
                    "venv",
                    [
                        uv_bin,
                        "venv",
                        str(venv_target),
                        "--directory",
                        str(venv_dir),
                    ],
                ),
                (
                    "sync",
                    [
                        uv_bin,
                        "sync",
                        "--directory",
                        str(venv_dir),
                        # The synthetic pyproject.toml we generate is
                        # NOT a real installable package — it only
                        # declares `dependencies = [...]`. Without
                        # this flag uv tries to install the project
                        # itself in editable mode, fails to find a
                        # build backend / sdist, and raises
                        # "Distribution not found at: file:///.../<venv_dir>".
                        # We only want the *dependencies* installed
                        # into the venv; the project itself is just
                        # a manifest.
                        "--no-install-project",
                    ],
                ),
            ]
            for label, cmd in uv_steps:
                logger.info(f"Venv {venv_id}: running 'uv {label}'...")
                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(venv_dir),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await process.communicate()
                logger.info(
                    f"Venv {venv_id}: 'uv {label}' rc={process.returncode}; "
                    f"stdout={stdout.decode().strip()!r}; "
                    f"stderr={stderr.decode().strip()!r}",
                )

                if process.returncode != 0:
                    error_msg = stderr.decode() if stderr else stdout.decode()
                    raise RuntimeError(
                        f"Failed to 'uv {label}' venv {venv_id}: {error_msg}",
                    )

            # Verify the venv layout we expect actually exists.
            # uv has been observed to return 0 from `uv venv` without
            # materializing the .venv (CI race / disk pressure / etc.) —
            # fail loud HERE with a focused error rather than later when
            # subprocess.create_subprocess_exec tries to invoke
            # `.venv/bin/python` and bubbles a generic FileNotFoundError.
            if not python_path.exists():
                raise RuntimeError(
                    f"Failed to materialize venv {venv_id}: "
                    f"expected python at {python_path} but it does not "
                    f"exist after `uv venv` + `uv sync` both returned 0. "
                    f"venv_dir={venv_dir} venv_target={venv_target}",
                )

            logger.info(f"Venv {venv_id}: sync complete")

        # Ensure runner script is present and up-to-date
        runner_content = self._get_runner_script_content()
        if not runner_path.exists() or runner_path.read_text() != runner_content:
            runner_path.write_text(runner_content)
            logger.info(f"Venv {venv_id}: runner script installed")

        return python_path

    async def _handle_rpc_call(
        self,
        path: str,
        kwargs: Dict[str, Any],
        primitives: Optional[Any] = None,
    ) -> Any:
        """
        Handle an RPC call from a subprocess.

        Every out-of-process execution path — one-shot venv, pooled venv, and
        shell — converges here, so this is where a steering session sees a
        subprocess's dispatches: while a call is in flight they are memoised
        for replay, pause holds the reply, and a pending correction raises
        :class:`ControlledInterruption` for the caller to translate into an
        ``rpc_interrupt`` message.

        Args:
            path: The RPC path (e.g., "contacts.ask", "computer.click")
            kwargs: The keyword arguments for the call
            primitives: The Primitives instance for state manager access

        Returns:
            The result of the RPC call
        """

        async def _dispatch() -> Any:
            return await self._dispatch_rpc_path(
                path=path,
                kwargs=kwargs,
                primitives=primitives,
            )

        return await dispatch_with_steering(
            active_session(),
            path,
            kwargs,
            _dispatch,
        )

    async def _dispatch_rpc_path(
        self,
        *,
        path: str,
        kwargs: Dict[str, Any],
        primitives: Optional[Any],
    ) -> Any:
        """Resolve one RPC path against the runtime and primitives and call it."""
        parts = path.split(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid RPC path: {path}")

        manager_name, method_name = parts

        if manager_name == "runtime" and method_name == "query_llm":
            from unify.common.reasoning import query_llm

            return self._make_json_serializable(await query_llm(**kwargs))

        if manager_name == "runtime" and method_name == "list_llms":
            from unify.common.reasoning import list_llms

            return list_llms(provider=kwargs.get("provider"))

        # Handle state manager primitives
        if primitives is None:
            raise RuntimeError("primitives not available")

        manager = getattr(primitives, manager_name, None)
        if manager is None:
            raise AttributeError(f"primitives has no manager '{manager_name}'")

        method = getattr(manager, method_name, None)
        if method is None:
            raise AttributeError(
                f"primitives.{manager_name} has no method '{method_name}'",
            )

        if asyncio.iscoroutinefunction(method):
            return await method(**kwargs)
        return method(**kwargs)

    async def execute_in_venv(
        self,
        *,
        venv_id: int,
        implementation: str,
        call_kwargs: Optional[Dict[str, Any]] = None,
        is_async: bool = True,
        initial_state: Optional[Dict[str, Any]] = None,
        primitives: Optional[Any] = None,
        env_overlay: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a function implementation in a custom virtual environment.

        This method:
        1. Ensures the venv is prepared (lazy creation on first use)
        2. Spawns a subprocess with the venv's Python interpreter
        3. Handles bidirectional RPC for primitives
        4. Returns the result from the subprocess

        While a steering session is in flight, each RPC reply doubles as a
        checkpoint and a correction re-runs the (patched) source in a fresh
        subprocess, replaying already-completed dispatches from the parent's
        cache.

        Args:
            venv_id: The virtual environment to use.
            implementation: The function source code.
            call_kwargs: Keyword arguments to pass to the function.
            is_async: Whether the function is async (default True).
            initial_state: Optional serialized state to inject before execution.
                Used for read_only mode to inherit state from a persistent session.
            primitives: The Primitives instance for RPC access to state managers.

        Returns:
            Dict with keys: result, error, stdout, stderr

        Raises:
            ValueError: If venv_id does not exist.
            RuntimeError: If execution fails.
        """
        call_kwargs = call_kwargs or {}

        # Ensure venv is ready
        python_path = await self.prepare_venv(venv_id=venv_id)
        runner_path = self._get_venv_runner_path(venv_id)

        env_overlay = env_overlay or {}

        # Execute in subprocess with bidirectional communication
        # Use start_new_session=True to create a new process group, allowing
        # us to kill all child processes (including multiprocessing workers)
        # with a single os.killpg() call.
        # Note: start_new_session is not supported on Windows
        use_process_group = sys.platform != "win32"

        # Diagnostic: prepare_venv just returned this python_path and
        # verified .exists() before returning. If the file is GONE by
        # the time we get here (CI race / external rmtree), bail with
        # a structured error rather than letting subprocess raise
        # FileNotFoundError with no surrounding state.
        if not python_path.exists():
            # Walk up the path tree and note which components exist.
            # If a high-level ancestor (e.g. `~/.unity/venvs/`) is
            # missing, the culprit is something rmtree-ing the
            # `unify/Local/.unity/` tree as a whole. If only the venv-
            # id leaf is missing, suspect per-test cleanup.
            ancestor_status: list[str] = []
            cursor: Path | None = python_path
            while cursor is not None and str(cursor) not in ("/", ""):
                ancestor_status.append(
                    f"{cursor.exists()}={cursor}",
                )
                next_cursor = cursor.parent
                if next_cursor == cursor:
                    break
                cursor = next_cursor

            venv_dir = python_path.parent.parent.parent
            parent_listing = "<not present>"
            if venv_dir.exists():
                try:
                    parent_listing = ", ".join(
                        sorted(p.name for p in venv_dir.iterdir()),
                    )
                except OSError as e:
                    parent_listing = f"<iterdir failed: {e}>"

            # The grandparent (the safe_ctx-keyed dir containing venv
            # ids) is the most informative — if THAT is gone too, the
            # whole venvs/<ctx>/ subtree was wiped. If it exists with
            # OTHER venv-id subdirs, only THIS venv-id was wiped.
            gp_listing = "<not present>"
            gp = venv_dir.parent
            if gp.exists():
                try:
                    gp_listing = ", ".join(sorted(p.name for p in gp.iterdir()))
                except OSError as e:
                    gp_listing = f"<iterdir failed: {e}>"

            try:
                import os as _os_diag

                cwd_str = _os_diag.getcwd()
            except Exception as e:
                cwd_str = f"<getcwd failed: {e}>"

            import os as _os_diag2

            home_str = _os_diag2.environ.get("HOME", "<unset>")
            pid_str = _os_diag2.getpid()

            raise RuntimeError(
                f"execute_in_venv: venv python disappeared between "
                f"prepare_venv() (which verified existence) and "
                f"create_subprocess_exec(). "
                f"venv_id={venv_id} pid={pid_str} cwd={cwd_str} "
                f"HOME={home_str}\n"
                f"  python_path={python_path}\n"
                f"  venv_dir={venv_dir} exists={venv_dir.exists()}\n"
                f"  venv_dir contents=[{parent_listing}]\n"
                f"  grandparent={gp} exists={gp.exists()}\n"
                f"  grandparent contents=[{gp_listing}]\n"
                f"  ancestor existence (deepest first): {ancestor_status}",
            )

        from unify.function_manager.execution_env import (
            sandbox_env as build_sandbox_env,
        )

        steering = active_session()

        async def _attempt(source: str) -> Dict[str, Any]:
            """Run one subprocess attempt at *source*, relaying its RPC."""
            execute_payload: Dict[str, Any] = {
                "type": "execute",
                "implementation": (
                    _instrument_for_child(source) if steering is not None else source
                ),
                "call_kwargs": call_kwargs,
                "is_async": is_async,
                "env_overlay": env_overlay,
            }
            if initial_state is not None:
                execute_payload["initial_state"] = initial_state

            process = await asyncio.create_subprocess_exec(
                str(python_path),
                str(runner_path),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=use_process_group,
                env=build_sandbox_env(),
            )

            async def _send(message: Dict[str, Any]) -> None:
                process.stdin.write((json.dumps(message) + "\n").encode())
                await process.stdin.drain()

            # Send initial execution request
            await _send(execute_payload)

            # Handle bidirectional communication
            stderr_output = []

            async def read_stderr():
                """Read stderr in background."""
                while True:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    stderr_output.append(line.decode())

            stderr_task = asyncio.create_task(read_stderr())
            # Set when a correction interrupted this attempt; the child's
            # completion is then an unwind to discard, not a result.
            interrupted: Optional[ControlledInterruption] = None
            # Corrections that land between dispatches reach the child
            # through the control channel, not through an RPC reply.
            watcher = (
                asyncio.create_task(
                    steering.relay_corrections(
                        source,
                        lambda request: _send(interrupt_directive(request)),
                    ),
                )
                if steering is not None
                else None
            )
            pause_watcher = (
                asyncio.create_task(
                    steering.relay_pause(
                        lambda paused: self._set_process_paused(
                            process,
                            use_process_group=use_process_group,
                            paused=paused,
                        ),
                    ),
                )
                if steering is not None
                else None
            )

            try:
                while True:
                    # Read next message from subprocess
                    line = await process.stdout.readline()
                    if not line:
                        # Process ended without sending complete message
                        await stderr_task
                        if interrupted is not None:
                            raise interrupted
                        return {
                            "result": None,
                            "error": "Subprocess ended unexpectedly",
                            "stdout": "",
                            "stderr": "".join(stderr_output),
                        }

                    try:
                        msg = json.loads(line.decode().strip())
                    except json.JSONDecodeError:
                        continue  # Skip malformed lines

                    msg_type = msg.get("type")

                    if msg_type == "rpc_call":
                        # Handle RPC call from subprocess
                        request_id = msg.get("id")

                        try:
                            result = await self._handle_rpc_call(
                                path=msg.get("path", ""),
                                kwargs=msg.get("kwargs", {}),
                                primitives=primitives,
                            )
                            response = {
                                "type": "rpc_result",
                                "id": request_id,
                                "result": self._make_json_serializable(result),
                            }
                        except ControlledInterruption as interruption:
                            # The child is blocked on this reply, so telling
                            # it to unwind here is the interrupt probe
                            # realised without instrumentation.
                            interrupted = interruption
                            response = {
                                "type": "rpc_interrupt",
                                "id": request_id,
                                "reason": str(interruption),
                            }
                        except Exception as e:
                            response = {
                                "type": "rpc_error",
                                "id": request_id,
                                "error": str(e),
                            }

                        await _send(response)

                    elif msg_type == "complete":
                        # Subprocess finished
                        await stderr_task
                        if interrupted is not None:
                            raise interrupted
                        child_interrupted = msg.get("interrupted")
                        if child_interrupted:
                            # The child unwound at an instrumented checkpoint;
                            # discard the attempt and retry.
                            raise ControlledInterruption(child_interrupted)
                        return {
                            "result": msg.get("result"),
                            "error": msg.get("error"),
                            "stdout": msg.get("stdout", ""),
                            "stderr": msg.get("stderr", "") + "".join(stderr_output),
                        }

            except (asyncio.CancelledError, ControlledInterruption):
                # Cancellation unwinds to the caller and an interruption to
                # the retry loop, both after cleanup in the finally block.
                raise
            except Exception as e:
                return {
                    "result": None,
                    "error": f"RPC error: {e}",
                    "stdout": "",
                    "stderr": "".join(stderr_output),
                }
            finally:
                for task in (watcher, pause_watcher):
                    if task is None:
                        continue
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        # A watcher can lose the race with the run ending;
                        # the attempt's own outcome stands.
                        logger.debug(
                            "steering: subprocess watcher failed",
                            exc_info=True,
                        )

                # Cancel stderr reader task
                stderr_task.cancel()
                try:
                    await stderr_task
                except asyncio.CancelledError:
                    pass

                # Ensure process and all its children are terminated
                if process.returncode is None:
                    await self._terminate_process_group(process, use_process_group)

        # One-shot subprocesses give a retry a clean slate: each attempt is a
        # fresh child, and the parent-side cache is what carries the completed
        # prefix across attempts.
        if steering is None:
            return await _attempt(implementation)
        try:
            return await run_with_steering(
                implementation,
                _attempt,
                session=steering,
            )
        except ExecutionStopped as stopped:
            return {
                "result": stopped.outcome,
                "error": None,
                "stdout": "",
                "stderr": "",
            }

    @staticmethod
    async def _set_process_paused(
        process: asyncio.subprocess.Process,
        *,
        use_process_group: bool,
        paused: bool,
    ) -> None:
        """Freeze or thaw a subprocess with SIGSTOP/SIGCONT.

        OS-level pause is what makes pause mean pause out-of-process: it holds
        the child wherever it is — mid-loop, mid-sleep, even inside blocking
        sync code no checkpoint can reach — where in-process pause can only
        hold at the next checkpoint. Resuming a process that never stopped is
        harmless, so callers thaw unconditionally on the way out; a frozen
        child would otherwise sit on SIGTERM forever. No-op on Windows, which
        has no stop signal.
        """
        if sys.platform == "win32" or process.returncode is not None:
            return
        sig = signal.SIGSTOP if paused else signal.SIGCONT
        try:
            if use_process_group and process.pid is not None:
                os.killpg(os.getpgid(process.pid), sig)
            else:
                process.send_signal(sig)
        except (ProcessLookupError, OSError):
            # The process ended while the pause state was changing.
            pass

    @staticmethod
    async def _terminate_process_group(
        process: asyncio.subprocess.Process,
        use_process_group: bool,
    ) -> None:
        """
        Terminate a subprocess and all its children (process group).

        Sends SIGTERM first for graceful shutdown, then SIGKILL if the process
        doesn't terminate within the timeout. A stopped (SIGSTOP) process is
        continued first so the termination signal can be delivered.

        Args:
            process: The subprocess to terminate.
            use_process_group: Whether the process was started with start_new_session=True.
        """
        await FunctionManager._set_process_paused(
            process,
            use_process_group=use_process_group,
            paused=False,
        )
        try:
            if use_process_group and process.pid is not None:
                # Kill the entire process group (subprocess + all its children)
                try:
                    pgid = os.getpgid(process.pid)
                    # Send SIGTERM for graceful shutdown
                    os.killpg(pgid, signal.SIGTERM)
                except (ProcessLookupError, OSError):
                    # Process already dead or no permission
                    pass
            else:
                # Fall back to terminating just the main process
                process.terminate()

            # Wait for process to terminate
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                # Process didn't terminate gracefully, force kill
                if use_process_group and process.pid is not None:
                    try:
                        pgid = os.getpgid(process.pid)
                        os.killpg(pgid, signal.SIGKILL)
                    except (ProcessLookupError, OSError):
                        pass
                else:
                    process.kill()
                # Wait for kill to complete
                try:
                    await asyncio.wait_for(process.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass
        except Exception:
            # Best effort cleanup - don't let cleanup errors propagate
            pass

    async def execute_function(
        self,
        *,
        function_name: str,
        call_kwargs: Optional[Dict[str, Any]] = None,
        target_venv_id: Optional[int] = ...,
        state_mode: Literal["stateful", "read_only", "stateless"] = "stateless",
        session_id: int = 0,
        venv_pool: Optional["VenvPool"] = None,
        shell_pool: Optional["ShellPool"] = None,
        extra_namespaces: Optional[Dict[str, Any]] = None,
        _parent_chat_context: Optional[list] = None,
    ) -> Any:
        """
        Execute a stored function by name with optional state mode overrides.

        This method looks up a function by name from the function table and
        executes it. It automatically routes to the appropriate executor based
        on the function's type:

        - **Primitives** (``is_primitive=True``): Resolved to their live
          callable via ``get_primitive_callable`` and invoked directly. The
          raw return value is passed through unmodified, which is critical
          for primitives that return ``SteerableToolHandle`` instances.
        - **Composed functions**: Executed via subprocess or in-process exec
          and wrapped in a ``{"result", "error", "stdout", "stderr"}`` dict.

        State modes (composed functions only):
        - "stateless" (default): Fresh subprocess with no inherited state. Pure
          function behavior. Backward compatible with previous behavior.
        - "stateful": Uses persistent pool connection. Variables from previous
          executions persist. Requires venv_pool (Python) or shell_pool (shell).
        - "read_only": Reads current state from pool but executes in ephemeral
          subprocess. Changes are NOT persisted. Useful for "what-if" exploration.

        Args:
            function_name: Name of the function to execute.
            call_kwargs: Keyword arguments to pass to the function.
            target_venv_id: Override the execution environment (Python only):
                - ... (Ellipsis): Use the function's stored venv_id (default)
                - None: Execute in the default Python environment
                - int: Execute in this specific venv_id
            state_mode: How to handle global state ("stateful", "read_only", "stateless").
            session_id: The session ID within the pool (default 0). Multiple sessions
                allow independent stateful execution contexts.
                Only applies to stateful/read_only modes.
            venv_pool: VenvPool for stateful/read_only modes with Python venv functions.
            shell_pool: ShellPool for stateful/read_only modes with shell functions.
            extra_namespaces: Named objects to inject into the function's execution
                namespace. For in-process execution, all entries are injected into
                globals. For venv/subprocess execution, the "primitives" entry
                (including primitives.computer) is bridged via RPC.

        Returns:
            For composed functions: dict with keys result, error, stdout, stderr.
            For primitives: the raw return value of the callable (may be a
            SteerableToolHandle or any other type).

        Raises:
            ValueError: If the function doesn't exist or has no implementation.
            ValueError: If state_mode requires a pool but none is provided.

        Anti-patterns:
            - Nesting ``asyncio.run(...)`` inside sync helpers called from
              offline Jobs / actor sandboxes (already under a running loop).
              Prefer ``async def`` + ``await``, or ``run_coro_sync``.
        """
        ns = extra_namespaces or {}
        # Look up function by name (compositional first, then optionally primitives).
        func_data = self._get_function_data_by_name(name=function_name)

        if func_data is None and self._include_primitives:
            func_data = self._get_primitive_data_by_name(name=function_name)

        if func_data is None and self._include_primitives:
            func_data = self._get_stored_primitive_data_by_name(name=function_name)

        if func_data is None:
            raise ValueError(f"Function '{function_name}' not found")

        # Direct executions (sub-agents, proxy re-entry)
        # feed the usage trace here; primitives are filtered inside.
        self._note_function_use(func_data)

        # Primitive execution: resolve callable and invoke directly.
        if func_data.get("is_primitive"):
            return await self._execute_primitive(
                func_data=func_data,
                call_kwargs=call_kwargs,
                extra_namespaces=ns,
                _parent_chat_context=_parent_chat_context,
            )

        implementation = func_data.get("implementation")
        if not isinstance(implementation, str) or not implementation.strip():
            raise ValueError(f"Function '{function_name}' has no implementation")

        # Check language and route appropriately
        language = func_data.get("language", "python")

        if language == "python":
            checker = self._tier0_checker(func_data)
            named_kwargs = dict(call_kwargs or {})
            if checker.active:
                checker.check_input(named_kwargs)
            outcome = await self._execute_python_function(
                func_data=func_data,
                implementation=implementation,
                call_kwargs=call_kwargs,
                target_venv_id=target_venv_id,
                state_mode=state_mode,
                session_id=session_id,
                venv_pool=venv_pool,
                extra_namespaces=ns,
                _parent_chat_context=_parent_chat_context,
            )
            if (
                checker.active
                and isinstance(outcome, dict)
                and not outcome.get("error")
            ):
                checker.check_output(result=outcome.get("result"), kwargs=named_kwargs)
            return outcome
        elif language in ("bash", "zsh", "sh", "powershell"):
            return await self._execute_shell_function(
                func_data=func_data,
                implementation=implementation,
                call_kwargs=call_kwargs,
                state_mode=state_mode,
                session_id=session_id,
                shell_pool=shell_pool,
                extra_namespaces=ns,
            )
        else:
            raise ValueError(f"Unsupported function language: {language}")

    # ------------------------------------------------------------------ #
    #  Primitive Execution Helpers                                       #
    # ------------------------------------------------------------------ #

    def _get_primitive_data_by_name(self, *, name: str) -> Optional[Dict[str, Any]]:
        """Look up primitive metadata by name from the in-memory registry."""
        primitives = self._registry.collect_primitives(self._primitive_scope)
        return primitives.get(name)

    def _get_stored_primitive_data_by_name(
        self,
        *,
        name: str,
        provider_backed_only: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Look up a primitive row by exact name from readable primitive contexts."""
        try:
            name_filter = normalize_filter_expr(f"name == {json.dumps(name)}")
        except Exception:
            name_filter = f"name == {json.dumps(name)}"
        if provider_backed_only:
            name_filter = (
                f'({name_filter}) and (metadata["source"] == "provider_backed")'
            )
        rows = self._primitive_logs(extra_filter=name_filter, limit=1)
        if rows:
            return dict(rows[0])
        return None

    async def _execute_primitive(
        self,
        *,
        func_data: Dict[str, Any],
        call_kwargs: Optional[Dict[str, Any]],
        extra_namespaces: Dict[str, Any],
        _parent_chat_context: Optional[list] = None,
    ) -> Any:
        """Resolve a primitive callable and invoke it directly.

        Returns the raw result of the callable (which may be a
        SteerableToolHandle for async tool loop primitives).
        """
        from unify.function_manager.primitives.runtime import get_primitive_callable

        callable_fn = get_primitive_callable(
            func_data,
            primitives=extra_namespaces.get("primitives"),
        )
        if callable_fn is None:
            raise ValueError(
                f"Could not resolve primitive callable for '{func_data.get('name')}'",
            )

        kwargs = call_kwargs or {}

        # Forward _parent_chat_context if the callable accepts it.
        if _parent_chat_context is not None:
            sig = inspect.signature(callable_fn)
            params = sig.parameters
            if "_parent_chat_context" in params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
            ):
                kwargs["_parent_chat_context"] = _parent_chat_context

        result = callable_fn(**kwargs)
        if inspect.isawaitable(result):
            result = await result
        return result

    # ------------------------------------------------------------------ #
    #  Remote Windows Execution Helpers                                  #
    # ------------------------------------------------------------------ #

    # Remote Windows local root (matches LOCAL_ROOT in agent-service)
    # Both default to ~/Unity/Local; on Windows VMs this is C:\Unity\Local
    REMOTE_WINDOWS_LOCAL_ROOT = "C:\\Unity\\Local"

    async def _execute_python_function(
        self,
        *,
        func_data: Dict[str, Any],
        implementation: str,
        call_kwargs: Optional[Dict[str, Any]],
        target_venv_id: Optional[int],
        state_mode: Literal["stateful", "read_only", "stateless"],
        session_id: int,
        venv_pool: Optional["VenvPool"],
        extra_namespaces: Dict[str, Any],
        _parent_chat_context: Optional[list] = None,
    ) -> Dict[str, Any]:
        """Execute a Python function with venv and state mode support."""
        # Strip @custom_function decorators (not available in subprocess runner)

        # Determine execution target venv
        if target_venv_id is ...:
            # Use function's default venv_id
            exec_venv_id = func_data.get("venv_id")
        else:
            # User override
            exec_venv_id = target_venv_id

        # Determine if function is async
        is_async = "async def" in implementation

        call_kwargs = call_kwargs or {}

        # Extract RPC-bridgeable namespaces for subprocess execution paths.
        primitives = extra_namespaces.get("primitives")

        # Handle execution based on venv and state_mode
        if exec_venv_id is None:
            # No venv - execute in default environment with state_mode support
            return await self._execute_in_default_env(
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                state_mode=state_mode,
                session_id=session_id,
                extra_namespaces=extra_namespaces,
                _parent_chat_context=_parent_chat_context,
            )

        # Venv execution - state_mode matters
        venv_id = int(exec_venv_id)

        if state_mode == "stateful":
            # Use persistent connection via VenvPool
            if venv_pool is None:
                raise ValueError(
                    "state_mode='stateful' requires venv_pool for venv functions. "
                    "Either provide venv_pool or use state_mode='stateless'.",
                )
            return await venv_pool.execute_in_venv(
                venv_id=venv_id,
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                session_id=session_id,
                primitives=primitives,
                function_manager=self,
            )

        elif state_mode == "read_only":
            # Get state from persistent connection, execute in ephemeral subprocess
            if venv_pool is None:
                raise ValueError(
                    "state_mode='read_only' requires venv_pool to read existing state. "
                    "Either provide venv_pool or use state_mode='stateless'.",
                )
            # Get current state from the persistent connection
            initial_state = await venv_pool.get_connection_state(
                venv_id=venv_id,
                function_manager=self,
                session_id=session_id,
            )
            # Execute in fresh subprocess with that state (not modifying persistent state)
            return await self.execute_in_venv(
                venv_id=venv_id,
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                initial_state=initial_state,
                primitives=primitives,
            )

        else:  # state_mode == "stateless"
            # Fresh subprocess with no inherited state
            return await self.execute_in_venv(
                venv_id=venv_id,
                implementation=implementation,
                call_kwargs=call_kwargs,
                is_async=is_async,
                primitives=primitives,
            )

    async def _execute_shell_function(
        self,
        *,
        func_data: Dict[str, Any],
        implementation: str,
        call_kwargs: Optional[Dict[str, Any]],
        state_mode: Literal["stateful", "read_only", "stateless"],
        session_id: int,
        shell_pool: Optional["ShellPool"],
        extra_namespaces: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Execute a shell function with state mode support.

        For shell functions:
        - "stateless": Uses execute_shell_script (fresh subprocess each time)
        - "stateful": Uses ShellPool for persistent sessions
        - "read_only": Not yet implemented (requires state snapshot/restore)
        """

        language = func_data.get("language", "bash")

        if state_mode == "stateless":
            # Use existing execute_shell_script (fresh subprocess each time)
            return await self.execute_shell_script(
                implementation=implementation,
                language=language,
                primitives=extra_namespaces.get("primitives"),
            )

        elif state_mode == "stateful":
            if shell_pool is None:
                raise ValueError(
                    "state_mode='stateful' requires shell_pool for shell functions. "
                    "Either provide shell_pool or use state_mode='stateless'.",
                )

            # Execute in persistent session via ShellPool
            result = await shell_pool.execute(
                language=language,
                command=implementation,
                session_id=session_id,
            )

            return {
                "result": result.exit_code,  # For shell, "result" is exit code
                "error": result.error,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

        elif state_mode == "read_only":
            # Get state from persistent session, execute in ephemeral session
            if shell_pool is None:
                raise ValueError(
                    "state_mode='read_only' requires shell_pool to read existing state. "
                    "Either provide shell_pool or use state_mode='stateless'.",
                )

            from .shell_session import ShellSession

            # Get current state from the persistent session
            session = await shell_pool.get_session(
                language=language,
                session_id=session_id,
            )
            state = await session.snapshot_state()

            # Execute in fresh ephemeral session with restored state
            ephemeral = ShellSession(language=language)
            try:
                await ephemeral.start()
                restore_result = await ephemeral.restore_state(state)
                if restore_result.error:
                    return {
                        "result": -1,
                        "error": f"Failed to restore state: {restore_result.error}",
                        "stdout": "",
                        "stderr": "",
                    }

                # Execute the command in ephemeral session
                result = await ephemeral.execute(implementation)

                return {
                    "result": result.exit_code,
                    "error": result.error,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                }
            finally:
                await ephemeral.close()

    async def _execute_in_default_env(
        self,
        *,
        implementation: str,
        call_kwargs: Dict[str, Any],
        is_async: bool,
        state_mode: Literal["stateful", "read_only", "stateless"] = "stateless",
        session_id: int = 0,
        extra_namespaces: Optional[Dict[str, Any]] = None,
        _parent_chat_context: Optional[list] = None,
    ) -> Dict[str, Any]:
        """
        Execute a function in the default Python environment (no custom venv).

        This runs the function in-process using the project's Python environment.

        State modes:
        - stateless: Fresh globals each time (pure function behavior)
        - stateful: Persistent globals per session_id (Jupyter-notebook style)
        - read_only: Reads existing state but doesn't persist changes
        """
        from .execution_env import create_base_globals
        import io
        import traceback
        from contextlib import redirect_stdout, redirect_stderr

        # Determine which globals dict to use based on state_mode
        if state_mode == "stateful":
            # Use persistent session globals
            if session_id not in self._in_process_sessions:
                self._in_process_sessions[session_id] = create_base_globals()
            globals_dict = self._in_process_sessions[session_id]
        elif state_mode == "read_only":
            # Copy state from persistent session into fresh globals
            globals_dict = create_base_globals()
            if session_id in self._in_process_sessions:
                # Copy user-defined state (excluding base globals and dunder names)
                base_keys = set(create_base_globals().keys())
                for key, value in self._in_process_sessions[session_id].items():
                    if key not in base_keys and not key.startswith("_"):
                        # Shallow copy - sufficient for most state
                        globals_dict[key] = value
        else:  # stateless
            globals_dict = create_base_globals()

        # Inject all extra namespaces into globals (always, since they may
        # change between calls).
        if extra_namespaces:
            globals_dict.update(extra_namespaces)

        # Wrap primitives with ContextForwardingProxy so that environment
        # methods called from composed functions receive _parent_chat_context
        # (mirroring the PythonExecutionSession.execute() pattern).
        _orig_prims = globals_dict.get("primitives")
        _wrapped_prims = _orig_prims
        if _parent_chat_context is not None and _wrapped_prims is not None:
            from .primitives.context_proxy import ContextForwardingProxy

            _wrapped_prims = ContextForwardingProxy(
                _wrapped_prims,
                _parent_chat_context=_parent_chat_context,
            )

        # Steering, when a call is in flight. This path runs a stored function
        # directly rather than through the sandbox, so without its own probes
        # the same function would be correctable or not depending purely on
        # which route reached it.
        steering = active_session()
        steering_token = None
        if steering is not None:
            steering_token = bind_session(
                globals_dict,
                steering,
                tool_namespaces=[],
            )
            if _wrapped_prims is not None:
                # Outermost, so memoisation sees a dispatch before context
                # forwarding does.
                _wrapped_prims = MemoisedDispatch(_wrapped_prims, steering)

        if _wrapped_prims is not _orig_prims:
            globals_dict["primitives"] = _wrapped_prims

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        result = None
        error = None

        async def _run_implementation(source: str) -> Any:
            """Define the function from *source* and call it.

            Name and asyncness are re-derived per attempt: a correction
            rewrites the definition, and nothing guarantees the replacement
            keeps the shape the caller was told about.
            """
            attempt_tree = ast.parse(source)
            definition = next(
                (
                    node
                    for node in attempt_tree.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                ),
                None,
            )
            if definition is None:
                raise ValueError("No function definition found in implementation")

            to_exec = source
            if steering is not None:
                to_exec = ast.unparse(
                    instrument(
                        attempt_tree,
                        tool_namespaces=set(DEFAULT_TOOL_NAMESPACES),
                    ),
                )

            exec(compile_function_source(definition.name, to_exec), globals_dict)
            fn = globals_dict.get(definition.name)
            if fn is None:
                raise ValueError(
                    f"Function '{definition.name}' not found after exec",
                )
            if isinstance(definition, ast.AsyncFunctionDef) or is_async:
                return await fn(**call_kwargs)
            return fn(**call_kwargs)

        try:
            with (
                redirect_stdout(
                    stdout_capture,
                ),
                redirect_stderr(stderr_capture),
            ):
                if steering is None:
                    result = await _run_implementation(implementation)
                else:
                    result = await run_with_steering(
                        implementation,
                        _run_implementation,
                        session=steering,
                    )
        except ExecutionStopped as stopped:
            result = stopped.outcome
        except Exception:
            error = traceback.format_exc()
        finally:
            # Stateful sessions reuse this dict across calls, so the probes and
            # the memoised namespace must not outlive the call that installed
            # them.
            if _wrapped_prims is not _orig_prims:
                if _orig_prims is None:
                    globals_dict.pop("primitives", None)
                else:
                    globals_dict["primitives"] = _orig_prims
            if steering_token is not None:
                restore_session(globals_dict, steering_token)

        return {
            "result": self._make_json_serializable(result),
            "error": error,
            "stdout": stdout_capture.getvalue(),
            "stderr": stderr_capture.getvalue(),
        }

    def _make_json_serializable(self, obj: Any) -> Any:
        """Convert an object to a JSON-serializable form."""
        if obj is None or isinstance(obj, (bool, int, float, str)):
            return obj
        if isinstance(obj, (list, tuple)):
            return [self._make_json_serializable(item) for item in obj]
        if isinstance(obj, dict):
            return {str(k): self._make_json_serializable(v) for k, v in obj.items()}
        # Handle Pydantic models
        try:
            from pydantic import BaseModel

            if isinstance(obj, BaseModel):
                return self._make_json_serializable(obj.model_dump())
        except ImportError:
            pass
        # For other types, convert to string representation
        return str(obj)

    # ────────────────────────────────────────────────────────────────────────────
    # Shell Script Execution with Primitives Bridge
    # ────────────────────────────────────────────────────────────────────────────

    def _get_shell_interpreter(self, language: str) -> List[str]:
        """
        Get the shell interpreter command for a given language.

        Args:
            language: One of "sh", "bash", "zsh", "powershell"

        Returns:
            List of command args to invoke the interpreter
        """
        interpreters = {
            "sh": ["/bin/sh"],
            "bash": ["/bin/bash"],
            "zsh": ["/bin/zsh"],
            "powershell": ["pwsh", "-NoProfile", "-NonInteractive", "-File"],
        }
        if language not in interpreters:
            raise ValueError(f"Unsupported shell language: {language}")
        return interpreters[language]

    def _get_primitives_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about available primitives for shell script introspection.

        Returns:
            Dict with structure:
            {
                "managers": {
                    "files": {
                        "description": "...",
                        "methods": {
                            "search_files": {"signature": "...", "docstring": "..."},
                            ...
                        }
                    },
                    ...
                }
            }
        """
        result: Dict[str, Dict[str, Any]] = {"managers": {}}

        # Use the scoped primitive_scope from this FunctionManager
        for spec in self._registry.manager_specs(self._primitive_scope):
            manager_name = spec.manager_alias
            description = spec.prompt_text(
                self._primitive_scope.scoped_managers,
            ).description

            # Get primitive rows which contain signature and docstring
            single_scope = PrimitiveScope(scoped_managers=frozenset({manager_name}))
            primitives_dict = self._registry.collect_primitives(single_scope)

            methods_info: Dict[str, Dict[str, str]] = {}
            for row in primitives_dict.values():
                method_name = row.get("primitive_method", "")
                methods_info[method_name] = {
                    "signature": row.get("argspec", ""),
                    "docstring": row.get("docstring", ""),
                }

            result["managers"][manager_name] = {
                "description": description,
                "methods": methods_info,
            }

        return result

    async def execute_shell_script(
        self,
        *,
        implementation: str,
        language: Literal["sh", "bash", "zsh", "powershell"] = "sh",
        call_args: Optional[List[str]] = None,
        env: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        primitives: Optional[Any] = None,
        timeout: float = 300.0,
    ) -> Dict[str, Any]:
        """
        Execute a shell script with access to Unity primitives via RPC.

        This method runs a shell script in a subprocess while providing access
        to all Unity primitives (ContactManager, FileManager, etc.) via the
        `unity-primitive` CLI command.

        Shell scripts can call primitives like:
            result=$(unity-primitive files search_files --references '{"query": "budget"}')
            contacts=$(unity-primitive contacts ask --text "Find Alice")

        Args:
            implementation: The shell script source code.
            language: Shell interpreter to use ("sh", "bash", "zsh", "powershell").
            call_args: Optional list of positional arguments to pass to the script.
            env: Optional environment variables to add to the script's environment.
            cwd: Optional working directory for the script.
            primitives: The Primitives instance for RPC access to state managers.
            timeout: Maximum execution time in seconds (default 5 minutes).

        Returns:
            Dict with keys:
            - result: The script's exit code (0 = success)
            - error: Error message if execution failed, None otherwise
            - stdout: Captured stdout from the script
            - stderr: Captured stderr from the script
        """
        call_args = call_args or []

        # Create temporary directory for script and socket
        with tempfile.TemporaryDirectory(prefix="unity_shell_") as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Write script to temporary file
            if language == "powershell":
                script_path = tmpdir_path / "script.ps1"
            else:
                script_path = tmpdir_path / "script.sh"

            script_path.write_text(implementation)
            script_path.chmod(0o755)

            # Create Unix domain socket for RPC
            socket_path = tmpdir_path / "rpc.sock"

            # Get the path to unity-primitive CLI
            shell_runner_path = Path(__file__).parent / "shell_runner.py"

            # Build environment for the subprocess (sanitized: no raw provider
            # tokens, plus localhost proxy endpoints).
            from unify.function_manager.execution_env import (
                sandbox_env as build_sandbox_env,
            )

            script_env = build_sandbox_env()
            script_env["UNIFY_RPC_SOCKET"] = str(socket_path)
            # Add the shell_runner.py as unity-primitive command
            # We create a wrapper script that invokes python with shell_runner.py
            wrapper_path = tmpdir_path / "unity-primitive"
            python_path = sys.executable
            wrapper_path.write_text(
                f'#!/bin/sh\nexec "{python_path}" "{shell_runner_path}" "$@"\n',
            )
            wrapper_path.chmod(0o755)
            # Prepend tmpdir to PATH so unity-primitive is available
            script_env["PATH"] = f"{tmpdir}:{script_env.get('PATH', '')}"

            # Add user-provided environment variables
            if env:
                script_env.update(env)

            # Set up the RPC server (Unix domain socket)
            server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server_socket.bind(str(socket_path))
            server_socket.listen(5)
            server_socket.setblocking(False)

            # Start the shell script subprocess
            interpreter = self._get_shell_interpreter(language)
            cmd = interpreter + [str(script_path)] + call_args

            use_process_group = sys.platform != "win32"
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=script_env,
                cwd=cwd,
                start_new_session=use_process_group,
            )
            steering = active_session()
            if steering is not None:
                # Shell never reaches run_with_steering (there is no source to
                # splice), so the script is bound here for the patch author to
                # read when it decides between stopping and doing nothing.
                steering.bind_source(implementation)
            stopped: Optional[ExecutionStopped] = None

            async def stop_process(request: Any) -> None:
                """Terminate the shell process when a correction abandons it."""
                nonlocal stopped
                stopped = ExecutionStopped(request.reason or "steered")
                if process.returncode is None:
                    await self._terminate_process_group(process, use_process_group)

            stdout_output: List[str] = []
            stderr_output: List[str] = []

            async def read_stdout():
                """Read stdout in background."""
                while True:
                    line = await process.stdout.readline()
                    if not line:
                        break
                    stdout_output.append(line.decode())

            async def read_stderr():
                """Read stderr in background."""
                while True:
                    line = await process.stderr.readline()
                    if not line:
                        break
                    stderr_output.append(line.decode())

            async def handle_rpc_client(client_socket: socket.socket):
                """Handle a single RPC client connection."""
                loop = asyncio.get_event_loop()
                try:
                    # Read request
                    data = b""
                    while True:
                        try:
                            chunk = await asyncio.wait_for(
                                loop.sock_recv(client_socket, 4096),
                                timeout=1.0,
                            )
                            if not chunk:
                                break
                            data += chunk
                            if b"\n" in data:
                                break
                        except asyncio.TimeoutError:
                            if process.returncode is not None:
                                break
                            continue

                    if not data:
                        return

                    request = json.loads(data.decode("utf-8").strip())
                    request_id = request.get("id", "")
                    path = request.get("path", "")
                    kwargs = request.get("kwargs", {})

                    # Handle introspection requests
                    if path == "_introspect.list_primitives":
                        result = self._get_primitives_metadata()
                        response = {
                            "type": "rpc_result",
                            "id": request_id,
                            "result": result,
                        }
                    else:
                        # Handle regular RPC calls
                        try:
                            result = await self._handle_rpc_call(
                                path=path,
                                kwargs=kwargs,
                                primitives=primitives,
                            )
                            result = self._make_json_serializable(result)
                            response = {
                                "type": "rpc_result",
                                "id": request_id,
                                "result": result,
                            }
                        except ControlledInterruption:
                            request = (
                                steering.interruption if steering is not None else None
                            )
                            if request is None or not request.stop:
                                raise
                            await stop_process(request)
                            return
                        except Exception as e:
                            logger.error(f"RPC error for {path}: {e}", exc_info=True)
                            response = {
                                "type": "rpc_error",
                                "id": request_id,
                                "error": str(e),
                            }

                    # Send response
                    response_data = (json.dumps(response) + "\n").encode("utf-8")
                    await loop.sock_sendall(client_socket, response_data)

                finally:
                    client_socket.close()

            async def accept_rpc_connections():
                """Accept and handle RPC connections from shell script."""
                loop = asyncio.get_event_loop()
                while process.returncode is None:
                    try:
                        client_socket, _ = await asyncio.wait_for(
                            loop.sock_accept(server_socket),
                            timeout=0.1,
                        )
                        # Handle client in background
                        asyncio.create_task(handle_rpc_client(client_socket))
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        if process.returncode is None:
                            logger.debug(f"RPC accept error: {e}")
                        break

            # Start all tasks
            stdout_task = asyncio.create_task(read_stdout())
            stderr_task = asyncio.create_task(read_stderr())
            rpc_task = asyncio.create_task(accept_rpc_connections())
            watcher = (
                asyncio.create_task(
                    steering.relay_corrections(implementation, stop_process),
                )
                if steering is not None
                else None
            )
            pause_watcher = (
                asyncio.create_task(
                    steering.relay_pause(
                        lambda paused: self._set_process_paused(
                            process,
                            use_process_group=use_process_group,
                            paused=paused,
                        ),
                    ),
                )
                if steering is not None
                else None
            )

            try:
                # Wait for process to complete with timeout
                try:
                    await asyncio.wait_for(process.wait(), timeout=timeout)
                except asyncio.TimeoutError:
                    # Process timed out
                    await self._terminate_process_group(process, use_process_group)
                    return {
                        "result": -1,
                        "error": f"Shell script timed out after {timeout}s",
                        "stdout": "".join(stdout_output),
                        "stderr": "".join(stderr_output),
                    }

                # Wait for stdout/stderr to be fully read
                await asyncio.gather(stdout_task, stderr_task)

                if stopped is not None:
                    return {
                        "result": stopped.outcome,
                        "error": None,
                        "stdout": "".join(stdout_output),
                        "stderr": "".join(stderr_output),
                    }

                # Build result
                exit_code = process.returncode
                return {
                    "result": exit_code,
                    "error": (
                        None
                        if exit_code == 0
                        else f"Script exited with code {exit_code}"
                    ),
                    "stdout": "".join(stdout_output),
                    "stderr": "".join(stderr_output),
                }

            except asyncio.CancelledError:
                await self._terminate_process_group(process, use_process_group)
                raise

            except Exception as e:
                return {
                    "result": -1,
                    "error": str(e),
                    "stdout": "".join(stdout_output),
                    "stderr": "".join(stderr_output),
                }

            finally:
                # Clean up
                for task in (watcher, pause_watcher):
                    if task is None:
                        continue
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass

                rpc_task.cancel()
                try:
                    await rpc_task
                except asyncio.CancelledError:
                    pass

                stdout_task.cancel()
                stderr_task.cancel()
                try:
                    await stdout_task
                except asyncio.CancelledError:
                    pass
                try:
                    await stderr_task
                except asyncio.CancelledError:
                    pass

                server_socket.close()

                # Ensure process is terminated
                if process.returncode is None:
                    await self._terminate_process_group(process, use_process_group)


def _wrap_compositional_write(method_name: str) -> None:
    original = getattr(FunctionManager, method_name)

    @functools.wraps(original)
    def wrapped(
        self: FunctionManager,
        *args: Any,
        destination: str | None = None,
        **kwargs: Any,
    ):
        try:
            context = self._function_context_for_destination(
                FUNCTIONS_COMPOSITIONAL_TABLE,
                destination=destination,
            )
        except ToolErrorException as exc:
            return exc.payload
        with self._temporary_function_context("_compositional_ctx", context):
            return original(self, *args, **kwargs)

    wrapped.__doc__ = (
        f"{original.__doc__ or ''}\n\n{FUNCTIONS_COMPOSITIONAL_DESTINATION_GUIDANCE}"
    )
    wrapped.__signature__ = _signature_with_destination(original)  # type: ignore[attr-defined]
    setattr(FunctionManager, method_name, wrapped)


def _wrap_venv_write(method_name: str) -> None:
    original = getattr(FunctionManager, method_name)

    @functools.wraps(original)
    def wrapped(
        self: FunctionManager,
        *args: Any,
        destination: str | None = None,
        **kwargs: Any,
    ):
        try:
            context = self._function_context_for_destination(
                FUNCTIONS_VENVS_TABLE,
                destination=destination,
            )
        except ToolErrorException as exc:
            return exc.payload
        with self._temporary_function_context("_venvs_ctx", context):
            return original(self, *args, **kwargs)

    wrapped.__doc__ = (
        f"{original.__doc__ or ''}\n\n{FUNCTIONS_VENV_DESTINATION_GUIDANCE}"
    )
    wrapped.__signature__ = _signature_with_destination(original)  # type: ignore[attr-defined]
    setattr(FunctionManager, method_name, wrapped)


def _signature_with_destination(method: Callable[..., Any]) -> inspect.Signature:
    # follow_wrapped=False: the concrete methods carry @functools.wraps(Base...),
    # and following the chain would resolve to the abstract signature, silently
    # dropping concrete-only parameters (e.g. add_functions' ``overwrite``) from
    # the LLM-visible tool schema.
    signature = inspect.signature(method, follow_wrapped=False)
    if "destination" in signature.parameters:
        return signature
    parameters = list(signature.parameters.values())
    destination_param = inspect.Parameter(
        "destination",
        inspect.Parameter.KEYWORD_ONLY,
        default=None,
        annotation=str | None,
    )
    insert_at = len(parameters)
    for index, parameter in enumerate(parameters):
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            insert_at = index
            break
    parameters.insert(insert_at, destination_param)
    return signature.replace(parameters=parameters)


for _method_name in (
    "add_functions",
    "delete_function",
    "set_function_venv",
):
    _wrap_compositional_write(_method_name)

for _method_name in (
    "add_venv",
    "delete_venv",
    "update_venv",
):
    _wrap_venv_write(_method_name)
