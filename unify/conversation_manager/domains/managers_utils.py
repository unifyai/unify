import asyncio
from time import perf_counter
from typing import TYPE_CHECKING

import unify

from unify.logger import LOGGER
from unify.common.startup_timing import log_startup_timing
from unify.common.hierarchical_logger import DEFAULT_ICON, ICONS
from unify.settings import SETTINGS
from unify.session_details import SESSION_DETAILS
from unify.common.async_tool_loop import SteerableToolHandle
from unify.contact_manager.types.contact import UNASSIGNED
from unify.conversation_manager.event_broker import get_event_broker
from unify.conversation_manager.events import (
    ActorClarificationRequest,
    ActorNotification,
    ActorResult,
    ActorSessionResponse,
    Event,
    GetChatHistory,
    InitializationComplete,
    LogMessageResponse,
    StoreChatHistory,
    UnifyMessageSent,
)
from unify.events.event_bus import EVENT_BUS
from unify.manager_registry import ManagerRegistry
from unify.function_manager.primitives import Primitives, default_runtime_scope
from unify.conversation_manager.cm_types import Medium

if TYPE_CHECKING:
    from unify.actor.base import BaseActor
    from unify.conversation_manager.conversation_manager import ConversationManager

event_broker = get_event_broker()


def ensure_runtime_context(*, strict: bool = False) -> str:
    """Rebind runtime context in this task and refresh ContextRegistry base."""
    from unify.common.runtime_context import bind_runtime_context_root

    return bind_runtime_context_root(strict=strict)


# EVENT BUS
async def get_last_store_chat_history() -> StoreChatHistory:
    _t0 = perf_counter()
    bus_events = await EVENT_BUS.search(
        filter='type == "Comms" and payload_cls == "StoreChatHistory"',
        limit=1,
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] managers.get_last_store_chat_history duration=%.2fs events=%d",
        perf_counter() - _t0,
        len(bus_events),
    )
    if len(bus_events):
        return Event.from_bus_event(bus_events[0])
    return None


def _get_sender_name(contact: dict | None) -> str:
    """Extract display name from a contact dict."""
    if not contact:
        return "Unknown"
    first_name = contact.get("first_name", "")
    surname = contact.get("surname", "")
    name = f"{first_name} {surname}".strip()
    return name or contact.get("email_address", "") or "Unknown"


# Event types that produce global-thread entries during hydration.
_MESSAGE_PRODUCING_EVENTS = {
    "UnifyMessageReceived",
    "UnifyMessageSent",
}


async def run_boot_hydration(cm: "ConversationManager") -> int:
    """Hydrate the global thread, then reopen the slow-brain render gate.

    The gate must reopen on every outcome — restored history, an empty
    store, or a failed search — because a turn held at the gate degrades to
    the pre-hydration view after its bounded wait anyway; keeping the gate
    closed past hydration buys nothing but latency.
    """
    try:
        return await hydrate_global_thread(cm)
    finally:
        cm._hydration_gate.set()


async def hydrate_global_thread(cm: "ConversationManager") -> int:
    """Populate the shared global deque from persisted EventBus Comms events.

    Called after initialization to restore conversation state from the previous
    session.  Hydrated (historical) messages are prepended to the global thread
    so that any messages that arrived during initialization keep their correct
    chronological position at the end.

    Returns the number of messages restored — zero both when there is
    genuinely no prior conversation and when the deployment does not persist
    the Comms stream. The caller records it so the initialization-complete
    notification can tell the brain the truth about what was loaded.
    """
    from unify.conversation_manager.domains.contact_index import ContactIndex

    deque_size = (
        cm.contact_index.global_thread.maxlen or ContactIndex.DEFAULT_GLOBAL_THREAD_SIZE
    )

    _t0 = perf_counter()
    bus_events = await EVENT_BUS.search(
        filter='type == "Comms"',
        limit=deque_size,
    )
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] managers.hydrate_global_thread.search duration=%.2fs events=%d limit=%d",
        perf_counter() - _t0,
        len(bus_events),
        deque_size,
    )

    if not bus_events:
        LOGGER.info(
            f"{ICONS['managers_worker']} [Hydration] No Comms events found, skipping hydration",
        )
        return 0

    # Bus events come in descending order (most recent first), reverse for chronological
    bus_events.reverse()

    # Build entries into a buffer via build_message (no append to the live
    # deque), so we can prepend them all at once and preserve chronological
    # ordering relative to any messages that arrived during initialization.
    hydrated_entries: list = []

    _t0 = perf_counter()
    for bus_event in bus_events:
        payload_cls = bus_event.payload_cls
        # Strip module prefix if present (e.g., "unify.conversation_manager.events.UnifyMessageSent")
        if "." in payload_cls:
            payload_cls = payload_cls.rsplit(".", 1)[-1]

        if payload_cls not in _MESSAGE_PRODUCING_EVENTS:
            continue

        try:
            cm_event = Event.from_bus_event(bus_event)
        except Exception:
            continue

        contact = getattr(cm_event, "contact", None) or {}
        contact_id = contact.get("contact_id")
        if contact_id is None:
            continue

        hydrated_entries.append(
            cm.contact_index.build_message(
                contact_id=contact_id,
                sender_name=_get_sender_name(contact),
                message_content=cm_event.content,
                role="user" if payload_cls == "UnifyMessageReceived" else "assistant",
                timestamp=cm_event.timestamp,
                attachments=getattr(cm_event, "attachments", None),
            ),
        )

    restored = len(hydrated_entries)
    # Prepend hydrated entries so historical messages appear before any
    # messages that arrived during initialization.
    cm.contact_index.prepend_entries(hydrated_entries)
    log_startup_timing(
        LOGGER,
        "⏱️ [StartupTiming] managers.hydrate_global_thread.render duration=%.2fs restored=%d events=%d",
        perf_counter() - _t0,
        restored,
        len(bus_events),
    )

    LOGGER.info(
        f"{ICONS['managers_worker']} [Hydration] Restored {restored} messages from {len(bus_events)} Comms events",
    )
    return restored


async def publish_bus_events(event):
    try:
        event_name = event.__class__.__name__
        bus_event = event.to_bus_event()
        LOGGER.debug(f"{DEFAULT_ICON} Publishing bus event {event_name}")
        await EVENT_BUS.publish(bus_event)
    except Exception as e:
        LOGGER.error(
            f"{ICONS['managers_worker']} [ManagersWorker] Error publishing bus event: {e}",
        )


# ACTOR
async def actor_watch_result(
    handle_id: int,
    handle: SteerableToolHandle,
    *,
    action_type: str = "",
) -> None:
    """Await final result and publish completion (or failure), then cleanup."""
    resolved_action_type = action_type or "act"
    try:
        result = await handle.result()
    except Exception as exc:
        error_text = f"Error getting actor result: {exc}"
        LOGGER.error(f"{ICONS['managers_worker']} [ManagersWorker] {error_text}")
        await event_broker.publish(
            "app:actor:result",
            ActorResult(
                handle_id=handle_id,
                success=False,
                result=None,
                error=error_text,
                action_type=resolved_action_type,
            ).to_json(),
        )
        return

    success = True
    error_text: str | None = None
    if isinstance(result, dict) and "error_kind" in result:
        success = False
        error_text = str(result.get("message") or result.get("error_kind"))
    elif isinstance(result, str):
        stripped_result = result.lstrip()
        if stripped_result[:5].lower() == "error":
            success = False
            error_text = result

    await event_broker.publish(
        "app:actor:result",
        ActorResult(
            handle_id=handle_id,
            success=success,
            result=result,
            error=error_text,
            action_type=resolved_action_type,
        ).to_json(),
    )


async def actor_watch_notifications(
    handle_id: int,
    handle: SteerableToolHandle,
) -> None:
    """Forward notifications and responses from the handle until it completes.

    The handle's notification queue carries two kinds of messages:

    - **``type="notification"``** — progress updates emitted by the
      ``send_notification`` tool (or the active-work heartbeat) while the
      actor is still working.
    - **``type="response"``** — turn-complete signals emitted when a
      persistent session enters its wait state. These mean the actor has
      finished the current turn and is awaiting the next ``interject``.

    Each type is published as a distinct CM event so the brain can tell
    them apart.

    Drain guarantee: a notification enqueued in the instant before
    ``handle.done()`` flips to ``True`` must still be observed -- the
    producer side always enqueues before signalling completion, but the
    ``while not handle.done()`` check races that signal. Once ``done()``
    is observed ``True``, keep polling with a short timeout (instead of
    exiting immediately) until the queue is actually empty.
    """
    _DRAIN_TIMEOUT = 0.1

    while True:
        already_done = handle.done()
        try:
            notif = await asyncio.wait_for(
                handle.next_notification(),
                timeout=_DRAIN_TIMEOUT if already_done else 30,
            )
        except asyncio.TimeoutError:
            if already_done:
                break
            continue

        # An empty payload means "nothing to report", never a real
        # notification — treat it as end-of-stream once the handle is done.
        # The explicit yield guards against a handle whose
        # ``next_notification`` completes synchronously: ``wait_for`` awaits
        # the coroutine inline, so without it this loop would never suspend
        # and would starve the entire event loop.
        if not notif:
            if already_done:
                break
            await asyncio.sleep(0)
            continue

        # Determine whether this is a turn-complete response or a progress
        # notification. The loop emits responses with {"type": "response", ...}.
        is_response = isinstance(notif, dict) and notif.get("type") == "response"

        if is_response:
            content = str(notif.get("content", ""))
            await event_broker.publish(
                "app:actor:session_response",
                ActorSessionResponse(
                    handle_id=handle_id,
                    content=content,
                ).to_json(),
            )
        else:
            # Extract a human-friendly message.
            #
            # Contract:
            # - Notifications may be plain strings (already display-ready), OR
            # - Structured dict payloads (recommended: include both "type" and "message").
            #
            # Fallback chain: "message" → "result_summary" → "type" → JSON dump.
            # "result_summary" is checked before "type" because step_complete
            # payloads carry their useful content in that field, not "message".
            msg: str
            if isinstance(notif, dict):
                if notif.get("message") is not None:
                    msg = str(notif.get("message"))
                elif notif.get("result_summary") is not None:
                    msg = str(notif.get("result_summary"))
                elif notif.get("type") is not None:
                    msg = str(notif.get("type"))
                else:
                    try:
                        import json as _json

                        msg = _json.dumps(notif, ensure_ascii=False, default=str)
                    except Exception:
                        msg = str(notif)
            else:
                msg = str(notif)

            completed = (
                bool(notif.get("completed", False))
                if isinstance(notif, dict)
                else False
            )
            kind = notif.get("type", "") if isinstance(notif, dict) else ""
            await event_broker.publish(
                "app:actor:notification",
                ActorNotification(
                    handle_id=handle_id,
                    response=msg,
                    completed=completed,
                    kind=kind,
                ).to_json(),
            )


async def actor_watch_clarifications(
    handle_id: int,
    handle: SteerableToolHandle,
) -> None:
    """Forward clarifications to CM until handle completes."""
    while not handle.done():
        # await clarification request
        try:
            clar = await asyncio.wait_for(handle.next_clarification(), timeout=30)
        except asyncio.TimeoutError:
            continue

        # get question and call id
        q = clar.get("question") if isinstance(clar, dict) else str(clar)
        call_id = clar.get("call_id") if isinstance(clar, dict) else None

        # publish clarification request
        await event_broker.publish(
            "app:actor:clarification_request",
            ActorClarificationRequest(
                handle_id=handle_id,
                query=q,
                call_id=call_id,
            ).to_json(),
        )


# TRANSCRIPTS

# Exchange-metadata key the conversation key is stored under.
_CONVERSATION_KEY_FIELD = "conversation_key"


def _conversation_key(contact_id: int) -> str:
    """The key that groups every chat message with one contact into one exchange.

    Inbound and outbound resolve to the same key, so an assistant reply lands
    in the exchange it answers, with no inactivity window.
    """
    return f"{Medium.UNIFY_MESSAGE.value}:dm:{contact_id}"


def _recover_exchange_id(
    cm: "ConversationManager",
    conversation_key: str,
) -> int:
    """Find the existing exchange for ``conversation_key`` in Exchanges metadata.

    Runs only on an in-memory cache miss (the first message with a contact
    after a CM restart), so a conversation survives restarts without opening
    a duplicate exchange. Returns ``UNASSIGNED`` when no exchange exists yet.
    """
    escaped_key = conversation_key.replace('"', '\\"')
    try:
        result = cm.transcript_manager.filter_exchanges(
            filter=(
                f'medium == "{Medium.UNIFY_MESSAGE.value}" '
                f'and metadata["{_CONVERSATION_KEY_FIELD}"] == "{escaped_key}"'
            ),
            limit=1,
        )
    except Exception:
        return UNASSIGNED
    exchanges = result.get("exchanges") or []
    if not exchanges:
        return UNASSIGNED
    return exchanges[0].exchange_id


async def log_message(cm: "ConversationManager", event: Event) -> None:
    """Log a chat message via TranscriptManager."""
    ensure_runtime_context()
    event_name = event.__class__.__name__
    LOGGER.debug(f"{DEFAULT_ICON} publishing transcript {event_name}")
    medium = Medium.UNIFY_MESSAGE
    role = "Assistant" if isinstance(event, UnifyMessageSent) else "User"
    contact_id = event.contact["contact_id"]
    if role == "Assistant":
        sender_id, receiver_ids = SESSION_DETAILS.self_contact_id, [contact_id]
    else:
        sender_id, receiver_ids = contact_id, [SESSION_DETAILS.self_contact_id]

    # Derived on every message, not only when the exchange is unknown: it is
    # also stamped onto the exchange as the join key that makes the
    # conversation recoverable after a restart.
    conversation_key = _conversation_key(contact_id)
    exchange_id = cm._conversation_exchange_ids.get(conversation_key, UNASSIGNED)
    if exchange_id == UNASSIGNED:
        exchange_id = _recover_exchange_id(cm, conversation_key)

    def _publish_transcript() -> int:
        nonlocal exchange_id
        try:
            LOGGER.debug(
                f"{ICONS['managers_worker']} [ManagersWorker] Logging message: {event.to_dict()}",
            )
            msg_data = {
                "medium": medium,
                "sender_id": sender_id,
                "receiver_ids": receiver_ids,
                "timestamp": event.timestamp,
                "content": event.content,
            }
            attachments = getattr(event, "attachments", [])
            if attachments:
                msg_data["attachments"] = attachments

            if exchange_id == UNASSIGNED:
                exchange_id, _ = (
                    cm.transcript_manager.log_first_message_in_new_exchange(
                        msg_data,
                        exchange_initial_metadata={
                            "medium": medium.value,
                            _CONVERSATION_KEY_FIELD: conversation_key,
                        },
                    )
                )
            else:
                cm.transcript_manager.log_messages(
                    {**msg_data, "exchange_id": exchange_id},
                    synchronous=True,
                )

            LOGGER.debug(
                f"{ICONS['managers_worker']} [ManagersWorker] Logged message: {medium}"
                f" from {sender_id} to {receiver_ids}",
            )
            return exchange_id
        except Exception as e:
            LOGGER.error(
                f"{ICONS['managers_worker']} [ManagersWorker] Error logging message: {e}",
            )
            return UNASSIGNED

    exchange_id = await asyncio.to_thread(_publish_transcript)

    # Cache the exchange immediately so the next message in this conversation
    # reuses it. The LogMessageResponse handler runs asynchronously on the
    # event loop, by which time the worker may already have started the next
    # log_message call and would otherwise create a duplicate exchange.
    if exchange_id != UNASSIGNED:
        cm._conversation_exchange_ids[conversation_key] = exchange_id

    # publish reply as event envelope
    await event_broker.publish(
        "app:logging:message_logged",
        LogMessageResponse(
            medium=medium,
            exchange_id=exchange_id,
        ).to_json(),
    )
    LOGGER.debug(
        f"{ICONS['managers_worker']} [ManagersWorker] Published exchange_id {exchange_id}",
    )


# Contact updates


async def update_session_contacts(
    cm: "ConversationManager",
    assistant_first_name: str,
    assistant_surname: str,
    assistant_number: str,
    assistant_email: str,
    user_first_name: str,
    user_surname: str,
    user_number: str,
    user_email: str,
    assistant_job_title: str | None = None,
) -> None:
    """
    Update the resolved assistant and boss contacts in the ContactManager when
    session details change.
    """
    if cm.contact_manager is None:
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] Cannot update contacts: contact_manager is None",
        )
        return

    async def _update_contact(
        contact_id: int,
        first_name: str,
        surname: str,
        phone_number: str,
        email_address: str,
        job_title: str | None = None,
    ):
        try:
            kwargs: dict = dict(
                contact_id=contact_id,
                phone_number=phone_number,
                email_address=email_address,
                first_name=first_name,
                surname=surname,
            )
            if job_title is not None and contact_id == SESSION_DETAILS.self_contact_id:
                kwargs["job_title"] = job_title
            await asyncio.to_thread(
                cm.contact_manager.update_contact,
                **kwargs,
            )
            LOGGER.info(
                f"{ICONS['managers_worker']} [ManagersWorker] Updated contact {contact_id}: {first_name} {surname}",
            )
        except Exception as e:
            LOGGER.error(
                f"{ICONS['managers_worker']} [ManagersWorker] Failed to update contact {contact_id}: {e}",
            )

    await _update_contact(
        SESSION_DETAILS.self_contact_id,
        assistant_first_name,
        assistant_surname,
        assistant_number,
        assistant_email,
        assistant_job_title,
    )

    await _update_contact(
        SESSION_DETAILS.boss_contact_id,
        user_first_name,
        user_surname,
        user_number,
        user_email,
    )


# Queueing operations that need managers

_operations_queue = asyncio.Queue()
_module_loop: asyncio.AbstractEventLoop | None = None


def _adopt_running_loop() -> None:
    """Recreate module-level asyncio primitives when their owning loop died.

    The queue and init lock are process-global, but they bind to the event
    loop that first awaits them. An in-process reboot — a fresh
    ConversationManager on a new loop over the same durable world — must not
    inherit them: a bound queue makes the successor's operations listener die
    on its first ``get`` (RuntimeError: bound to a different event loop),
    after which every queued operation — EventBus persistence among them —
    silently accumulates unprocessed; a lock held by a task frozen on the
    dead loop blocks the successor's init outright. The predecessor's queued
    operations die with it, exactly as they would with its process. A live
    owning loop is never preempted.
    """
    global _operations_queue, _init_lock, _module_loop
    loop = asyncio.get_running_loop()
    owner = _module_loop
    if owner is loop:
        return
    if owner is not None and owner.is_running() and not owner.is_closed():
        return
    if owner is not None:
        _operations_queue = asyncio.Queue()
        _init_lock = asyncio.Lock()
    _module_loop = loop


# A backlog this deep means nothing is consuming the queue: the listener
# normally drains an operation in milliseconds, and producers enqueue one
# per handled event. Growth past this line is a missing/dead
# listen_to_operations, not load.
_OPERATIONS_QUEUE_BACKLOG_WARN_AT = 50
_operations_backlog_warned = False


async def queue_operation(async_func: callable, *args, **kwargs) -> None:
    """
    Queue an async operation to be executed when managers are initialized.
    The operation will be processed by listen_to_operations().
    """
    global _operations_backlog_warned
    _adopt_running_loop()
    await _operations_queue.put((async_func, args, kwargs))
    # An unbounded queue with no consumer fails silently: every enqueued
    # operation — EventBus persistence among them — simply never happens,
    # and nothing anywhere says so. Embedders that boot the CM without
    # spawning listen_to_operations have lost whole conversation streams
    # this way. One warning per backlog episode turns that black hole into
    # a grep-able line.
    depth = _operations_queue.qsize()
    if depth >= _OPERATIONS_QUEUE_BACKLOG_WARN_AT:
        if not _operations_backlog_warned:
            _operations_backlog_warned = True
            LOGGER.warning(
                f"{ICONS['managers_worker']} [ManagersWorker] Operations "
                f"queue backlog reached {depth} with nothing draining it — "
                "is listen_to_operations running? Queued work (EventBus "
                "persistence among it) is not being executed.",
            )
    elif depth < _OPERATIONS_QUEUE_BACKLOG_WARN_AT // 2:
        _operations_backlog_warned = False


async def wait_for_initialization(
    cm: "ConversationManager",
) -> None:
    """
    Wait for initialization to complete.

    Polls cm.initialized with no timeout. Initialization failures are
    surfaced by init_conv_manager itself (logged errors). A timeout here
    would silently kill the operations queue processor on slow cold starts,
    causing queued work to be orphaned.
    """
    while not cm.initialized:
        await asyncio.sleep(0.1)


async def listen_to_operations(cm: "ConversationManager") -> None:
    """
    Worker loop that processes queued operations once initialized.
    Should be started as a background task alongside init_conv_manager.
    """
    _adopt_running_loop()
    # Wait for initialization to complete
    await wait_for_initialization(cm)
    ensure_runtime_context()

    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Operations listener started, processing queue...",
    )

    # Process operations as they come in
    while True:
        # Wait for next operation (with timeout to allow checking for shutdown)
        try:
            async_func, args, kwargs = await asyncio.wait_for(
                _operations_queue.get(),
                timeout=1.0,
            )
        except asyncio.TimeoutError:
            continue

        # Execute the operation
        func_name = getattr(async_func, "__name__", str(async_func))
        try:
            await async_func(*args, **kwargs)
        except Exception as e:
            LOGGER.error(
                f"{ICONS['managers_worker']} [ManagersWorker] Error executing {func_name}: {e}",
            )
        finally:
            _operations_queue.task_done()


# Initialization

_init_lock = asyncio.Lock()


def _init_managers(
    cm: "ConversationManager",
    loop: asyncio.AbstractEventLoop,
    actor: "BaseActor | None" = None,
) -> None:
    """
    Initialize all managers in a separate thread.
    The main event loop is passed for managers that need to schedule async tasks.

    Args:
        cm: The ConversationManager instance to initialize.
        loop: The main event loop for scheduling async tasks.
        actor: Optional pre-instantiated Actor. If provided, used directly instead
            of creating one via ManagerRegistry. Useful for testing with specific
            Actor implementations.
    """
    start_time = perf_counter()

    # 0. Initialize the runtime (idempotent — SESSION_DETAILS.assistant.agent_id
    #    is already populated, so unify.init() reads it for the context root).
    LOGGER.debug(f"{ICONS['managers_worker']} [ManagersWorker] Initializing unify...")
    local_start_time = perf_counter()
    unify.init()
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Runtime initialized in "
        f"{perf_counter() - local_start_time:.2f} seconds",
    )

    # 1. Configure EventBus
    EVENT_BUS.set_window("Comms", 100)

    # 1b. Kick off hydration concurrently — it only needs unify.init() and
    # EventBus config (both done). Runs on the main event loop while the
    # remaining managers initialize in this thread. Completion reopens the
    # slow-brain render gate ``init_conv_manager`` closed, releasing any
    # turn held for a hydrated view without waiting for the rest of init.
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Starting concurrent hydration...",
    )
    cm._hydration_future = asyncio.run_coroutine_threadsafe(
        run_boot_hydration(cm),
        loop,
    )

    # 2. Initialize ContactManager (respects SETTINGS.contact.IMPL)
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Initializing ContactManager...",
    )
    local_start_time = perf_counter()
    cm.contact_manager = ManagerRegistry.get_contact_manager(
        description="production deployment",
    )
    # Wire up ContactManager to ContactIndex for always-fresh contact data
    cm.contact_index.set_contact_manager(cm.contact_manager)
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] ContactManager ({type(cm.contact_manager).__name__}) initialized in "
        f"{perf_counter() - local_start_time:.2f} seconds",
    )

    # 3. Initialize TranscriptManager (respects SETTINGS.transcript.IMPL)
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Initializing TranscriptManager...",
    )
    local_start_time = perf_counter()
    cm.transcript_manager = ManagerRegistry.get_transcript_manager(
        description="production deployment",
        contact_manager=cm.contact_manager,
    )
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] TranscriptManager ({type(cm.transcript_manager).__name__}) initialized in "
        f"{perf_counter() - local_start_time:.2f} seconds",
    )

    # 4. Initialize MemoryManager (optional - respects SETTINGS.memory.ENABLED and IMPL)
    if SETTINGS.memory.ENABLED:
        try:
            from unify.memory_manager.memory_manager import MemoryManager

            LOGGER.info(
                f"{ICONS['managers_worker']} [ManagersWorker] Initializing MemoryManager...",
            )
            local_start_time = perf_counter()
            mem_cfg = MemoryManager.MemoryConfig(
                contacts=SETTINGS.memory.CONTACTS,
                bios=SETTINGS.memory.BIOS,
                rolling_summaries=SETTINGS.memory.ROLLING_SUMMARIES,
                response_policies=SETTINGS.memory.RESPONSE_POLICIES,
                knowledge=SETTINGS.memory.KNOWLEDGE,
                tasks=SETTINGS.memory.TASKS,
            )
            cm.memory_manager = ManagerRegistry.get_memory_manager(
                transcript_manager=cm.transcript_manager,
                contact_manager=cm.contact_manager,
                config=mem_cfg,
                loop=loop,
            )
            LOGGER.info(
                f"{ICONS['managers_worker']} [ManagersWorker] MemoryManager initialized in "
                f"{perf_counter() - local_start_time:.2f} seconds",
            )
        except Exception as e:
            LOGGER.warning(
                f"{ICONS['managers_worker']} [ManagersWorker] MemoryManager init failed (degraded): {e}",
            )
    else:
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] MemoryManager disabled (SETTINGS.memory.ENABLED=False)",
        )

    # 5. Initialize ConversationManagerHandle (respects SETTINGS.conversation.IMPL)
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] Initializing ConversationManagerHandle...",
    )
    local_start_time = perf_counter()
    # ConversationManagerHandle has different constructor args for real vs simulated
    if SETTINGS.conversation.IMPL == "simulated":
        cm._conversation_manager_handle = (
            ManagerRegistry.get_conversation_manager_handle(
                description="production deployment",
                assistant_id=SESSION_DETAILS.assistant.agent_id,
                contact_id=str(SESSION_DETAILS.boss_contact_id),
            )
        )
    else:
        cm._conversation_manager_handle = (
            ManagerRegistry.get_conversation_manager_handle(
                event_broker=cm.event_broker,
                conversation_id=SESSION_DETAILS.assistant.agent_id,
                contact_id=str(SESSION_DETAILS.boss_contact_id),
                transcript_manager=cm.transcript_manager,
                conversation_manager=cm,
            )
        )
    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] ConversationManagerHandle ({type(cm._conversation_manager_handle).__name__}) initialized in "
        f"{perf_counter() - local_start_time:.2f} seconds",
    )

    # 6. Initialize Actor (use provided actor or create via ManagerRegistry)
    LOGGER.debug(f"{ICONS['managers_worker']} [ManagersWorker] Initializing Actor...")
    try:
        local_start_time = perf_counter()
        if actor is not None:
            # Use pre-instantiated actor (e.g., for testing)
            cm.actor = actor
        else:
            # Create via ManagerRegistry (respects SETTINGS.actor.IMPL)
            from unify.actor.environments import (
                StateManagerEnvironment,
                ActorEnvironment,
            )

            cm.actor = ManagerRegistry.get_actor(
                description="production deployment",
                environments=[
                    StateManagerEnvironment(
                        Primitives(primitive_scope=default_runtime_scope()),
                    ),
                    ActorEnvironment(),
                ],
            )
        actor_cls = type(cm.actor).__name__
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] Actor ({actor_cls}) initialized in "
            f"{perf_counter() - local_start_time:.2f} seconds",
        )
    except Exception as e:
        LOGGER.error(
            f"{ICONS['managers_worker']} [ManagersWorker] Error initializing Actor: {e}",
        )

    # 7. Initialize FileManager (eagerly, so the FileRecords context exists
    #    before any file operations or background tasks attempt to use it)
    try:
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] Initializing FileManager...",
        )
        local_start_time = perf_counter()
        fm = ManagerRegistry.get_file_manager()
        # Force the lazy DataManager property to resolve now while ContextVars
        # are correct.  The ingestion pipeline later accesses _data_manager from
        # ThreadPoolExecutor workers where ContextVars may not propagate — eager
        # init avoids the resulting empty-context / double-slash paths.
        _ = fm._data_manager  # noqa: F841
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] FileManager initialized in "
            f"{perf_counter() - local_start_time:.2f} seconds",
        )
    except Exception as e:
        LOGGER.warning(
            f"{ICONS['managers_worker']} [ManagersWorker] FileManager init failed (degraded): {e}",
        )

    LOGGER.info(
        f"{ICONS['managers_worker']} [ManagersWorker] All managers initialized in "
        f"{perf_counter() - start_time:.2f} seconds",
    )

    # 8. Static primitives live in the global builtins catalogue (seeded at
    #    start-up), so no per-assistant primitive sync is needed here.
    _init_fm = ManagerRegistry.get_function_manager()

    # 9. Pre-warm embedding columns for all managers (best-effort, avoids
    #    cold-start latency on the first vector search after a fresh hire).
    #    Also explicitly warm the FunctionManager (not in the singleton cache
    #    due to _force_new=True) so Primitives embeddings are ready.
    try:
        LOGGER.debug(
            f"{ICONS['managers_worker']} [ManagersWorker] Warming embedding columns...",
        )
        local_start_time = perf_counter()
        ManagerRegistry.warm_all_embeddings()
        _init_fm.warm_embeddings()
        LOGGER.info(
            f"{ICONS['managers_worker']} [ManagersWorker] Embedding columns warmed in "
            f"{perf_counter() - local_start_time:.2f} seconds",
        )
    except Exception as e:
        LOGGER.warning(
            f"{ICONS['managers_worker']} [ManagersWorker] Embedding warm-up failed (degraded): {e}",
        )


async def init_conv_manager(
    cm: "ConversationManager",
    *,
    actor: "BaseActor | None" = None,
) -> None:
    """
    Initialize all managers for the ConversationManager.
    All initialization runs in a separate thread (non-blocking).

    Args:
        cm: The ConversationManager instance to initialize.
        actor: Optional pre-instantiated Actor. If provided, used directly instead
            of creating one via ManagerRegistry. Useful for testing with specific
            Actor implementations (e.g., SimulatedActor).
    """
    LOGGER.debug(f"{ICONS['managers_worker']} [ManagersWorker] Processing startup")

    _adopt_running_loop()
    async with _init_lock:
        start_time = perf_counter()
        if cm.initialized:
            LOGGER.info(
                f"{ICONS['managers_worker']} [ManagersWorker] Already initialized, skipping",
            )
            return

        # Close the slow-brain render gate for the boot window. An inbound
        # that lands mid-boot still queues a turn, but that turn holds at
        # render time until hydration resolves (see
        # ``ConversationManager._run_llm``), so the first reply after a wake
        # never answers from an empty conversation view while the history
        # that would answer it is seconds from landing. ``run_boot_hydration``
        # reopens it; the ``finally`` below covers boots that die before the
        # hydration task ever gets created.
        cm._hydration_gate.clear()

        try:
            # Get the main event loop to pass to managers that need it
            loop = asyncio.get_running_loop()

            # Anchor the canonical session root before and after worker init.
            # unify.init() inside _init_managers sets the store ContextVars
            # (CONTEXT_READ/CONTEXT_WRITE) but asyncio.to_thread runs on a
            # copy of the caller's context — changes don't propagate back.
            # Re-apply the context afterwards so any lazily-created managers
            # in the main async context see the correct values.
            _t0 = perf_counter()
            ensure_runtime_context(strict=True)
            log_startup_timing(
                LOGGER,
                "⏱️ [StartupTiming] managers.init_conv_manager.pre_thread_context duration=%.2fs",
                perf_counter() - _t0,
            )

            _t0 = perf_counter()
            await asyncio.to_thread(_init_managers, cm, loop, actor)
            log_startup_timing(
                LOGGER,
                "⏱️ [StartupTiming] managers.init_conv_manager.to_thread duration=%.2fs",
                perf_counter() - _t0,
            )

            _t0 = perf_counter()
            ensure_runtime_context(strict=True)
            log_startup_timing(
                LOGGER,
                "⏱️ [StartupTiming] managers.init_conv_manager.reapply_context duration=%.2fs",
                perf_counter() - _t0,
            )

            store_chat_history = await get_last_store_chat_history()
            if store_chat_history:
                _t0 = perf_counter()
                await cm.event_broker.publish(
                    "app:comms:chat_history",
                    GetChatHistory(
                        chat_history=store_chat_history.chat_history,
                    ).to_json(),
                )
                log_startup_timing(
                    LOGGER,
                    "⏱️ [StartupTiming] managers.init_conv_manager.publish_chat_history duration=%.2fs",
                    perf_counter() - _t0,
                )

            cm.initialized = True

            # Await the concurrent hydration that was kicked off inside
            # _init_managers right after EventBus config.  In practice it
            # finishes long before this point (hidden behind ContactManager
            # init), so this is effectively a no-op await.
            hydration_future = getattr(cm, "_hydration_future", None)
            if hydration_future is not None:
                try:
                    _t0 = perf_counter()
                    # The count feeds the initialization-complete notification:
                    # "history has been loaded" is only said when it is true.
                    cm._hydrated_history_count = int(
                        await asyncio.wrap_future(hydration_future) or 0,
                    )
                    log_startup_timing(
                        LOGGER,
                        "⏱️ [StartupTiming] managers.init_conv_manager.await_hydration duration=%.2fs",
                        perf_counter() - _t0,
                    )
                    LOGGER.info(
                        f"{ICONS['managers_worker']} [ManagersWorker] "
                        "Concurrent hydration completed",
                    )
                except Exception as e:
                    LOGGER.error(
                        f"{ICONS['managers_worker']} [ManagersWorker] "
                        f"Global thread hydration failed: {e}",
                    )
                    import traceback

                    traceback.print_exc()
                finally:
                    cm._hydration_future = None

            # Start the in-process activation scheduler: the asyncio
            # supervisor that fires scheduled tasks onto the event broker.
            # Must run after managers are initialised because the scheduler
            # reads ``Tasks/Executions`` through the same storage layer the
            # managers configure.
            try:
                from unify.task_scheduler.local_scheduler import build_materializer

                _t0 = perf_counter()
                cm._activation_materializer = build_materializer(cm)
                await cm._activation_materializer.start()
                log_startup_timing(
                    LOGGER,
                    "⏱️ [StartupTiming] managers.init_conv_manager.start_local_scheduler duration=%.2fs",
                    perf_counter() - _t0,
                )
            except Exception as exc:
                LOGGER.warning(
                    f"{ICONS['managers_worker']} [ManagersWorker] "
                    f"LocalActivationScheduler failed to start (degraded): {exc}",
                )
                cm._activation_materializer = None

            # Publish initialization complete event.  The registered
            # InitializationComplete handler pushes a notification and
            # triggers a brain turn so it can follow up on deferred requests.
            _t0 = perf_counter()
            await event_broker.publish(
                "app:comms:initialization_complete",
                InitializationComplete().to_json(),
            )
            log_startup_timing(
                LOGGER,
                "⏱️ [StartupTiming] managers.init_conv_manager.publish_initialization_complete duration=%.2fs",
                perf_counter() - _t0,
            )

            LOGGER.info(
                f"{ICONS['managers_worker']} [ManagersWorker] Initialization complete in "
                f"{perf_counter() - start_time:.2f} seconds",
            )

        except Exception as e:
            LOGGER.error(
                f"{ICONS['managers_worker']} [ManagersWorker] Error during initialization: {e}",
            )
            raise
        finally:
            # Idempotent: hydration completion normally reopened this long
            # ago. Guarantees no path out of a boot — success or failure —
            # leaves brain turns holding until their timeout.
            cm._hydration_gate.set()
