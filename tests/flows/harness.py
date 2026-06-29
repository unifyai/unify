"""Real ConversationManager harness for core user-flow tests.

Boots an in-process CM with a real CodeAct actor and real manager backends
against the local Orchestra project. Inbound messages are injected through
the same EventPublisher path as the ``unity`` sandbox CLI; outbound replies
are captured from the in-memory outbound transport and ``UnifyMessageSent``
events on the CM event broker.
"""

from __future__ import annotations

import os

# Flow tests run in parallel via parallel_run.sh; disable the shared LLM cache
# before any unity/unillm imports so completions cannot bleed across sessions.
# Mirrors conftest: a developer who opts into a per-process cache by exporting
# UNILLM_CACHE_DIR keeps their setting.
if not os.environ.get("UNILLM_CACHE_DIR"):
    os.environ["UNILLM_CACHE"] = "false"

import asyncio
import hashlib
import json
import time
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from unity.actor.code_act_actor import CodeActActor
from unity.actor.environments import StateManagerEnvironment
from unity.conversation_manager.comms_manager import CommsManager
from unity.conversation_manager.domains import managers_utils
from unity.conversation_manager.domains.comms_utils import set_outbound_transport
from unity.conversation_manager.event_broker import get_event_broker, reset_event_broker
from unity.conversation_manager.events import Event, UnifyMessageSent
from unity.conversation_manager.main import run_conversation_manager
from unity.gateway.factory import (
    create_ingress_transport_factory,
    create_outbound_transport,
)
from unity.gateway.outbound_inmemory import InMemoryOutboundTransport
from unity.manager_registry import ManagerRegistry
from unity.session_details import SESSION_DETAILS
from sandboxes.conversation_manager.actor_factory import ActorFactory
from sandboxes.conversation_manager.event_publisher import (
    EventPublisher,
    build_unify_attachment_meta,
    get_user_contact,
)


def _runtime_user_context(context_path: str) -> str:
    """Map a flow context path to the SESSION_DETAILS user_context component."""

    return context_path.rsplit("/", 1)[0]


def _manager_method_calls(events: list[Any]) -> list[tuple[str, str]]:
    """Return ``(manager, method)`` pairs from captured ManagerMethod events.

    Reads the incoming phase only — one entry per primitive invocation the brain
    made — so callers see exactly which ``primitives.*`` surfaces were exercised.
    """

    calls: list[tuple[str, str]] = []
    for event in events:
        payload = getattr(event, "payload", None)
        if not isinstance(payload, dict) or payload.get("phase") != "incoming":
            continue
        manager = payload.get("manager")
        method = payload.get("method")
        if manager and method:
            calls.append((manager, method))
    return calls


def assert_primitive_invoked(
    events: list[Any],
    manager: str,
    method: str | None = None,
    *,
    phase: str = "incoming",
) -> None:
    """Assert the brain invoked ``primitives.<manager>`` during a captured turn.

    ``events`` is the list yielded by ``capture_events("ManagerMethod")`` around a
    flow turn. The ManagerMethod telemetry is published by the real
    ``@log_manager_call``/``@log_manager_result`` decorators on the production
    manager methods, so a match proves the brain reached the result through the
    intended primitive rather than an alternative path (a shell command, a direct
    Orchestra write, or answering from memory). ``manager`` is the telemetry name
    (``"ContactManager"``, ``"FileManager"``, ...); pass ``method=None`` to accept
    any method on that manager.
    """

    for event in events:
        payload = getattr(event, "payload", None)
        if not isinstance(payload, dict):
            continue
        if payload.get("manager") != manager:
            continue
        if method is not None and payload.get("method") != method:
            continue
        if phase is not None and payload.get("phase") != phase:
            continue
        return

    observed = sorted({f"{mgr}.{meth}" for mgr, meth in _manager_method_calls(events)})
    target = manager if method is None else f"{manager}.{method}"
    raise AssertionError(
        f"Expected the brain to invoke {target} during this turn, but it was not "
        f"among the primitive calls it made: {observed or '(none)'}. The brain may "
        "have reached the answer through an alternative path (e.g. a shell "
        "command) instead of the primitive.",
    )


@dataclass
class FlowHarness:
    """Drive a real CM turn and assert user-visible outcomes."""

    cm: Any
    publisher: EventPublisher
    outbound: InMemoryOutboundTransport
    context_path: str = "default/0"
    _reply_events: list[UnifyMessageSent] = field(default_factory=list)
    _reply_baseline: int = 0
    _listener_task: asyncio.Task | None = None
    _operations_listener_task: asyncio.Task | None = None

    async def wait_until(
        self,
        predicate,
        *,
        timeout: float = 240.0,
        interval: float = 0.5,
        description: str = "condition",
    ) -> Any:
        """Poll until ``predicate()`` returns a truthy value."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            await asyncio.sleep(interval)
        raise TimeoutError(f"Timed out waiting for {description} after {timeout}s")

    async def wait_for_contact_email(
        self,
        email: str,
        *,
        timeout: float = 240.0,
    ) -> Any:
        """Wait until a contact row with ``email`` exists in Orchestra."""

        def _contact_email(contact: Any) -> str | None:
            if isinstance(contact, dict):
                return contact.get("email_address")
            return getattr(contact, "email_address", None)

        def _found() -> Any | None:
            result = self.manager("contact").filter_contacts(
                filter=f"email_address == '{email}'",
            )
            for contact in result.get("contacts") or []:
                if _contact_email(contact) == email:
                    return contact
            return None

        return await self.wait_until(
            _found,
            timeout=timeout,
            description=f"contact email {email}",
        )

    async def wait_for_task_name(
        self,
        name: str,
        *,
        timeout: float = 300.0,
    ) -> Any:
        """Wait until a task ``Task`` row with ``name`` is persisted.

        Returns the typed ``Task`` (not a raw row dict) so callers can assert on
        ``status``, ``schedule``, and ``offline``. The unique name keeps the
        ``_filter_tasks`` lookup unambiguous; the poll absorbs the brief
        read-after-write window before the row is queryable.
        """

        def _found() -> Any | None:
            scheduler = self.manager("task")
            rows = scheduler._filter_tasks(filter=f"name == '{name}'")
            if rows:
                return rows[0]
            for row in scheduler._filter_tasks():
                if getattr(row, "name", None) == name:
                    return row
            return None

        return await self.wait_until(
            _found,
            timeout=timeout,
            description=f"task {name}",
        )

    async def wait_for_secret_name(
        self,
        name: str,
        *,
        timeout: float = 240.0,
    ) -> str:
        """Wait until a secret with ``name`` is readable from SecretManager."""

        def _found() -> str | None:
            keys = self.manager("secret")._list_secret_keys()
            return name if name in keys else None

        return await self.wait_until(
            _found,
            timeout=timeout,
            description=f"secret {name}",
        )

    def seed_knowledge_table(
        self,
        *,
        table_name: str,
        rows: list[dict[str, Any]],
        columns: dict[str, str] | None = None,
    ) -> None:
        """Create a knowledge table, insert rows, and verify they are readable."""

        km = self.manager("knowledge")
        if columns:
            km._create_table(name=table_name, columns=columns)
        else:
            km._create_table(name=table_name)
        outcome = km._add_rows(table=table_name, rows=rows)
        if outcome.get("outcome") != "rows added successfully":
            raise RuntimeError(
                f"Knowledge table {table_name} add_rows failed: {outcome!r}",
            )
        filtered = km._filter(tables=table_name, filter=None)
        if not (filtered.get(table_name) or []):
            raise RuntimeError(
                f"Knowledge table {table_name} not readable after seed: {filtered!r}",
            )

    async def start_listener(self) -> None:
        """Subscribe to assistant unify replies on the CM event broker."""

        async def _listen() -> None:
            async with self.cm.event_broker.pubsub() as pubsub:
                await pubsub.subscribe("app:comms:unify_message_sent")
                while True:
                    msg = await pubsub.get_message(
                        timeout=1.0,
                        ignore_subscribe_messages=True,
                    )
                    if not msg or msg.get("type") != "message":
                        continue
                    try:
                        event = Event.from_json(msg["data"])
                    except Exception:
                        continue
                    if isinstance(event, UnifyMessageSent):
                        self._reply_events.append(event)

        self._listener_task = asyncio.create_task(_listen())

    async def inject_unify_message(
        self,
        message: str,
        *,
        attachments: list[Path] | None = None,
    ) -> None:
        """Deliver an inbound unify chat message through the real ingress path.

        Builds the same ``thread="unify_message"`` envelope the gateway
        publishes and hands it to the CommsManager's in-memory ingress
        transport, so the turn exercises ``dispatch_inbound_envelope``
        normalization (contact resolution + backup-contacts publish) exactly
        as a hosted webhook would.
        """

        self._bind_flow_context()
        self.reset_turn_state()
        await _wait_for_operations_queue_idle(timeout=30.0)
        self._reply_baseline = len(self._reply_events)
        await self.deliver_inbound_envelope(
            thread="unify_message",
            event={
                "contact_id": self._boss_contact()["contact_id"],
                "contacts": [self._boss_contact()],
                "assistant_id": str(SESSION_DETAILS.assistant.agent_id or ""),
                "body": message,
                "attachments": build_unify_attachment_meta(attachments),
            },
        )

    def _boss_contact(self) -> dict[str, Any]:
        """Boss/owner contact dict carried on inbound envelopes for this run."""

        return get_user_contact(self.cm)

    async def deliver_inbound_envelope(
        self,
        *,
        thread: str,
        event: dict[str, Any],
    ) -> None:
        """Deliver a raw ``{thread, publish_timestamp, event}`` inbound envelope.

        Routes through the CommsManager ingress transport — the production
        normalization path materialized in ``build_flow_harness``.
        """

        envelope = {
            "thread": thread,
            "publish_timestamp": time.time(),
            "event": event,
        }
        ingress = getattr(self.cm.comms_manager, "ingress_transport", None)
        if ingress is None:
            raise RuntimeError(
                "Flow harness has no ingress transport; build_flow_harness must "
                "materialize one before delivering inbound envelopes",
            )
        await ingress.deliver(envelope, source_topic=thread)

    async def start_meet(self) -> None:
        """Signal that a web voice meeting's agent has joined (no LiveKit).

        Publishes ``UnifyMeetStarted`` — the event the voice agent emits once
        it joins the room — so meet lifecycle handling runs without standing up
        a LiveKit server.
        """

        from unity.conversation_manager.events import UnifyMeetStarted

        self._bind_flow_context()
        self.reset_turn_state()
        await self.publisher.publish_event(
            UnifyMeetStarted(contact=self._boss_contact()),
        )

    async def speak_in_meet(self, text: str) -> None:
        """Deliver a spoken user utterance into the active meet."""

        from unity.conversation_manager.events import InboundUnifyMeetUtterance

        self._reply_baseline = len(self._reply_events)
        await self.publisher.publish_event(
            InboundUnifyMeetUtterance(contact=self._boss_contact(), content=text),
        )

    async def end_meet(self) -> None:
        """End the active meet and let lifecycle teardown run."""

        from unity.conversation_manager.events import UnifyMeetEnded

        await self.publisher.publish_event(
            UnifyMeetEnded(contact=self._boss_contact()),
        )

    def meet_active(self) -> bool:
        """True while the CM considers a web voice meeting in progress."""

        return self.cm.call_manager.unify_meet_start_timestamp is not None

    def meet_messages(self) -> list[str]:
        """Message contents recorded in the boss conversation thread."""

        contact_id = self._boss_contact()["contact_id"]
        messages = self.cm.contact_index.get_messages_for_contact(contact_id)
        return [str(getattr(m, "content", m) or "") for m in messages]

    def _outbox_reply_texts(self) -> list[str]:
        texts: list[str] = []
        for envelope in self.read_outbox():
            event = envelope.get("event") or {}
            content = str(event.get("content") or "").strip()
            if content:
                texts.append(content)
        return texts

    def _new_reply_events(self) -> list[UnifyMessageSent]:
        return self._reply_events[self._reply_baseline :]

    async def wait_for_unify_reply_containing(
        self,
        text: str,
        *,
        timeout: float = 300.0,
        min_length: int = 1,
    ) -> UnifyMessageSent:
        """Wait for an assistant unify reply whose content includes ``text``."""

        needle = text.strip().lower()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in self._new_reply_events():
                content = str(getattr(event, "content", "") or "").strip()
                if len(content) >= min_length and needle in content.lower():
                    return event
            for content in self._outbox_reply_texts():
                if len(content) >= min_length and needle in content.lower():
                    return UnifyMessageSent(
                        contact=self._boss_contact(),
                        content=content,
                        attachments=[],
                    )
            await asyncio.sleep(0.25)
        raise TimeoutError(
            f"No UnifyMessageSent reply containing {text!r} within {timeout}s "
            f"(seen {len(self._reply_events)} broker events, "
            f"{len(self._outbox_reply_texts())} outbox replies)",
        )

    async def wait_for_unify_reply(
        self,
        *,
        timeout: float = 180.0,
        min_length: int = 1,
    ) -> UnifyMessageSent:
        """Wait until the assistant publishes a unify reply event."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in self._new_reply_events():
                content = str(getattr(event, "content", "") or "").strip()
                if len(content) >= min_length:
                    return event
            for content in self._outbox_reply_texts():
                if len(content) >= min_length:
                    return UnifyMessageSent(
                        contact=self._boss_contact(),
                        content=content,
                        attachments=[],
                    )
            await asyncio.sleep(0.25)
        raise TimeoutError(
            f"No UnifyMessageSent reply within {timeout}s "
            f"(seen {len(self._reply_events)} partial events, "
            f"{len(self._outbox_reply_texts())} outbox replies)",
        )

    def reset_turn_state(self) -> None:
        """Clear per-turn reply/outbound state between tests."""

        self._reply_events.clear()
        self._reply_baseline = 0
        with self.outbound._lock:  # noqa: SLF001 — harness-owned transport reset
            self.outbound._published.clear()

    def _clear_cm_conversation_state(self) -> None:
        contact_index = getattr(self.cm, "contact_index", None)
        if contact_index is not None:
            contact_index.clear_conversations()

    async def drain_startup_replies(
        self,
        *,
        timeout: float = 90.0,
        quiet_seconds: float = 2.0,
    ) -> None:
        """Wait out InitializationComplete slow-brain replies, then reset."""

        self._bind_flow_context()
        self._clear_cm_conversation_state()
        deadline = time.monotonic() + timeout
        last_change = time.monotonic()
        last_total = -1
        while time.monotonic() < deadline:
            total = len(self._reply_events) + len(self.read_outbox())
            if total != last_total:
                last_change = time.monotonic()
                last_total = total
            if time.monotonic() - last_change >= quiet_seconds:
                break
            await asyncio.sleep(0.25)
        await _wait_for_operations_queue_idle(timeout=60.0)
        self.reset_turn_state()
        self._clear_cm_conversation_state()

    def read_outbox(self) -> list[dict[str, Any]]:
        """Return outbound unify envelopes captured by the in-memory transport."""

        items: list[dict[str, Any]] = []
        for envelope in self.outbound.published:
            if envelope.thread != "unify_message_outbound":
                continue
            payload = json.loads(envelope.message.decode("utf-8"))
            items.append(payload)
        return items

    def manager(self, alias: str) -> Any:
        """Return a real manager singleton for persisted-state assertions."""

        self._bind_flow_context()
        alias_getters = {
            "contact": "get_contact_manager",
            "knowledge": "get_knowledge_manager",
            "task": "get_task_scheduler",
            "file": "get_file_manager",
            "transcript": "get_transcript_manager",
            "secret": "get_secret_manager",  # pragma: allowlist secret
            "data": "get_data_manager",
            "dashboard": "get_dashboard_manager",
        }
        getter_name = alias_getters.get(alias, f"get_{alias}_manager")
        getter = getattr(ManagerRegistry, getter_name, None)
        if getter is None:
            raise ValueError(f"Unknown manager alias: {alias}")
        return getter()

    def _bind_flow_context(self) -> None:
        """Rebind Orchestra writes to the per-test context for this harness."""

        if not self.context_path:
            return
        try:
            import unisdk
            from unity.common.context_registry import ContextRegistry
            from unity.knowledge_manager.knowledge_manager import KNOWLEDGE_TABLE

            try:
                unisdk.set_context(self.context_path, relative=False, skip_create=True)
            except Exception:
                pass
            ContextRegistry.set_base_context(self.context_path)
            knowledge_manager = ManagerRegistry.get_knowledge_manager()
            knowledge_manager._ctx = (
                f"{self.context_path.rstrip('/')}/{KNOWLEDGE_TABLE}"
            )
            object.__setattr__(
                knowledge_manager,
                "_KnowledgeManager__data_manager",
                None,
            )
            SESSION_DETAILS.user.id = _runtime_user_context(self.context_path)
        except Exception:
            pass

    async def shutdown(self) -> None:
        if self._operations_listener_task is not None:
            await _wait_for_operations_queue_idle(timeout=60.0)
            self._operations_listener_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._operations_listener_task
            self._operations_listener_task = None
        await _reset_operations_queue()

        if self._listener_task is not None:
            self._listener_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._listener_task
        try:
            self.cm.stop.set()
        except Exception:
            pass
        try:
            await asyncio.wait_for(self.cm.cleanup(), timeout=30.0)
        except Exception:
            pass
        set_outbound_transport(None)
        reset_event_broker()


async def _reset_operations_queue() -> None:
    """Replace the module queue on the current event loop between flow tests."""

    managers_utils._operations_queue = asyncio.Queue()


async def _wait_for_operations_queue_idle(*, timeout: float = 30.0) -> None:
    """Best-effort wait for queued manager ops to finish before the next test."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if managers_utils._operations_queue.empty():
            await asyncio.sleep(0.25)
            if managers_utils._operations_queue.empty():
                return
        await asyncio.sleep(0.25)


async def build_flow_harness(
    *,
    project_name: str,
    context_path: str = "default/0",
) -> FlowHarness:
    """Start a real CM + CodeAct actor for flow tests."""

    import unisdk
    from unity.settings import SETTINGS

    await _reset_operations_queue()

    isolation_user = hashlib.sha256(context_path.encode()).hexdigest()[:16]
    os.environ["USER_ID"] = isolation_user

    unisdk.activate(project_name, overwrite=False)
    try:
        from unity.common.context_registry import ContextRegistry
        from unity.events.event_bus import EVENT_BUS

        ContextRegistry.clear()
        EVENT_BUS.clear(delete_contexts=False)
    except Exception:
        pass
    try:
        unisdk.set_context(context_path, relative=False, skip_create=False)
    except Exception:
        unisdk.set_context(context_path, relative=False, skip_create=True)

    ActorFactory._apply_manager_impl_env("real")
    ManagerRegistry.clear()
    try:
        reset_event_broker()
    except Exception:
        pass
    event_broker = get_event_broker()
    try:
        managers_utils.event_broker = event_broker  # type: ignore[attr-defined]
    except Exception:
        pass

    # Run as the unassigned assistant: keeps real managers off the Orchestra
    # contact-membership path and prevents run_conversation_manager from
    # auto-triggering a duplicate init (which only fires for a real agent_id).
    # USER_ID stays hashed for Orchestra comms isolation; SESSION_DETAILS.user.id
    # must match the flow context prefix so ensure_runtime_context() does not
    # rebind managers back to {hash}/0 after seeding knowledge in context_path.
    SESSION_DETAILS.assistant.agent_id = None

    SETTINGS.knowledge.ENABLED = True
    SETTINGS.file.ENABLED = True
    SETTINGS.task.LOCAL_SCHEDULER_ENABLED = True

    cm = await run_conversation_manager(
        project_name=project_name,
        event_broker=event_broker,
        enable_comms_manager=False,
        apply_test_mocks=False,
    )
    SESSION_DETAILS.user.id = _runtime_user_context(context_path)

    # Capture outbound on an in-memory transport and route inbound through a
    # real CommsManager + in-memory ingress transport, so flow turns exercise
    # the production envelope -> dispatch_inbound_envelope normalization without
    # standing up Pub/Sub. run_conversation_manager leaves comms disabled under
    # tests; the harness owns this wiring directly so the unassigned-assistant
    # poll loop (CommsManager.start with agent_id=None) never spins.
    outbound = create_outbound_transport(kind="inmemory")
    set_outbound_transport(outbound)
    comms_manager = CommsManager(
        event_broker=event_broker,
        ingress_transport_factory=create_ingress_transport_factory(kind="inmemory"),
    )
    cm.comms_manager = comms_manager
    await comms_manager._start_inbound_subscription()

    primitives = ActorFactory.build_primitives()
    actor = CodeActActor(environments=[StateManagerEnvironment(primitives)])
    await managers_utils.init_conv_manager(cm, actor=actor)

    operations_listener_task = asyncio.create_task(
        managers_utils.listen_to_operations(cm),
    )

    if cm.contact_manager is not None:
        try:
            cm.contact_manager.update_contact(
                contact_id=1,
                first_name=os.getenv("USER_FIRST_NAME", "Alex"),
                surname=os.getenv("USER_SURNAME", "Rivera"),
                email_address=os.getenv("USER_EMAIL", "alex.rivera@example.com"),
                phone_number=os.getenv("USER_NUMBER", "+14155550142"),
                should_respond=True,
            )
        except Exception:
            pass

    publisher = EventPublisher(
        cm=cm,
        state=SimpleNamespace(
            in_meet=False,
            live_voice_session=None,
            last_event_published_at=0.0,
        ),
    )

    harness = FlowHarness(
        cm=cm,
        publisher=publisher,
        outbound=outbound,
        context_path=context_path,
    )
    harness._operations_listener_task = operations_listener_task
    harness._bind_flow_context()
    await harness.start_listener()
    await harness.drain_startup_replies(timeout=90.0)
    return harness
