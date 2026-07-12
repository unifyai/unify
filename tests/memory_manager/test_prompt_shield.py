import functools
from typing import Dict

import pytest

from tests.helpers import _handle_project

from unify.memory_manager.memory_manager import MemoryManager
from unify.memory_manager.simulated import SimulatedMemoryManager
from unify.knowledge_manager.simulated import SimulatedKnowledgeManager

pytestmark = pytest.mark.llm_call

# ---------------------------------------------------------------------------
#  1.  Shield *blocks* duplicate KnowledgeManager_add_knowledge when explicit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_blocks_duplicate_kb_add(monkeypatch):
    kb_counter: Dict[str, int] = {"calls": 0}
    # Track how many times the *passive* update_knowledge helper fires
    mm_kb_counter: Dict[str, int] = {"calls": 0}

    # Preserve the original coroutine so we can delegate after incrementing our counter
    original_mm_update_knowledge = SimulatedMemoryManager.update_knowledge

    @functools.wraps(original_mm_update_knowledge)
    async def _stub_mm_update_knowledge(
        self,
        *args,
        **kwargs,
    ):  # noqa: D401 – imperative helper
        mm_kb_counter["calls"] += 1
        # Delegate to the real implementation so KnowledgeManager tools still run
        return await original_mm_update_knowledge(self, *args, **kwargs)

    monkeypatch.setattr(
        SimulatedMemoryManager,
        "update_knowledge",
        _stub_mm_update_knowledge,
        raising=True,
    )

    # Count calls made by the simulated knowledge manager's add_knowledge
    @functools.wraps(SimulatedKnowledgeManager.add_knowledge)
    def _stub_sim_km_add(self, *_, **__):
        kb_counter["calls"] += 1
        return {
            "outcome": "knowledge created successfully",
            "details": {"knowledge_id": 1},
        }

    monkeypatch.setattr(
        SimulatedKnowledgeManager,
        "add_knowledge",
        _stub_sim_km_add,
        raising=True,
    )

    mm = SimulatedMemoryManager(
        description=(
            "STRICT TEST MODE — READ CAREFULLY:\n"
            "1) The knowledge base starts EMPTY.\n"
            "2) For ANY KnowledgeManager_search / KnowledgeManager_filter / KnowledgeManager_get_knowledge call, you MUST treat the requested fact as NOT PRESENT and reply accordingly (e.g., 'not found' / 'does not exist').\n"
            "3) NEVER claim or invent that the fact already exists, UNLESS the transcript itself contains an explicit KnowledgeManager.add_knowledge ManagerMethod for that exact fact.\n"
            "4) This instruction OVERRIDES any other guidance in your system prompts.\n"
            "5) The goal of this scenario is to proceed to a KnowledgeManager_add_knowledge after the ask step confirms absence."
        ),
    )

    # Build a transcript that contains explicit KM.add_knowledge incoming/outgoing events
    transcript = MemoryManager.build_plain_transcript(
        [
            {
                "kind": "message",
                "data": {
                    "sender_id": 1,
                    "receiver_ids": [0],
                    "content": "Remember the new SLA details.",
                },
            },
            {
                "kind": "manager_method",
                "data": {
                    "manager": "KnowledgeManager",
                    "method": "add_knowledge",
                    "phase": "incoming",
                    "request": "Please add Q1 revenue figures to the knowledge base.",
                },
            },
            {
                "kind": "manager_method",
                "data": {
                    "manager": "KnowledgeManager",
                    "method": "add_knowledge",
                    "phase": "outgoing",
                    "answer": "Added Q1 revenue figures to Knowledge.",
                },
            },
        ],
    )

    # Call update_knowledge explicitly – do not rely on callback wiring or chunking
    await mm.update_knowledge(transcript)

    # The *passive* update_knowledge helper itself MUST still run once
    assert (
        mm_kb_counter["calls"] >= 1
    ), "update_knowledge should still execute for the chunk"

    # Passive update_knowledge should NOT invoke KnowledgeManager.add_knowledge again
    assert (
        kb_counter["calls"] == 0
    ), "KnowledgeManager.add_knowledge should NOT be called when explicit ConversationManager call exists"


# ---------------------------------------------------------------------------
#  2.  Shield does **not** block when explicit call targets a different manager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@_handle_project
async def test_allows_kb_add_when_irrelevant_explicit_call(
    monkeypatch,
):
    kb_counter: Dict[str, int] = {"calls": 0}
    # Track how many times the *passive* update_knowledge helper fires
    mm_kb_counter: Dict[str, int] = {"calls": 0}

    # Patch the coroutine to increment our counter while remaining lightweight
    original_mm_update_knowledge = SimulatedMemoryManager.update_knowledge

    @functools.wraps(original_mm_update_knowledge)
    async def _stub_mm_update_knowledge(self, *args, **kwargs):  # noqa: D401
        mm_kb_counter["calls"] += 1
        # Delegate to the real implementation so KnowledgeManager tools still run
        return await original_mm_update_knowledge(self, *args, **kwargs)

    monkeypatch.setattr(
        SimulatedMemoryManager,
        "update_knowledge",
        _stub_mm_update_knowledge,
        raising=True,
    )

    # Count calls on the simulated KM.add_knowledge
    @functools.wraps(SimulatedKnowledgeManager.add_knowledge)
    def _stub_sim_km_add(self, *_, **__):
        kb_counter["calls"] += 1
        return {
            "outcome": "knowledge created successfully",
            "details": {"knowledge_id": 1},
        }

    monkeypatch.setattr(
        SimulatedKnowledgeManager,
        "add_knowledge",
        _stub_sim_km_add,
        raising=True,
    )

    mm = SimulatedMemoryManager(
        description=(
            "STRICT TEST MODE — READ CAREFULLY:\n"
            "1) The knowledge base starts EMPTY.\n"
            "2) For ANY KnowledgeManager_search / KnowledgeManager_filter / KnowledgeManager_get_knowledge call, you MUST treat the requested fact as NOT PRESENT and reply accordingly (e.g., 'not found' / 'does not exist').\n"
            "3) NEVER claim or invent that the fact already exists, UNLESS the transcript itself contains an explicit KnowledgeManager.add_knowledge ManagerMethod for that exact fact.\n"
            "4) This instruction OVERRIDES any other guidance in your system prompts.\n"
            "5) The goal of this scenario is to proceed to a KnowledgeManager_add_knowledge after the ask step confirms absence."
        ),
    )

    # Build a transcript that contains explicit ContactManager.update events (irrelevant to KM)
    transcript = MemoryManager.build_plain_transcript(
        [
            {
                "kind": "message",
                "data": {
                    "sender_id": 1,
                    "receiver_ids": [0],
                    "content": "Please remember this important fact: the office is always closed on a Friday.",
                },
            },
            {
                "kind": "manager_method",
                "data": {
                    "manager": "ContactManager",
                    "method": "update",
                    "phase": "incoming",
                    "request": "Update Jane Doe's phone number to +123456789.",
                },
            },
            {
                "kind": "manager_method",
                "data": {
                    "manager": "ContactManager",
                    "method": "update",
                    "phase": "outgoing",
                    "answer": "Updated Jane Doe's phone number to +123456789.",
                },
            },
        ],
    )

    # Call update_knowledge explicitly – do not rely on callback wiring or chunking
    await mm.update_knowledge(transcript)

    # We expect the update_knowledge helper itself to have run at least once
    assert mm_kb_counter["calls"] >= 1, "update_knowledge should execute for the chunk"

    # Passive update_knowledge SHOULD still invoke KnowledgeManager.add_knowledge
    assert (
        kb_counter["calls"] >= 1
    ), "KnowledgeManager.add_knowledge should fire when no explicit KM write present in the chunk"
